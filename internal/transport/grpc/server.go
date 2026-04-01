// Package grpc — реализация gRPC сервера для приёма телеметрии.
//
// Этот пакет реализует интерфейс TelemetryServiceServer,
// сгенерированный из telemetry.proto.
//
// АРХИТЕКТУРНОЕ РЕШЕНИЕ:
// gRPC сервер — тонкий слой ("тонкий контроллер").
// Он НЕ содержит бизнес-логику (запись в БД, ML инференс и т.д.).
// Он только:
// 1. Принимает данные по сети
// 2. Валидирует входные данные
// 3. Передаёт в бизнес-слой через интерфейс (TelemetryHandler)
// 4. Формирует ответ
//
// ПОЧЕМУ так?
// - Тестируемость: бизнес-логику можно тестировать без поднятия gRPC сервера
// - Гибкость: можно заменить gRPC на NATS/HTTP без изменения бизнес-логики
// - Единственная ответственность: сервер отвечает за транспорт, не за логику
package grpc

import (
	"context"
	"fmt"
	"io"
	"log"

	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
)

// TelemetryHandler — интерфейс для обработки телеметрии.
//
// ПОЧЕМУ интерфейс, а не конкретная структура?
// Принцип инверсии зависимостей (Dependency Inversion - буква D в SOLID):
// - gRPC сервер зависит от АБСТРАКЦИИ (интерфейс), а не от конкретной реализации
// - В проде: реальный обработчик (пишет в TimescaleDB)
// - В тестах: мок (просто считает вызовы)
// - При смене БД: меняем реализацию, gRPC сервер не трогаем
//
// На собеседовании это частый вопрос: "Как вы обеспечиваете тестируемость?"
// Ответ: через интерфейсы и dependency injection.
type TelemetryHandler interface {
	// HandleBatch обрабатывает пакет телеметрии.
	// Возвращает количество принятых записей и ошибки по отдельным записям.
	HandleBatch(ctx context.Context, records []*pb.TelemetryRecord) (accepted int, errors []*pb.RecordError, err error)
}

// Server — реализация gRPC сервиса TelemetryService.
type Server struct {
	// UnimplementedTelemetryServiceServer — встраиваемая структура.
	// ПОЧЕМУ нужна?
	// gRPC требует реализовать ВСЕ методы сервиса. Если мы добавим новый RPC
	// в .proto, старый код без этой структуры перестанет компилироваться.
	// UnimplementedTelemetryServiceServer предоставляет заглушки для всех методов —
	// новые RPC будут возвращать "not implemented" вместо ошибки компиляции.
	// Это обеспечивает forward compatibility (совместимость при обновлении .proto).
	pb.UnimplementedTelemetryServiceServer

	handler TelemetryHandler
}

// NewServer создаёт новый gRPC сервер с указанным обработчиком.
//
// ПОЧЕМУ конструктор (NewServer), а не прямое создание структуры?
// 1. Валидация: можно проверить что handler != nil
// 2. Инкапсуляция: поля структуры неэкспортированы (строчная буква)
// 3. Конвенция Go: New<Type> — стандартный паттерн создания
func NewServer(handler TelemetryHandler) *Server {
	return &Server{handler: handler}
}

// Ingest — приём батча телеметрии (unary RPC).
// Клиент отправляет TelemetryBatch, получает IngestResponse.
//
// Это основной метод для отправки данных из WAL — когда агент
// накопил записи за время offline и отправляет их пачкой.
func (s *Server) Ingest(ctx context.Context, batch *pb.TelemetryBatch) (*pb.IngestResponse, error) {
	// Логируем входящий запрос — в проде это заменяется на structured logging
	// (например, zerolog или zap), но для обучения log.Printf нагляднее
	log.Printf("[gRPC] Ingest: получен батч от агента %s, записей: %d",
		batch.AgentId, len(batch.Records))

	// Валидация — на границе системы ВСЕГДА проверяем входные данные.
	// ПОЧЕМУ здесь, а не в handler?
	// - Транспортный слой отвечает за валидацию формата (есть ли записи?)
	// - Бизнес-слой отвечает за валидацию логики (корректны ли значения?)
	if len(batch.Records) == 0 {
		return &pb.IngestResponse{AcceptedCount: 0}, nil
	}

	// Передаём в бизнес-слой через интерфейс
	accepted, recordErrors, err := s.handler.HandleBatch(ctx, batch.Records)
	if err != nil {
		// ПОЧЕМУ возвращаем gRPC ошибку, а не кладём в IngestResponse?
		// gRPC различает:
		// - Ошибка метода (return nil, err): клиент знает что запрос ПРОВАЛИЛСЯ
		// - Частичный успех (return response, nil): клиент знает что часть записей принята
		// Полный провал (БД недоступна) — это ошибка метода.
		// Частичный провал (2 из 100 записей невалидны) — это response с errors.
		return nil, fmt.Errorf("ошибка обработки батча: %w", err)
	}

	log.Printf("[gRPC] Ingest: принято %d записей, ошибок: %d", accepted, len(recordErrors))

	return &pb.IngestResponse{
		AcceptedCount: int32(accepted),
		Errors:        recordErrors,
	}, nil
}

// StreamTelemetry — потоковый приём телеметрии (client streaming RPC).
// Клиент открывает поток и шлёт записи одну за другой.
// Сервер отвечает ОДИН раз когда клиент закроет поток.
//
// ЗАЧЕМ streaming если есть Ingest?
// - Ingest: для батчей из WAL (offline → online, отправляем накопленное)
// - StreamTelemetry: для real-time потока (станок работает, данные идут непрерывно)
//
// Разница:
// - Ingest: новое TCP-соединение на каждый батч
// - Stream: одно соединение, данные текут непрерывно (меньше overhead)
func (s *Server) StreamTelemetry(stream pb.TelemetryService_StreamTelemetryServer) error {
	var records []*pb.TelemetryRecord
	var totalAccepted int32

	for {
		// Recv() блокируется до получения следующей записи или закрытия потока
		record, err := stream.Recv()
		if err == io.EOF {
			// Клиент закрыл поток — обрабатываем накопленные записи
			// и отправляем итоговый ответ
			break
		}
		if err != nil {
			return fmt.Errorf("ошибка чтения из потока: %w", err)
		}

		records = append(records, record)

		// FLUSH по размеру батча — не копим бесконечно в памяти.
		// ПОЧЕМУ 100?
		// - Слишком мало (1): по сути нет батчинга, каждая запись = отдельная транзакция в БД
		// - Слишком много (10000): большое потребление памяти при потоковой передаче
		// - 100: одна транзакция в БД на 100 записей — хороший баланс
		if len(records) >= 100 {
			accepted, _, err := s.handler.HandleBatch(stream.Context(), records)
			if err != nil {
				return fmt.Errorf("ошибка обработки батча в потоке: %w", err)
			}
			totalAccepted += int32(accepted)
			// Очищаем слайс, ПЕРЕИСПОЛЬЗУЯ underlying array
			// ПОЧЕМУ records[:0], а не records = nil?
			// records[:0] сохраняет выделенную память (capacity)
			// records = nil освободит массив, и Go выделит новый при следующем append
			// При потоковой передаче мы ЗНАЕМ что записи будут приходить — переиспользуем
			records = records[:0]
		}
	}

	// Обрабатываем остаток (< 100 записей)
	if len(records) > 0 {
		accepted, _, err := s.handler.HandleBatch(stream.Context(), records)
		if err != nil {
			return fmt.Errorf("ошибка обработки остатка в потоке: %w", err)
		}
		totalAccepted += int32(accepted)
	}

	log.Printf("[gRPC] StreamTelemetry: поток завершён, всего принято: %d", totalAccepted)

	// SendAndClose — отправляем ответ И закрываем поток со стороны сервера
	return stream.SendAndClose(&pb.IngestResponse{
		AcceptedCount: totalAccepted,
	})
}
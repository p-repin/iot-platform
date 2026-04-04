// Package grpc — реализация gRPC сервера для приёма телеметрии.
//
// Этот пакет реализует интерфейс TelemetryServiceServer,
// сгенерированный из telemetry.proto.
//
// АРХИТЕКТУРНОЕ РЕШЕНИЕ:
// gRPC сервер — тонкий слой ("тонкий контроллер").
// Он НЕ содержит бизнес-логику (запись в БД, ML инференс и т.д.).
// Он только:
// 1. Принимает данные по сети (protobuf)
// 2. Конвертирует pb → entity (доменные сущности)
// 3. Передаёт в сервисный слой через интерфейс (service.TelemetryService)
// 4. Конвертирует entity → pb (ответ)
//
// ПОЧЕМУ конвертация pb↔entity здесь, а не в service?
// Транспортный слой ОТВЕЧАЕТ за формат данных на границе системы.
// Service работает ТОЛЬКО с доменными типами (entity.Record, entity.Batch).
// Это позволяет добавить NATS/HTTP транспорт без изменения service —
// каждый транспорт конвертирует свой формат в entity самостоятельно.
package grpc

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/pzmash/iot-platform/internal/entity"
	"github.com/pzmash/iot-platform/internal/service"
	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
)

// Server — реализация gRPC сервиса TelemetryService.
type Server struct {
	// UnimplementedTelemetryServiceServer — встраиваемая структура.
	// gRPC требует реализовать ВСЕ методы сервиса. Если мы добавим новый RPC
	// в .proto, старый код без этой структуры перестанет компилироваться.
	// UnimplementedTelemetryServiceServer предоставляет заглушки для всех методов —
	// новые RPC будут возвращать "not implemented" вместо ошибки компиляции.
	pb.UnimplementedTelemetryServiceServer

	// svc — сервисный слой (бизнес-логика).
	// ПОЧЕМУ интерфейс, а не конкретная структура?
	// Принцип инверсии зависимостей (Dependency Inversion - буква D в SOLID):
	// - gRPC сервер зависит от АБСТРАКЦИИ (интерфейс), а не от конкретной реализации
	// - В проде: DBService (пишет в TimescaleDB)
	// - В тестах: мок (просто считает вызовы)
	// - При смене БД: меняем реализацию сервиса, gRPC сервер не трогаем
	svc service.TelemetryService
}

// NewServer создаёт новый gRPC сервер с указанным сервисом.
//
// ПОЧЕМУ конструктор (NewServer), а не прямое создание структуры?
// 1. Валидация: можно проверить что svc != nil
// 2. Инкапсуляция: поля структуры неэкспортированы (строчная буква)
// 3. Конвенция Go: New<Type> — стандартный паттерн создания
func NewServer(svc service.TelemetryService) *Server {
	return &Server{svc: svc}
}

// Ingest — приём батча телеметрии (unary RPC).
// Клиент отправляет TelemetryBatch, получает IngestResponse.
//
// Это основной метод для отправки данных из WAL — когда агент
// накопил записи за время offline и отправляет их пачкой.
func (s *Server) Ingest(ctx context.Context, batch *pb.TelemetryBatch) (*pb.IngestResponse, error) {
	log.Printf("[gRPC] Ingest: получен батч от агента %s, записей: %d",
		batch.AgentId, len(batch.Records))

	// Валидация — на границе системы ВСЕГДА проверяем входные данные.
	// Транспортный слой отвечает за валидацию формата (есть ли записи?).
	// Бизнес-слой отвечает за валидацию логики (корректны ли значения?).
	if len(batch.Records) == 0 {
		return &pb.IngestResponse{AcceptedCount: 0}, nil
	}

	// Конвертируем pb → entity (граница транспорта и бизнес-логики)
	domainBatch := pbBatchToEntity(batch)

	// Передаём в сервисный слой
	accepted, recordErrors, err := s.svc.HandleBatch(ctx, domainBatch)
	if err != nil {
		return nil, fmt.Errorf("ошибка обработки батча: %w", err)
	}

	log.Printf("[gRPC] Ingest: принято %d записей, ошибок: %d", accepted, len(recordErrors))

	return &pb.IngestResponse{
		AcceptedCount: int32(accepted),
		Errors:        entityErrorsToPb(recordErrors),
	}, nil
}

// StreamTelemetry — потоковый приём телеметрии (client streaming RPC).
// Клиент открывает поток и шлёт записи одну за другой.
// Сервер отвечает ОДИН раз когда клиент закроет поток.
//
// ЗАЧЕМ streaming если есть Ingest?
// - Ingest: для батчей из WAL (offline → online, отправляем накопленное)
// - StreamTelemetry: для real-time потока (станок работает, данные идут непрерывно)
func (s *Server) StreamTelemetry(stream pb.TelemetryService_StreamTelemetryServer) error {
	var records []entity.Record
	var totalAccepted int32

	for {
		record, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("ошибка чтения из потока: %w", err)
		}

		records = append(records, pbRecordToEntity(record))

		// FLUSH по размеру батча — не копим бесконечно в памяти.
		if len(records) >= 100 {
			batch := entity.Batch{Records: records}
			accepted, _, err := s.svc.HandleBatch(stream.Context(), batch)
			if err != nil {
				return fmt.Errorf("ошибка обработки батча в потоке: %w", err)
			}
			totalAccepted += int32(accepted)
			records = records[:0]
		}
	}

	// Обрабатываем остаток (< 100 записей)
	if len(records) > 0 {
		batch := entity.Batch{Records: records}
		accepted, _, err := s.svc.HandleBatch(stream.Context(), batch)
		if err != nil {
			return fmt.Errorf("ошибка обработки остатка в потоке: %w", err)
		}
		totalAccepted += int32(accepted)
	}

	log.Printf("[gRPC] StreamTelemetry: поток завершён, всего принято: %d", totalAccepted)

	return stream.SendAndClose(&pb.IngestResponse{
		AcceptedCount: totalAccepted,
	})
}

// ============================================================
// Конвертеры pb ↔ entity
// ============================================================
//
// ПОЧЕМУ конвертеры здесь, а не в отдельном пакете?
// Конвертация — ответственность транспортного слоя.
// Каждый транспорт (gRPC, NATS, HTTP) конвертирует свой формат в entity.
// Если вынести в отдельный пакет — получим "пакет ради пакета".
// Функции неэкспортированы (строчная буква) — используются только здесь.

// pbRecordToEntity конвертирует один protobuf Record в доменный entity.Record.
func pbRecordToEntity(r *pb.TelemetryRecord) entity.Record {
	return entity.Record{
		RecordID:    r.RecordId,
		MachineID:   r.MachineId,
		Timestamp:   r.Timestamp.AsTime(),
		Temperature: r.Temperature,
		Vibration:   r.Vibration,
		Pressure:    r.Pressure,
	}
}

// pbBatchToEntity конвертирует protobuf TelemetryBatch в доменный entity.Batch.
func pbBatchToEntity(batch *pb.TelemetryBatch) entity.Batch {
	records := make([]entity.Record, 0, len(batch.Records))
	for _, r := range batch.Records {
		records = append(records, pbRecordToEntity(r))
	}
	return entity.Batch{
		Records: records,
		AgentID: batch.AgentId,
	}
}

// entityErrorsToPb конвертирует доменные ошибки в protobuf RecordError.
func entityErrorsToPb(errors []entity.RecordError) []*pb.RecordError {
	if len(errors) == 0 {
		return nil
	}
	pbErrors := make([]*pb.RecordError, 0, len(errors))
	for _, e := range errors {
		pbErrors = append(pbErrors, &pb.RecordError{
			RecordId: e.RecordID,
			Message:  e.Message,
		})
	}
	return pbErrors
}

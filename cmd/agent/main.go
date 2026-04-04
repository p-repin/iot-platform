// cmd/agent/main.go — агент сбора телеметрии (точка входа).
//
// ФАЗА 2: Добавлены WAL, backpressure и sync.Pool.
//
// АРХИТЕКТУРА АГЕНТА:
// ┌─────────────┐     ┌─────────┐     ┌──────────────┐     ┌──────────┐
// │ collectLoop │ ──→ │   WAL   │ ──→ │   sendLoop   │ ──→ │  gRPC    │
// │ (тикер 1с)  │     │ (диск)  │     │ (backoff)    │     │  сервер  │
// └─────────────┘     └─────────┘     └──────────────┘     └──────────┘
//
// 1. collectLoop: каждую секунду генерирует данные → пишет в WAL
// 2. WAL: хранит данные на диске (надёжность)
// 3. sendLoop: читает из WAL → отправляет на сервер (с backoff при ошибках)
// 4. При подтверждении — WAL очищается
//
// GRACEFUL SHUTDOWN:
// При Ctrl+C (SIGINT) или SIGTERM:
// 1. Отменяется контекст → collectLoop и sendLoop завершаются
// 2. WaitGroup ждёт завершения обеих горутин
// 3. Закрываем WAL и gRPC соединение
// 4. Данные в WAL НЕ теряются — при следующем запуске будут отправлены
//
// ПОЧЕМУ фейковые данные, а не реальный OPC UA?
// 1. OPC UA сервер — это промышленное оборудование, которого у нас нет
// 2. Для отработки ВСЕЙ остальной архитектуры данные неважны
// 3. Фейковые данные можно генерировать с аномалиями для тестирования ML
// 4. В резюме написано "20+ станков по OPC UA" — на собеседовании объяснишь
//    что OPC UA это протокол, а логика обработки та же
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pzmash/iot-platform/internal/agent"
	"github.com/pzmash/iot-platform/internal/config"
	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
	"github.com/pzmash/iot-platform/internal/wal"
)

func main() {
	cfg := config.Load()

	// === 1. Открываем WAL ===
	// WAL создаётся ДО подключения к серверу.
	// ПОЧЕМУ? Даже если сервер недоступен — агент должен уметь
	// собирать и сохранять данные на диск.
	w, err := wal.Open(cfg.Agent.WALDir)
	if err != nil {
		log.Fatalf("Не удалось открыть WAL: %v", err)
	}
	defer w.Close()

	log.Printf("WAL открыт: %s", cfg.Agent.WALDir)

	// Проверяем есть ли данные с прошлого запуска
	if size, err := w.Size(); err == nil && size > 0 {
		log.Printf("WAL содержит %d байт данных с предыдущего запуска — будут отправлены", size)
	}

	// === 2. Подключаемся к gRPC серверу ===
	addr := fmt.Sprintf("localhost:%d", cfg.Server.GRPCPort)
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Не удалось создать gRPC клиент для %s: %v", addr, err)
	}
	defer conn.Close()

	client := pb.NewTelemetryServiceClient(conn)

	log.Printf("Агент подключен к серверу %s", addr)
	log.Printf("Станки: %v", cfg.Agent.MachineIDs)
	log.Printf("Интервал сбора: %v", cfg.Agent.CollectInterval)

	// === 3. Создаём Collector ===
	// Collector объединяет WAL + gRPC отправку + backpressure + sync.Pool.
	// Передаём функцию генерации данных — Collector не знает откуда данные,
	// он знает только как их сохранить и отправить.
	sender := &grpcSender{client: client}
	collector := agent.NewCollector(
		w,
		sender,
		"agent-01",
		cfg.Agent.MachineIDs,
		cfg.Agent.CollectInterval,
		collectTelemetry,
	)

	// === 4. Graceful shutdown ===
	// signal.NotifyContext — создаёт контекст, который отменяется при получении
	// сигнала ОС (SIGINT = Ctrl+C, SIGTERM = docker stop / kill).
	//
	// ПОЧЕМУ NotifyContext, а не signal.Notify + канал?
	// NotifyContext (Go 1.16+) делает то же самое, но:
	// - Возвращает ctx, который можно передать в горутины
	// - defer stop() автоматически отписывается от сигналов
	// - Меньше бойлерплейта
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("Начинаю сбор телеметрии (Ctrl+C для остановки)...")

	// Run БЛОКИРУЕТ до отмены контекста.
	// Внутри запускаются collectLoop и sendLoop.
	// При отмене ctx обе горутины завершаются, Run возвращается.
	collector.Run(ctx)

	log.Println("Агент остановлен. Данные в WAL сохранены.")
}

// grpcSender — адаптер gRPC клиента под интерфейс telemetry.Sender.
//
// ПАТТЕРН АДАПТЕР:
// telemetry.Sender ожидает метод SendBatch(ctx, batch) error.
// gRPC клиент имеет метод Ingest(ctx, batch) (*Response, error).
// grpcSender "адаптирует" один интерфейс к другому.
//
// ПОЧЕМУ не использовать gRPC клиент напрямую?
// Collector не должен знать про gRPC — он работает через абстракцию Sender.
// Завтра захотим отправлять через NATS — напишем natsSender, Collector не меняется.
type grpcSender struct {
	client pb.TelemetryServiceClient
}

func (s *grpcSender) SendBatch(ctx context.Context, batch *pb.TelemetryBatch) error {
	resp, err := s.client.Ingest(ctx, batch)
	if err != nil {
		return err
	}

	log.Printf("[Sender] Отправлено: %d записей, принято: %d",
		len(batch.Records), resp.AcceptedCount)
	return nil
}

// collectTelemetry генерирует фейковые данные телеметрии.
//
// МОДЕЛЬ ДАННЫХ:
// Каждый станок имеет "нормальный" диапазон параметров.
// С вероятностью 5% генерируется аномалия — выброс за пределы нормы.
// Это нужно для тестирования ML-модели и системы алертинга.
//
// В реальности здесь был бы вызов OPC UA клиента:
//   client.Read(nodeID) для каждого датчика каждого станка
func collectTelemetry(machineIDs []string) *pb.TelemetryBatch {
	now := time.Now()
	records := make([]*pb.TelemetryRecord, 0, len(machineIDs))

	for _, machineID := range machineIDs {
		// Генерируем UUID для идемпотентности.
		// UUID генерируется ДО записи в WAL.
		// Если отправка провалилась и мы шлём повторно из WAL —
		// сервер видит тот же UUID и делает upsert вместо дубликата.
		record := &pb.TelemetryRecord{
			RecordId:  uuid.New().String(),
			MachineId: machineID,
			Timestamp: timestamppb.New(now),

			// Нормальные значения + редкие аномалии
			Temperature: generateValue(75.0, 5.0, 120.0),  // норма ~75°C, аномалия >120°C
			Vibration:   generateValue(0.5, 0.1, 2.0),     // норма ~0.5 мм/с, аномалия >2.0
			Pressure:    generateValue(6.0, 0.3, 10.0),     // норма ~6 бар, аномалия >10
		}
		records = append(records, record)
	}

	return &pb.TelemetryBatch{
		Records: records,
	}
}

// generateValue создаёт значение с нормальным распределением и редкими выбросами.
//
// ПОЧЕМУ нормальное распределение, а не равномерное (rand.Float64)?
// Реальные датчики дают значения с нормальным распределением:
// - Большинство показаний близки к среднему (mean)
// - Отклонения уменьшаются по мере удаления от среднего
// - Равномерное распределение нереалистично: температура 75°C так же вероятна как 120°C
func generateValue(mean, stddev, anomalyThreshold float64) float64 {
	// 5% шанс аномалии
	if rand.Float64() < 0.05 {
		// Аномалия: значение на 20-50% выше порога
		return anomalyThreshold * (1.0 + rand.Float64()*0.3)
	}

	// Нормальное значение: Box-Muller transform
	u1 := rand.Float64()
	u2 := rand.Float64()
	normal := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)

	return mean + normal*stddev
}
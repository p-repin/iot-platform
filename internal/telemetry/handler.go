// Package telemetry — бизнес-логика обработки телеметрии.
//
// Этот пакет не знает ничего про gRPC, HTTP или NATS.
// Он работает с данными, а не с транспортом.
//
// ПОЧЕМУ бизнес-логика отделена от транспорта?
// Представь: сейчас данные приходят по gRPC.
// Завтра решили добавить приём через NATS.
// Если логика "записать в БД + проверить аномалию" внутри gRPC сервера —
// придётся дублировать её для NATS.
// Если логика в отдельном пакете — и gRPC, и NATS вызывают один HandleBatch.
package telemetry

import (
	"context"
	"log"

	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
)

// LogHandler — простая реализация обработчика, которая логирует данные.
//
// Это ВРЕМЕННАЯ реализация для проверки что gRPC пайплайн работает.
// В следующих фазах заменим на реальную: запись в TimescaleDB + ML инференс.
//
// ПАТТЕРН: начинаем с простейшей реализации, убеждаемся что "труба" работает,
// затем наращиваем функционал. Это лучше чем написать всё сразу и потом
// час искать баг — непонятно, проблема в gRPC или в записи в БД?
type LogHandler struct{}

// HandleBatch реализует интерфейс grpc.TelemetryHandler.
// Пока просто логирует каждую запись.
func (h *LogHandler) HandleBatch(ctx context.Context, records []*pb.TelemetryRecord) (int, []*pb.RecordError, error) {
	for _, r := range records {
		log.Printf("[Handler] machine=%s temp=%.1f vibr=%.3f press=%.1f id=%s",
			r.MachineId,
			r.Temperature,
			r.Vibration,
			r.Pressure,
			r.RecordId,
		)
	}

	// Все записи "приняты" (пока просто залогированы)
	return len(records), nil, nil
}
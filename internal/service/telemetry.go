// Package service — бизнес-логика платформы.
//
// Сервисный слой содержит ИНТЕРФЕЙСЫ и их реализации.
// Он зависит ТОЛЬКО от entity (доменные сущности).
// Он НЕ знает про gRPC, protobuf, HTTP или конкретную БД.
//
// ПОЧЕМУ интерфейс определён ЗДЕСЬ, а не в transport?
// Принцип инверсии зависимостей (Dependency Inversion, буква D в SOLID):
// - Абстракция принадлежит тому, кто её ИСПОЛЬЗУЕТ, а не тому, кто реализует.
// - Раньше TelemetryHandler был в transport/grpc — транспорт диктовал контракт.
// - Теперь TelemetryService в service — бизнес-слой определяет свой контракт,
//   а транспорт подстраивается.
//
// На собеседовании: "Как вы обеспечиваете слабую связанность между слоями?"
// Ответ: интерфейсы определяются в слое-потребителе, а не в слое-реализации.
package service

import (
	"context"
	"log"

	"github.com/pzmash/iot-platform/internal/entity"
)

// TelemetryService — интерфейс бизнес-логики обработки телеметрии.
//
// Транспортный слой (gRPC, NATS, HTTP) вызывает этот интерфейс.
// Реализация может быть любой:
// - LogService (временная, для отладки)
// - DBService (запись в TimescaleDB, Фаза 3)
// - MLService (инференс + запись, Фаза 4)
type TelemetryService interface {
	HandleBatch(ctx context.Context, batch entity.Batch) (accepted int, errors []entity.RecordError, err error)
}

// LogService — временная реализация, которая логирует данные.
//
// Используется пока нет подключения к TimescaleDB (до Фазы 3).
// Позволяет убедиться что весь пайплайн работает: агент → gRPC → service.
//
// ПАТТЕРН: начинаем с простейшей реализации, проверяем "трубу",
// потом подключаем реальное хранилище. Если сразу писать в БД —
// непонятно, баг в gRPC или в SQL-запросе?
type LogService struct{}

// HandleBatch логирует каждую запись из батча.
//
// В Фазе 3 здесь будет:
// 1. Вызов repository.InsertBatch (запись в TimescaleDB)
// 2. Возврат ошибок по отдельным записям (если upsert упал)
func (s *LogService) HandleBatch(ctx context.Context, batch entity.Batch) (int, []entity.RecordError, error) {
	for _, r := range batch.Records {
		log.Printf("[Service] machine=%s temp=%.1f vibr=%.3f press=%.1f id=%s",
			r.MachineID,
			r.Temperature,
			r.Vibration,
			r.Pressure,
			r.RecordID,
		)
	}

	// Все записи "приняты" (пока просто залогированы)
	return len(batch.Records), nil, nil
}

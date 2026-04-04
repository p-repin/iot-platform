// Package repository — интерфейсы доступа к данным.
//
// Репозиторий — это абстракция над хранилищем данных.
// Сервисный слой работает с интерфейсом, а не с конкретной БД.
//
// ПОЧЕМУ отдельный пакет для интерфейсов?
// 1. Чистая архитектура: service зависит от repository (интерфейс),
//    а конкретная реализация (PostgreSQL, MongoDB) — в отдельном пакете.
// 2. Тестируемость: в тестах подставляем мок-репозиторий.
// 3. Гибкость: замена TimescaleDB на другое хранилище —
//    пишем новую реализацию, service не меняется.
//
// НАПРАВЛЕНИЕ ЗАВИСИМОСТЕЙ:
// transport → service → repository (интерфейс) → entity
//                              ↑
//                   реализация (postgres, mock) тоже зависит от entity
//
// На собеседовании: "Как вы обеспечиваете тестируемость слоя данных?"
// Ответ: через интерфейс репозитория. В тестах — мок, в проде — pgxpool.
package repository

import (
	"context"

	"github.com/pzmash/iot-platform/internal/entity"
)

// TelemetryRepository — интерфейс для работы с хранилищем телеметрии.
//
// Реализации (будут в Фазе 3):
// - PostgresRepository: запись в TimescaleDB через pgxpool
// - MockRepository: для тестов (in-memory)
//
// ПОЧЕМУ InsertBatch, а не Insert(record)?
// Batch insert одной транзакцией в 10-100 раз быстрее
// чем N отдельных INSERT. При 20 станках × 1 запись/сек
// это критично для latency.
type TelemetryRepository interface {
	// InsertBatch записывает пакет записей в хранилище.
	// Реализация должна быть идемпотентной: ON CONFLICT (record_id, time) DO UPDATE.
	InsertBatch(ctx context.Context, records []entity.Record) error

	// Close освобождает ресурсы (закрывает пул соединений).
	// Вызывается при graceful shutdown сервера.
	Close()
}

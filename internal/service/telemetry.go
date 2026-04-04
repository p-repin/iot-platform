// Package service — бизнес-логика платформы.
//
// Сервисный слой содержит ИНТЕРФЕЙСЫ и их реализации.
// Он зависит от entity (доменные сущности) и repository (интерфейс хранилища).
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
	"log/slog"

	"github.com/pzmash/iot-platform/internal/entity"
	"github.com/pzmash/iot-platform/internal/repository"
)

// TelemetryService — интерфейс бизнес-логики обработки телеметрии.
//
// Транспортный слой (gRPC, NATS, HTTP) вызывает этот интерфейс.
// Реализация может быть любой:
// - LogService (временная, для отладки без БД)
// - DBService (запись в TimescaleDB)
// - В будущем: MLService (инференс + запись, Фаза 4)
type TelemetryService interface {
	HandleBatch(ctx context.Context, batch entity.Batch) (accepted int, errors []entity.RecordError, err error)
}

// ============================================================
// DBService — основная реализация: запись в TimescaleDB
// ============================================================

// DBService — сервис, который записывает телеметрию в БД через репозиторий.
//
// ПОЧЕМУ сервис не работает с pgxpool напрямую?
// Сервис зависит от ИНТЕРФЕЙСА repository.TelemetryRepository, а не от PostgreSQL.
// Это позволяет:
// 1. Тестировать сервис с мок-репозиторием (без реальной БД)
// 2. Заменить PostgreSQL на другую БД без изменения сервиса
// 3. Добавить кэширование (декоратор поверх репозитория) прозрачно
type DBService struct {
	repo repository.TelemetryRepository
}

// NewDBService создаёт сервис с указанным репозиторием.
//
// COMPOSITION ROOT (cmd/server/main.go) создаёт зависимости:
//   repo := repository.NewPostgresRepo(ctx, dsn)
//   svc  := service.NewDBService(repo)
//   srv  := grpc.NewServer(svc)
//
// Каждый слой получает свои зависимости через конструктор — это DI (Dependency Injection).
func NewDBService(repo repository.TelemetryRepository) *DBService {
	return &DBService{repo: repo}
}

// HandleBatch записывает батч телеметрии в БД.
//
// ЛОГИКА:
// 1. Логируем входящий батч (structured logging для DevOps)
// 2. Вызываем репозиторий для записи в БД
// 3. При ошибке — логируем и возвращаем (агент пришлёт повторно)
// 4. При успехе — возвращаем количество принятых записей
//
// В будущем здесь добавится:
// - Фаза 4: вызов ML-инференса после записи
// - Фаза 6: проверка алертинг-правил (temperature > 100 → warning)
func (s *DBService) HandleBatch(ctx context.Context, batch entity.Batch) (int, []entity.RecordError, error) {
	slog.Debug("Обработка батча",
		"agent_id", batch.AgentID,
		"records", len(batch.Records),
	)

	if err := s.repo.InsertBatch(ctx, batch.Records); err != nil {
		slog.Error("Ошибка записи батча в БД",
			"agent_id", batch.AgentID,
			"records", len(batch.Records),
			"error", err,
		)
		return 0, nil, err
	}

	slog.Info("Батч записан в БД",
		"agent_id", batch.AgentID,
		"records", len(batch.Records),
	)

	return len(batch.Records), nil, nil
}

// ============================================================
// LogService — отладочная реализация (без БД)
// ============================================================

// LogService — реализация для отладки, которая только логирует данные.
//
// Полезна для:
// - Запуска сервера без docker-compose (нет TimescaleDB)
// - Тестирования gRPC пайплайна изолированно от БД
// - Первоначальной проверки что "труба" работает
type LogService struct{}

// HandleBatch логирует каждую запись из батча.
func (s *LogService) HandleBatch(ctx context.Context, batch entity.Batch) (int, []entity.RecordError, error) {
	for _, r := range batch.Records {
		slog.Info("Телеметрия (log mode)",
			"machine_id", r.MachineID,
			"temperature", r.Temperature,
			"vibration", r.Vibration,
			"pressure", r.Pressure,
			"record_id", r.RecordID,
		)
	}

	return len(batch.Records), nil, nil
}

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Event — событие для Event Sourcing.
//
// ЗАЧЕМ Event Sourcing для результатов инференса?
// Каждый результат ML-модели — это событие в истории детали:
// "В 10:15:30 модель anomaly_v1 дала score 0.87 для станка CNC-07"
// При расследовании брака можно "проиграть" все события и увидеть,
// когда модель впервые заметила аномалию.
//
// Event — доменная сущность, но живёт в repository, а не в entity,
// потому что пока используется только для записи (нет бизнес-логики).
// Когда появится фаза 5 (Event Sourcing), Event переедет в entity.
type Event struct {
	EventType   string    // тип события: "InferenceCompleted"
	AggregateID string    // UUID агрегата (деталь или станок)
	Payload     any       // данные события (сериализуется в JSONB)
	CreatedAt   time.Time // время события
}

// EventRepository — интерфейс для записи событий.
//
// Отделён от TelemetryRepository:
// - Разная ответственность (SRP): телеметрия — данные, события — аудит
// - Разные паттерны доступа: телеметрия — batch upsert, события — append-only
// - В будущем события могут переехать в отдельное хранилище (EventStoreDB)
type EventRepository interface {
	// InsertEvent записывает одно событие в append-only журнал.
	InsertEvent(ctx context.Context, event Event) error
	// Close освобождает ресурсы.
	Close()
}

// PostgresEventRepo — реализация EventRepository для PostgreSQL.
//
// Использует тот же pgxpool, что и PostgresRepo (telemetry).
// Пул соединений общий — это нормально, pgxpool потокобезопасен.
type PostgresEventRepo struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPostgresEventRepo создаёт репозиторий событий.
//
// pool — тот же пул, что используется для телеметрии.
// Создавать отдельный пул для событий — перерасход ресурсов:
// события пишутся редко (только при аномалиях), а каждый пул
// держит min 4 соединения к PostgreSQL.
func NewPostgresEventRepo(pool *pgxpool.Pool, logger *slog.Logger) *PostgresEventRepo {
	return &PostgresEventRepo{
		pool:   pool,
		logger: logger,
	}
}

// InsertEvent записывает событие в таблицу events.
//
// ПОЧЕМУ не batch insert?
// События инференса возникают поштучно (при обнаружении аномалии).
// Батчить их нет смысла — аномалий мало (иначе что-то сильно не так с оборудованием).
//
// ПОЧЕМУ JSONB для payload?
// Каждый тип события имеет свою структуру данных:
// InferenceCompleted: {model: "anomaly_v1", score: 0.87, threshold: 0.7}
// PartCreated: {material: "steel", batch: "B-42"}
// JSONB позволяет хранить произвольную структуру без ALTER TABLE.
func (r *PostgresEventRepo) InsertEvent(ctx context.Context, event Event) error {
	// Сериализуем payload в JSON для записи в JSONB-колонку
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("ошибка сериализации payload: %w", err)
	}

	query := `
		INSERT INTO events (event_type, aggregate_id, payload, created_at)
		VALUES ($1, $2::uuid, $3::jsonb, $4)
	`

	_, err = r.pool.Exec(ctx, query,
		event.EventType,
		event.AggregateID,
		string(payloadJSON),
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("ошибка записи события %s: %w", event.EventType, err)
	}

	r.logger.Info("событие записано",
		"event_type", event.EventType,
		"aggregate_id", event.AggregateID,
	)

	return nil
}

// Close — no-op, т.к. пул общий с TelemetryRepository.
// Закрытие пула — ответственность main.go (Composition Root).
func (r *PostgresEventRepo) Close() {
	// Пул закрывается в main.go, не здесь
}

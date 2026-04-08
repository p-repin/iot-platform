package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================
// EventStore — интерфейс хранилища событий
// ============================================================

// EventStore — append-only хранилище событий.
//
// ПОЧЕМУ интерфейс определён ЗДЕСЬ (в events), а не в repository?
// Принцип инверсии зависимостей (Dependency Inversion):
// интерфейс принадлежит ПОТРЕБИТЕЛЮ, а не реализации.
// Пакет events использует EventStore для Rebuild агрегатов —
// значит, он и определяет контракт.
//
// На собеседовании: "Как устроен ваш Event Store?"
// Ответ: append-only журнал в PostgreSQL. Два метода:
// Append (запись) и LoadEvents (чтение для восстановления состояния).
// Событие неизменяемо — никаких UPDATE, только INSERT.
type EventStore interface {
	// Append записывает событие в журнал.
	// Возвращает присвоенный event_id (BIGSERIAL из БД).
	// Событие записывается атомарно — либо всё, либо ничего.
	Append(ctx context.Context, event DomainEvent) (int64, error)

	// LoadEvents загружает ВСЕ события агрегата в порядке event_id.
	// Порядок критически важен: PartCreated должен идти раньше PartMachined.
	// Используется для восстановления текущего состояния агрегата (Rebuild).
	LoadEvents(ctx context.Context, aggregateID string) ([]DomainEvent, error)
}

// ============================================================
// PostgresEventStore — реализация для PostgreSQL
// ============================================================

// PostgresEventStore — реализация EventStore на PostgreSQL.
//
// Использует тот же pgxpool, что и TelemetryRepository.
// ПОЧЕМУ общий пул?
// - pgxpool потокобезопасен (можно вызывать из разных горутин)
// - События пишутся редко (при аномалиях + симулятор) — отдельный пул = перерасход
// - Каждый пул держит min 4 TCP-соединения к PostgreSQL
type PostgresEventStore struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPostgresEventStore создаёт хранилище событий.
//
// pool — общий пул соединений из PostgresRepo (Composition Root передаёт через repo.Pool()).
// logger — для structured logging (JSON, парсится Promtail/Loki).
func NewPostgresEventStore(pool *pgxpool.Pool, logger *slog.Logger) *PostgresEventStore {
	return &PostgresEventStore{
		pool:   pool,
		logger: logger,
	}
}

// Append записывает одно событие в таблицу events.
//
// ПОЧЕМУ QueryRow, а не Exec?
// Нам нужен event_id, который генерирует БД (BIGSERIAL).
// RETURNING event_id возвращает его — QueryRow читает результат.
// Exec не возвращает данные из RETURNING.
//
// ПОЧЕМУ не batch insert?
// События создаются поштучно (по одному на операцию).
// Batch insert нужен для массовой загрузки — не наш случай.
func (s *PostgresEventStore) Append(ctx context.Context, event DomainEvent) (int64, error) {
	// Сериализуем payload в JSON для JSONB-колонки
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return 0, fmt.Errorf("ошибка сериализации payload: %w", err)
	}

	query := `
		INSERT INTO events (event_type, aggregate_id, payload, created_at)
		VALUES ($1, $2::uuid, $3::jsonb, $4)
		RETURNING event_id
	`

	var eventID int64
	err = s.pool.QueryRow(ctx, query,
		string(event.EventType),
		event.AggregateID,
		string(payloadJSON),
		event.CreatedAt,
	).Scan(&eventID)

	if err != nil {
		return 0, fmt.Errorf("ошибка записи события %s: %w", event.EventType, err)
	}

	s.logger.Info("событие записано",
		"event_id", eventID,
		"event_type", event.EventType,
		"aggregate_id", event.AggregateID,
	)

	return eventID, nil
}

// LoadEvents загружает все события агрегата из БД.
//
// ПОРЯДОК: ORDER BY event_id — гарантирует хронологический порядок.
// BIGSERIAL монотонно растёт, поэтому event_id = порядок записи.
//
// ДЕСЕРИАЛИЗАЦИЯ: payload хранится как JSONB (произвольная структура).
// При чтении мы знаем event_type → знаем в какой struct десериализовать.
// deserializePayload делает switch по event_type.
//
// На собеседовании: "Как вы восстанавливаете состояние агрегата?"
// Ответ: SELECT * FROM events WHERE aggregate_id = ? ORDER BY event_id,
// затем for each event → aggregate.Apply(event).
func (s *PostgresEventStore) LoadEvents(ctx context.Context, aggregateID string) ([]DomainEvent, error) {
	query := `
		SELECT event_id, event_type, aggregate_id, payload, created_at
		FROM events
		WHERE aggregate_id = $1::uuid
		ORDER BY event_id
	`

	rows, err := s.pool.Query(ctx, query, aggregateID)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки событий для %s: %w", aggregateID, err)
	}
	defer rows.Close()

	var events []DomainEvent

	for rows.Next() {
		var (
			eventID     int64
			eventType   string
			aggID       string
			payloadJSON []byte
			createdAt   time.Time
		)

		if err := rows.Scan(&eventID, &eventType, &aggID, &payloadJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("ошибка чтения строки события: %w", err)
		}

		// Десериализуем JSONB payload в типизированную структуру
		payload, err := deserializePayload(EventType(eventType), payloadJSON)
		if err != nil {
			// Не падаем на неизвестном типе — логируем и пропускаем.
			// ПОЧЕМУ не падаем? В проде могут быть старые события с устаревшими типами.
			// Лучше восстановить частичное состояние, чем не восстановить ничего.
			s.logger.Warn("неизвестный тип события, пропускаю",
				"event_id", eventID,
				"event_type", eventType,
				"error", err,
			)
			continue
		}

		events = append(events, DomainEvent{
			EventID:     eventID,
			EventType:   EventType(eventType),
			AggregateID: aggID,
			Payload:     payload,
			CreatedAt:   createdAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка итерации по событиям: %w", err)
	}

	return events, nil
}

// deserializePayload десериализует JSONB в типизированную структуру.
//
// ПОЧЕМУ switch, а не map[EventType]func?
// У нас 4 типа событий — switch читается проще.
// Когда типов станет >10, можно перейти на registry-паттерн.
//
// ПОЧЕМУ возвращаем указатель (*PartCreatedPayload)?
// Aggregate.Apply использует type assertion: payload.(*PartCreatedPayload).
// Указатель позволяет избежать копирования и единообразен с Go-идиомами.
func deserializePayload(eventType EventType, raw []byte) (any, error) {
	switch eventType {
	case PartCreated:
		var p PartCreatedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("ошибка десериализации PartCreated: %w", err)
		}
		return &p, nil

	case PartMachined:
		var p PartMachinedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("ошибка десериализации PartMachined: %w", err)
		}
		return &p, nil

	case InferenceCompleted:
		var p InferenceCompletedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("ошибка десериализации InferenceCompleted: %w", err)
		}
		return &p, nil

	case PartShipped:
		var p PartShippedPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("ошибка десериализации PartShipped: %w", err)
		}
		return &p, nil

	default:
		return nil, fmt.Errorf("неизвестный тип события: %s", eventType)
	}
}

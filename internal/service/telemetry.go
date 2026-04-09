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
	"time"

	"github.com/google/uuid"
	"github.com/pzmash/iot-platform/internal/alerting"
	"github.com/pzmash/iot-platform/internal/entity"
	"github.com/pzmash/iot-platform/internal/events"
	"github.com/pzmash/iot-platform/internal/inference"
	"github.com/pzmash/iot-platform/internal/metrics"
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
	// inference — ML-сервис для обнаружения аномалий (опционален).
	// Если nil — сервер работает без ML (graceful degradation).
	// ПОЧЕМУ опционален?
	// - Для dev-окружения без ONNX Runtime
	// - Для запуска без обученной модели (первый деплой)
	// - ML — дополнение к pipeline, а не обязательный шаг
	inference *inference.Service
	// eventStore — хранилище событий Event Sourcing (Фаза 5).
	// Аномалии записываются как InferenceCompleted в append-only журнал.
	eventStore events.EventStore
	// alertEngine — движок проверки правил алертинга (Фаза 6).
	// Проверяет каждую запись: temperature > 100 → warning и т.д.
	alertEngine *alerting.Engine
	// alertNotifier — отправка алертов в Redis pub/sub + сохранение critical в БД.
	alertNotifier *alerting.Notifier
}

// NewDBService создаёт сервис с указанным репозиторием.
//
// COMPOSITION ROOT (cmd/server/main.go) создаёт зависимости:
//   repo := repository.NewPostgresRepo(ctx, dsn)
//   svc  := service.NewDBService(repo, inferSvc, eventStore, engine, notifier)
//   srv  := grpc.NewServer(svc)
//
// Каждый слой получает свои зависимости через конструктор — это DI (Dependency Injection).
// inference, eventStore, alertEngine, alertNotifier могут быть nil — graceful degradation.
func NewDBService(
	repo repository.TelemetryRepository,
	inferSvc *inference.Service,
	eventStore events.EventStore,
	alertEngine *alerting.Engine,
	alertNotifier *alerting.Notifier,
) *DBService {
	return &DBService{
		repo:          repo,
		inference:     inferSvc,
		eventStore:    eventStore,
		alertEngine:   alertEngine,
		alertNotifier: alertNotifier,
	}
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
	// === Prometheus метрики (Фаза 6) ===
	// Замеряем общее время обработки батча (БД + инференс + алертинг).
	// time.Since в defer гарантирует замер даже при ошибке.
	start := time.Now()
	defer func() {
		metrics.BatchDuration.Observe(time.Since(start).Seconds())
	}()

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
		metrics.ErrorsTotal.WithLabelValues("db_write").Inc()
		return 0, nil, err
	}

	// Обновляем счётчики Prometheus
	metrics.RecordsTotal.Add(float64(len(batch.Records)))
	metrics.BatchesTotal.Inc()

	slog.Info("Батч записан в БД",
		"agent_id", batch.AgentID,
		"records", len(batch.Records),
	)

	// === ML-инференс (Фаза 4) + Event Sourcing (Фаза 5) ===
	// Вызываем ПОСЛЕ успешной записи в БД.
	// ПОЧЕМУ после, а не до?
	// 1. Данные уже сохранены — даже если ML упадёт, телеметрия не потеряна
	// 2. ML — дополнение, а не критический путь (best-effort)
	// 3. При ошибке инференса мы логируем, но не возвращаем ошибку агенту
	if s.inference != nil {
		inferStart := time.Now()
		results := s.inference.Analyze(ctx, batch.Records)
		metrics.InferenceDuration.Observe(time.Since(inferStart).Seconds())

		// Записываем аномалии в Event Sourcing журнал через EventStore.
		for _, r := range results {
			if r.IsAnomaly && s.eventStore != nil {
				event := events.DomainEvent{
					EventType:   events.InferenceCompleted,
					AggregateID: uuid.New().String(),
					Payload: &events.InferenceCompletedPayload{
						MachineID: r.MachineID,
						Score:     r.Score,
						IsAnomaly: r.IsAnomaly,
						Model:     "anomaly_detector_v1",
					},
					CreatedAt: r.Timestamp,
				}
				if _, err := s.eventStore.Append(ctx, event); err != nil {
					slog.Error("Ошибка записи события инференса",
						"machine_id", r.MachineID,
						"error", err,
					)
					metrics.ErrorsTotal.WithLabelValues("event_store").Inc()
				}
			}
		}
	}

	// === Публикация телеметрии на дашборд (Фаза 6) ===
	// Отправляем показания станков через Redis pub/sub → WebSocket → браузер.
	// Оператор видит текущие значения температуры, вибрации, давления.
	if s.alertNotifier != nil {
		s.alertNotifier.PublishTelemetry(ctx, batch.Records)
	}

	// === Алертинг (Фаза 6) ===
	// Проверяем каждую запись батча по правилам алертинга.
	// ПОЧЕМУ после записи в БД и инференса?
	// 1. Данные уже сохранены — алертинг не блокирует основной pipeline
	// 2. Алерты — best-effort: если Redis недоступен, данные не теряются
	// 3. В будущем можно комбинировать: ML-аномалия + превышение порога = higher severity
	if s.alertEngine != nil && s.alertNotifier != nil {
		alerts := s.alertEngine.EvaluateBatch(batch.Records)
		if len(alerts) > 0 {
			s.alertNotifier.Notify(ctx, alerts)

			// Обновляем Prometheus-счётчик алертов по severity
			for _, a := range alerts {
				metrics.AlertsTotal.WithLabelValues(string(a.Severity)).Inc()
			}

			slog.Info("Алерты обработаны",
				"count", len(alerts),
				"agent_id", batch.AgentID,
			)
		}
	}

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

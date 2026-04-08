package events

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// ============================================================
// PartSimulator — симулятор жизненного цикла деталей
// ============================================================

// PartSimulator генерирует синтетический жизненный цикл деталей.
//
// ЗАЧЕМ симулятор?
// В реальном производстве деталь проходит цикл за часы/дни.
// Для демонстрации Event Sourcing нам нужно видеть полный цикл
// за секунды: PartCreated → PartMachined → InferenceCompleted → PartShipped.
//
// Симулятор:
// 1. Создаёт деталь (PartCreated)
// 2. "Обрабатывает" на 1-3 станках (PartMachined)
// 3. "Прогоняет" ML-инференс (InferenceCompleted)
// 4. "Отгружает" (PartShipped)
// 5. Восстанавливает состояние из БД (LoadEvents + Rebuild) и логирует
//
// Шаг 5 — ключевой: он ДОКАЗЫВАЕТ что Event Sourcing работает.
// Мы записали события, а потом восстановили состояние — и оно корректно.
//
// На собеседовании: "Покажите как работает Event Sourcing"
// Ответ: запускаем сервер, в логах видим полный цикл детали + восстановление.
type PartSimulator struct {
	store    EventStore
	interval time.Duration
	logger   *slog.Logger
}

// NewPartSimulator создаёт симулятор.
//
// store — EventStore для записи/чтения событий.
// interval — интервал между генерацией деталей (по умолчанию 10с).
func NewPartSimulator(store EventStore, interval time.Duration, logger *slog.Logger) *PartSimulator {
	return &PartSimulator{
		store:    store,
		interval: interval,
		logger:   logger,
	}
}

// Run запускает симулятор. Блокирует до отмены контекста.
//
// Запускается в отдельной горутине из main.go:
//
//	go simulator.Run(ctx)
//
// При получении SIGTERM контекст отменяется → цикл завершается.
func (s *PartSimulator) Run(ctx context.Context) {
	s.logger.Info("симулятор деталей запущен",
		"interval", s.interval,
	)

	// Первая деталь — сразу при старте (чтобы в логах было видно)
	s.simulatePartLifecycle(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("симулятор деталей остановлен")
			return
		case <-ticker.C:
			s.simulatePartLifecycle(ctx)
		}
	}
}

// machines — список станков для симуляции.
// Совпадает с MachineIDs из конфига агента.
var machines = []string{"CNC-01", "CNC-02", "CNC-03", "LATHE-01", "LATHE-02", "MILL-01"}

// operations — типы операций для каждого типа станка.
var operations = map[string]string{
	"CNC":   "milling",  // фрезерование
	"LATHE": "turning",  // токарка
	"MILL":  "grinding", // шлифовка
}

// materials — материалы для случайного выбора.
var materials = []string{"steel", "aluminum", "titanium", "brass"}

// destinations — пункты отгрузки.
var destinations = []string{"Склад-А", "Склад-Б", "Заказчик-Ростех", "Заказчик-ОДК"}

// simulatePartLifecycle генерирует полный жизненный цикл одной детали.
//
// ПОРЯДОК СОБЫТИЙ:
// 1. PartCreated       — деталь создана из материала X, партия Y
// 2. PartMachined (1-3)— обработка на станках (случайное количество)
// 3. InferenceCompleted— ML-модель проверила показания
// 4. PartShipped       — отгрузка (90% проходят контроль, 10% брак)
//
// ПОСЛЕ записи — LoadEvents + Rebuild для проверки.
func (s *PartSimulator) simulatePartLifecycle(ctx context.Context) {
	partID := uuid.New().String()
	now := time.Now()

	s.logger.Info("=== Начало жизненного цикла детали ===",
		"part_id", partID,
	)

	// --- 1. PartCreated ---
	partNumber := fmt.Sprintf("P-%s", partID[:8])
	material := materials[rand.Intn(len(materials))]
	batchID := fmt.Sprintf("B-%03d", rand.Intn(999)+1)

	_, err := s.store.Append(ctx, DomainEvent{
		EventType:   PartCreated,
		AggregateID: partID,
		Payload: &PartCreatedPayload{
			PartNumber: partNumber,
			Material:   material,
			BatchID:    batchID,
		},
		CreatedAt: now,
	})
	if err != nil {
		s.logger.Error("ошибка создания детали", "error", err)
		return
	}

	// --- 2. PartMachined (1-3 операции) ---
	// ПОЧЕМУ случайное количество операций?
	// В реальности деталь может проходить от 1 до 5+ обработок.
	// Случайность делает демонстрацию более реалистичной.
	numOps := rand.Intn(3) + 1 // 1, 2 или 3 операции
	for i := 0; i < numOps; i++ {
		machine := machines[rand.Intn(len(machines))]
		// Определяем операцию по типу станка (CNC→milling, LATHE→turning, MILL→grinding)
		op := operationForMachine(machine)
		duration := 30.0 + rand.Float64()*150.0 // 30-180 секунд

		_, err := s.store.Append(ctx, DomainEvent{
			EventType:   PartMachined,
			AggregateID: partID,
			Payload: &PartMachinedPayload{
				MachineID:   machine,
				Operation:   op,
				DurationSec: duration,
			},
			CreatedAt: now.Add(time.Duration(i+1) * time.Minute), // +1 мин на каждую операцию
		})
		if err != nil {
			s.logger.Error("ошибка записи обработки", "error", err)
			return
		}
	}

	// --- 3. InferenceCompleted ---
	// Симулируем результат ML-модели.
	// Score случайный: большинство деталей нормальные (score < 0.7),
	// но иногда попадаются аномалии (score > 0.7).
	score := rand.Float64() // 0..1
	isAnomaly := score > 0.7
	lastMachine := machines[rand.Intn(len(machines))]

	_, err = s.store.Append(ctx, DomainEvent{
		EventType:   InferenceCompleted,
		AggregateID: partID,
		Payload: &InferenceCompletedPayload{
			MachineID: lastMachine,
			Score:     score,
			IsAnomaly: isAnomaly,
			Model:     "anomaly_detector_v1",
		},
		CreatedAt: now.Add(time.Duration(numOps+1) * time.Minute),
	})
	if err != nil {
		s.logger.Error("ошибка записи инференса", "error", err)
		return
	}

	// --- 4. PartShipped ---
	// 90% деталей проходят контроль, 10% — брак.
	// ПОЧЕМУ 10% брака?
	// Реалистичный показатель для демонстрации. При расследовании брака
	// можно "проиграть" все события и найти на каком этапе что-то пошло не так.
	passed := rand.Float64() > 0.1
	destination := destinations[rand.Intn(len(destinations))]

	_, err = s.store.Append(ctx, DomainEvent{
		EventType:   PartShipped,
		AggregateID: partID,
		Payload: &PartShippedPayload{
			Destination: destination,
			InspectorID: fmt.Sprintf("inspector-%02d", rand.Intn(10)+1),
			Passed:      passed,
		},
		CreatedAt: now.Add(time.Duration(numOps+2) * time.Minute),
	})
	if err != nil {
		s.logger.Error("ошибка записи отгрузки", "error", err)
		return
	}

	// --- 5. Восстановление состояния (PROOF of Event Sourcing) ---
	// Загружаем события из БД и восстанавливаем состояние.
	// Это ДОКАЗЫВАЕТ что Event Sourcing работает:
	// мы не хранили состояние явно — мы восстановили его из событий.
	loadedEvents, err := s.store.LoadEvents(ctx, partID)
	if err != nil {
		s.logger.Error("ошибка загрузки событий", "error", err)
		return
	}

	part, err := Rebuild(loadedEvents)
	if err != nil {
		s.logger.Error("ошибка восстановления состояния", "error", err)
		return
	}

	// Формируем отчёт
	status := "✓ годна"
	if !passed {
		status = "✗ БРАК"
	}

	s.logger.Info("=== Деталь прослежена (Event Sourcing) ===",
		"part_number", part.PartNumber,
		"material", part.Material,
		"batch_id", part.BatchID,
		"state", part.State,
		"operations", len(part.MachineOps),
		"inspections", len(part.Inspections),
		"anomaly_detected", isAnomaly,
		"quality", status,
		"destination", destination,
		"version", part.Version,
		"events_total", len(loadedEvents),
	)
}

// operationForMachine определяет тип операции по префиксу станка.
//
// CNC-01 → "milling", LATHE-02 → "turning", MILL-01 → "grinding".
// Если префикс неизвестен — "processing" (запасной вариант).
func operationForMachine(machineID string) string {
	for prefix, op := range operations {
		if len(machineID) >= len(prefix) && machineID[:len(prefix)] == prefix {
			return op
		}
	}
	return "processing"
}

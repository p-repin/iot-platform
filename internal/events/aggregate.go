package events

import "fmt"

// ============================================================
// Part — агрегат Event Sourcing
// ============================================================

// PartState — текущее состояние детали в жизненном цикле.
//
// Состояния образуют конечный автомат (state machine):
//
//	"" (пусто) → PartCreated  → StateCreated
//	StateCreated  → PartMachined → StateMachined
//	StateMachined → PartMachined → StateMachined  (несколько обработок)
//	StateMachined → PartShipped  → StateShipped
//
// InferenceCompleted НЕ меняет состояние — это "побочное" событие,
// результат ML-анализа, который не влияет на физический lifecycle детали.
//
// На собеседовании: "Какой паттерн вы используете для управления состоянием?"
// Ответ: State Machine через Event Sourcing — каждое событие = переход.
// Текущее состояние не хранится явно, а восстанавливается из журнала.
type PartState string

const (
	// StateCreated — деталь создана, ещё не обработана.
	StateCreated PartState = "created"
	// StateMachined — деталь прошла обработку на станке (одну или несколько).
	StateMachined PartState = "machined"
	// StateShipped — деталь отгружена заказчику (финальное состояние).
	StateShipped PartState = "shipped"
)

// MachineOp — запись об одной операции обработки на станке.
//
// Деталь может пройти несколько операций на разных станках:
// CNC-01 (фрезерование, 120с) → LATHE-01 (токарка, 90с)
// Каждая операция сохраняется для полной прослеживаемости.
type MachineOp struct {
	MachineID   string  // ID станка
	Operation   string  // тип операции
	DurationSec float64 // длительность в секундах
}

// Part — агрегат: деталь и её полная история производства.
//
// ЧТО ТАКОЕ АГРЕГАТ в Event Sourcing?
// Агрегат — это сущность, состояние которой определяется
// последовательностью событий. У агрегата есть:
// 1. Идентификатор (aggregate_id) — UUID детали
// 2. Текущее состояние — восстанавливается через Apply
// 3. Версия — количество применённых событий (для оптимистичной блокировки)
//
// ПОЧЕМУ состояние не хранится в отдельной таблице?
// При Event Sourcing "source of truth" — это события, а не таблица состояний.
// Если нужна таблица для быстрых запросов — это CQRS (read model),
// которая строится ИЗ событий, но не является источником истины.
//
// На собеседовании: "Как восстановить состояние детали?"
// Ответ: SELECT * FROM events WHERE aggregate_id = ? ORDER BY event_id,
// затем part := Rebuild(events) — прогоняем каждое событие через Apply.
type Part struct {
	ID         string     // aggregate_id (UUID детали)
	State      PartState  // текущее состояние
	PartNumber string     // номер детали (из PartCreated)
	Material   string     // материал (из PartCreated)
	BatchID    string     // ID партии материала (из PartCreated)

	// MachineOps — история всех обработок на станках.
	// Порядок важен: первая обработка = первый элемент.
	MachineOps []MachineOp

	// Inspections — результаты всех ML-проверок.
	// Деталь может проверяться на каждом этапе обработки.
	Inspections []InferenceCompletedPayload

	// ShipmentInfo — данные отгрузки (nil если деталь ещё не отгружена).
	ShipmentInfo *PartShippedPayload

	// Version — количество применённых событий.
	// ЗАЧЕМ?
	// 1. Оптимистичная блокировка: при конкурентной записи проверяем
	//    что версия не изменилась (пока не реализовано, но паттерн заложен)
	// 2. Дебаг: сразу видно сколько событий у детали
	Version int
}

// Apply применяет одно событие к агрегату, обновляя состояние.
//
// ПОЧЕМУ не просто обновлять поля напрямую?
// Apply — это единственная точка изменения состояния.
// Все бизнес-правила переходов сконцентрированы здесь:
// - Нельзя обработать деталь, которая ещё не создана
// - Нельзя отгрузить деталь, которая не обработана
// - Нельзя создать деталь дважды
//
// Это гарантирует КОНСИСТЕНТНОСТЬ: если события записаны в журнал,
// при воспроизведении мы всегда получим одинаковое состояние.
func (p *Part) Apply(event DomainEvent) error {
	switch event.EventType {

	case PartCreated:
		// Первое событие — деталь рождается
		if p.State != "" {
			return fmt.Errorf("деталь %s уже создана, текущее состояние: %s", p.ID, p.State)
		}
		payload, ok := event.Payload.(*PartCreatedPayload)
		if !ok {
			return fmt.Errorf("невалидный payload для PartCreated")
		}
		p.ID = event.AggregateID
		p.State = StateCreated
		p.PartNumber = payload.PartNumber
		p.Material = payload.Material
		p.BatchID = payload.BatchID

	case PartMachined:
		// Обработка возможна только для созданной/уже обработанной детали.
		// StateMachined → PartMachined допускается: деталь проходит несколько станков.
		if p.State != StateCreated && p.State != StateMachined {
			return fmt.Errorf("невозможно обработать деталь в состоянии: %s", p.State)
		}
		payload, ok := event.Payload.(*PartMachinedPayload)
		if !ok {
			return fmt.Errorf("невалидный payload для PartMachined")
		}
		p.State = StateMachined
		p.MachineOps = append(p.MachineOps, MachineOp{
			MachineID:   payload.MachineID,
			Operation:   payload.Operation,
			DurationSec: payload.DurationSec,
		})

	case InferenceCompleted:
		// ML-проверка возможна в любом состоянии (кроме пустого).
		// InferenceCompleted НЕ меняет State — это побочное событие.
		// ПОЧЕМУ?
		// ML-инференс — это наблюдение, а не действие над деталью.
		// Деталь не "становится проверенной" — она продолжает обработку.
		if p.State == "" {
			return fmt.Errorf("невозможно выполнить инференс для несуществующей детали")
		}
		payload, ok := event.Payload.(*InferenceCompletedPayload)
		if !ok {
			return fmt.Errorf("невалидный payload для InferenceCompleted")
		}
		p.Inspections = append(p.Inspections, *payload)

	case PartShipped:
		// Отгрузка возможна только после обработки.
		// ПОЧЕМУ нельзя отгрузить из StateCreated?
		// Необработанная деталь — это заготовка. Отгружать заготовку бессмысленно.
		if p.State != StateMachined {
			return fmt.Errorf("отгрузка возможна только после обработки, текущее состояние: %s", p.State)
		}
		payload, ok := event.Payload.(*PartShippedPayload)
		if !ok {
			return fmt.Errorf("невалидный payload для PartShipped")
		}
		p.State = StateShipped
		p.ShipmentInfo = payload

	default:
		return fmt.Errorf("неизвестный тип события: %s", event.EventType)
	}

	p.Version++
	return nil
}

// Rebuild восстанавливает состояние детали из списка событий.
//
// Это КЛЮЧЕВАЯ операция Event Sourcing: "проигрываем" все события
// от первого до последнего и получаем текущее состояние.
//
// ПРИМЕР:
//
//	events := store.LoadEvents(ctx, "part-uuid-123")
//	part, err := Rebuild(events)
//	// part.State == StateShipped
//	// part.MachineOps == [{CNC-01, milling, 120}, {LATHE-01, turning, 90}]
//	// part.Version == 5 (PartCreated + 2x PartMachined + InferenceCompleted + PartShipped)
//
// На собеседовании: "А если событий миллион, Rebuild будет медленным?"
// Ответ: Да. Решения:
// 1. Снэпшоты (snapshots): сохраняем состояние каждые N событий,
//    при Rebuild начинаем с последнего снэпшота
// 2. CQRS: для чтения используем отдельную read-модель (обычная таблица),
//    которая обновляется при каждом новом событии
// Для 20 станков и тысяч деталей это не проблема — Rebuild мгновенный.
func Rebuild(events []DomainEvent) (*Part, error) {
	part := &Part{}
	for _, e := range events {
		if err := part.Apply(e); err != nil {
			return nil, fmt.Errorf("ошибка при воспроизведении события #%d (%s): %w",
				e.EventID, e.EventType, err)
		}
	}
	return part, nil
}

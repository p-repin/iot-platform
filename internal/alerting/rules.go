// Package alerting — система алертинга для IoT-платформы.
//
// ЗАЧЕМ алертинг?
// Телеметрия без алертинга — это просто данные. Оператор не может
// круглосуточно смотреть на графики 20 станков. Алертинг автоматически
// обнаруживает аномалии и уведомляет оператора.
//
// ТРИ УРОВНЯ КРИТИЧНОСТИ (стандарт в DevOps/мониторинге):
// - info:     параметр приближается к границе, пока не требует действий
// - warning:  параметр вышел за soft limit, нужно обратить внимание
// - critical: требуется немедленное вмешательство (остановка, проверка)
//
// На собеседовании: "Как вы реализовали алертинг?"
// Ответ: Rule-based engine с тремя уровнями severity.
// Правила конфигурируемые (без пересборки). Critical алерты сохраняются в БД,
// все алерты публикуются через Redis pub/sub для WebSocket-дашборда.
package alerting

import (
	"fmt"
	"time"

	"github.com/pzmash/iot-platform/internal/entity"
)

// Severity — уровень критичности алерта.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Alert — сработавший алерт.
//
// Содержит всю информацию для оператора:
// какой станок, что случилось, насколько критично.
// Сохраняется в БД (critical) и публикуется через Redis pub/sub (все уровни).
type Alert struct {
	MachineID string    `json:"machine_id"`
	Severity  Severity  `json:"severity"`
	Message   string    `json:"message"`
	Value     float64   `json:"value"`     // фактическое значение параметра
	Threshold float64   `json:"threshold"` // порог, который был превышен
	Parameter string    `json:"parameter"` // какой параметр ("temperature", "vibration", "pressure")
	CreatedAt time.Time `json:"created_at"`
}

// Rule — одно правило алертинга.
//
// ПОЧЕМУ структура, а не функция?
// Структуру можно сериализовать (в конфиг/БД), логировать, отображать на дашборде.
// Функцию — нет. В будущем правила можно будет менять через UI без пересборки.
type Rule struct {
	Parameter string   // "temperature", "vibration", "pressure"
	Severity  Severity // уровень при срабатывании
	Threshold float64  // порог: значение > threshold → алерт
}

// DefaultRules — набор правил по умолчанию для производственного оборудования.
//
// ПОЧЕМУ именно такие пороги?
// - Температура:
//   > 90°C  (info)     — станок нагревается, стоит наблюдать
//   > 100°C (warning)  — перегрев, нужна проверка охлаждения
//   > 120°C (critical) — риск повреждения оборудования, остановка
//
// - Вибрация:
//   > 8 мм/с  (info)     — повышенная вибрация, возможен износ
//   > 12 мм/с (warning)  — вибрация опасная, проверить крепления
//   > 15 мм/с (critical) — остановить станок, износ подшипников
//
// - Давление:
//   > 9 бар   (info)     — давление приближается к пределу
//   > 10 бар  (warning)  — превышение рабочего давления
//   > 12 бар  (critical) — риск разрушения пневмосистемы
//
// В реальном проекте пороги настраиваются для каждого типа станка.
func DefaultRules() []Rule {
	return []Rule{
		// Температура
		{Parameter: "temperature", Severity: SeverityInfo, Threshold: 90},
		{Parameter: "temperature", Severity: SeverityWarning, Threshold: 100},
		{Parameter: "temperature", Severity: SeverityCritical, Threshold: 120},

		// Вибрация
		{Parameter: "vibration", Severity: SeverityInfo, Threshold: 8},
		{Parameter: "vibration", Severity: SeverityWarning, Threshold: 12},
		{Parameter: "vibration", Severity: SeverityCritical, Threshold: 15},

		// Давление
		{Parameter: "pressure", Severity: SeverityInfo, Threshold: 9},
		{Parameter: "pressure", Severity: SeverityWarning, Threshold: 10},
		{Parameter: "pressure", Severity: SeverityCritical, Threshold: 12},
	}
}

// Engine — движок проверки правил алертинга.
//
// ПОЧЕМУ отдельный struct, а не просто функция Evaluate?
// Engine хранит правила — при инициализации мы ОДИН РАЗ группируем правила
// по параметрам и сортируем по убыванию threshold. Это позволяет при проверке
// найти САМЫЙ ВЫСОКИЙ сработавший уровень за O(n) вместо O(n*m).
//
// На собеседовании: "Что если два правила сработали одновременно?"
// Ответ: возвращаем алерт с НАИВЫСШИМ severity. Если temperature = 130,
// сработают info (>90), warning (>100) и critical (>120) — возвращаем critical.
type Engine struct {
	rules []Rule
}

// NewEngine создаёт движок алертинга с указанными правилами.
func NewEngine(rules []Rule) *Engine {
	return &Engine{rules: rules}
}

// Evaluate проверяет одну запись телеметрии по всем правилам.
//
// ЛОГИКА: для каждого параметра (temperature, vibration, pressure)
// проверяем ВСЕ правила и берём наивысший severity.
// Возвращает список сработавших алертов (0..3 — по одному на параметр).
//
// ПОЧЕМУ возвращаем только наивысший severity на параметр?
// Если temperature=130, оператору не нужно 3 алерта (info+warning+critical).
// Ему нужен ОДИН алерт: "critical: температура 130°C > 120°C".
func (e *Engine) Evaluate(record entity.Record) []Alert {
	var alerts []Alert

	// Проверяем каждый параметр отдельно.
	// Для каждого находим самый "тяжёлый" сработавший порог.
	type paramCheck struct {
		name  string
		value float64
	}

	params := []paramCheck{
		{"temperature", record.Temperature},
		{"vibration", record.Vibration},
		{"pressure", record.Pressure},
	}

	for _, p := range params {
		var best *Alert

		for _, rule := range e.rules {
			if rule.Parameter != p.name {
				continue
			}
			if p.value <= rule.Threshold {
				continue
			}

			// Сработало правило. Берём наивысший severity.
			// Порядок: critical > warning > info.
			if best == nil || severityWeight(rule.Severity) > severityWeight(best.Severity) {
				alert := Alert{
					MachineID: record.MachineID,
					Severity:  rule.Severity,
					Message:   formatMessage(p.name, p.value, rule.Threshold, rule.Severity),
					Value:     p.value,
					Threshold: rule.Threshold,
					Parameter: p.name,
					CreatedAt: record.Timestamp,
				}
				best = &alert
			}
		}

		if best != nil {
			alerts = append(alerts, *best)
		}
	}

	return alerts
}

// EvaluateBatch проверяет все записи батча и возвращает все сработавшие алерты.
func (e *Engine) EvaluateBatch(records []entity.Record) []Alert {
	var all []Alert
	for _, r := range records {
		all = append(all, e.Evaluate(r)...)
	}
	return all
}

// severityWeight — числовой вес severity для сравнения.
func severityWeight(s Severity) int {
	switch s {
	case SeverityInfo:
		return 1
	case SeverityWarning:
		return 2
	case SeverityCritical:
		return 3
	default:
		return 0
	}
}

// formatMessage — формирует человекочитаемое сообщение для оператора.
//
// ПОЧЕМУ на русском? Операторы на производстве — русскоязычные.
// В интернациональной команде добавили бы i18n, но для нашего проекта — русский.
func formatMessage(param string, value, threshold float64, sev Severity) string {
	paramNames := map[string]string{
		"temperature": "Температура",
		"vibration":   "Вибрация",
		"pressure":    "Давление",
	}

	units := map[string]string{
		"temperature": "°C",
		"vibration":   "мм/с",
		"pressure":    "бар",
	}

	sevNames := map[Severity]string{
		SeverityInfo:     "ИНФО",
		SeverityWarning:  "ВНИМАНИЕ",
		SeverityCritical: "КРИТИЧНО",
	}

	name := paramNames[param]
	unit := units[param]
	sevName := sevNames[sev]

	return fmt.Sprintf("[%s] %s = %.1f%s (порог: %.1f%s)", sevName, name, value, unit, threshold, unit)
}

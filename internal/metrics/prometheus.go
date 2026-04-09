// Package metrics — Prometheus метрики для мониторинга IoT-платформы.
//
// ЗАЧЕМ Prometheus?
// "Если ты не можешь это измерить, ты не можешь это улучшить" (Питер Друкер).
// Prometheus — стандарт мониторинга в Kubernetes/DevOps:
// - Pull-модель: Prometheus сам ходит за метриками (endpoint /metrics)
// - Мощный язык запросов PromQL
// - Интеграция с Grafana для визуализации
// - Алертинг через Alertmanager
//
// ТИПЫ МЕТРИК (важно для собеседования):
// 1. Counter — монотонно растущий счётчик (только увеличивается)
//    Пример: количество принятых записей. Никогда не уменьшается.
//    НЕПРАВИЛЬНО для значений которые могут уменьшаться (активные соединения).
//
// 2. Histogram — распределение значений (percentiles: p50, p95, p99)
//    Пример: время обработки батча. Позволяет ответить:
//    "95% батчей обрабатываются за < 50мс" (SLO/SLA).
//
// 3. Gauge — текущее значение (может расти и уменьшаться)
//    Пример: размер WAL файла, количество WebSocket-клиентов.
//
// На собеседовании: "Какие метрики вы снимаете? Почему именно эти?"
// Ответ: RED-метрики (Rate, Errors, Duration) — стандарт для микросервисов.
// Rate: records_total. Errors: errors_total. Duration: batch_duration_seconds.
// Плюс бизнес-метрики: alerts_total по severity.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Метрики регистрируются через promauto — автоматическая регистрация
// в prometheus.DefaultRegisterer. Не нужно вызывать Register вручную.
//
// ПОЧЕМУ глобальные переменные, а не struct?
// В Prometheus Go SDK метрики — синглтоны (один Counter на имя).
// Передавать их через DI добавляет сложности без выгоды.
// Паттерн "глобальные метрики" — стандарт в Go-проектах (используется
// в kubernetes, etcd, prometheus server и других крупных проектах).

// === Счётчики (Counters) ===
// Монотонно растут. Prometheus вычисляет rate() — скорость изменения.

// RecordsTotal — общее количество принятых записей телеметрии.
// rate(records_total[5m]) = записей в секунду за последние 5 минут.
var RecordsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "pzmash",
	Subsystem: "telemetry",
	Name:      "records_total",
	Help:      "Общее количество принятых записей телеметрии",
})

// BatchesTotal — количество обработанных батчей.
// Один батч = один вызов HandleBatch.
var BatchesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "pzmash",
	Subsystem: "telemetry",
	Name:      "batches_total",
	Help:      "Общее количество обработанных батчей",
})

// ErrorsTotal — количество ошибок при обработке (по типу).
// Labels: type={"db_write", "inference", "alerting", "event_store"}
//
// ПОЧЕМУ labels, а не отдельные счётчики?
// Labels позволяют агрегировать: sum(errors_total) = все ошибки,
// или фильтровать: errors_total{type="db_write"} = только ошибки БД.
var ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "pzmash",
	Subsystem: "telemetry",
	Name:      "errors_total",
	Help:      "Количество ошибок обработки (по типу)",
}, []string{"type"})

// AlertsTotal — количество сработавших алертов (по severity).
// Labels: severity={"info", "warning", "critical"}
var AlertsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "pzmash",
	Subsystem: "alerting",
	Name:      "alerts_total",
	Help:      "Количество сработавших алертов (по уровню)",
}, []string{"severity"})

// === Гистограммы (Histograms) ===
// Измеряют распределение значений. Prometheus вычисляет percentiles.

// BatchDuration — время обработки батча (запись в БД + инференс + алертинг).
// Buckets подобраны для типичных значений: от 1мс до 10с.
//
// ПОЧЕМУ именно такие buckets?
// - 0.001 (1мс): нижняя граница — обработка в памяти
// - 0.005-0.01 (5-10мс): быстрая запись в БД
// - 0.05-0.1 (50-100мс): нормальная запись с инференсом
// - 0.5-1 (500мс-1с): медленная запись, возможна проблема
// - 5-10 (5-10с): таймаут, однозначно проблема
//
// SLO: batch_duration_seconds{quantile="0.95"} < 0.1 (95% за < 100мс)
var BatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "pzmash",
	Subsystem: "telemetry",
	Name:      "batch_duration_seconds",
	Help:      "Время обработки батча (секунды)",
	Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5, 10},
})

// InferenceDuration — время ML-инференса для одного батча.
// Отдельная метрика от BatchDuration — позволяет понять
// сколько от общего времени занимает ML.
var InferenceDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "pzmash",
	Subsystem: "inference",
	Name:      "duration_seconds",
	Help:      "Время ML-инференса для батча (секунды)",
	Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
})

// === Gauges ===
// Текущее значение, может расти и уменьшаться.

// WebSocketClients — количество подключённых WebSocket-клиентов.
// Gauge, потому что клиенты подключаются и отключаются.
var WebSocketClients = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "pzmash",
	Subsystem: "websocket",
	Name:      "clients_connected",
	Help:      "Количество подключённых WebSocket-клиентов",
})

// ActiveConnections — количество активных gRPC-соединений.
var ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "pzmash",
	Subsystem: "grpc",
	Name:      "active_connections",
	Help:      "Количество активных gRPC-соединений",
})

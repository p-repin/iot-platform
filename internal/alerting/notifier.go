package alerting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/pzmash/iot-platform/internal/entity"
)

// RedisChannel — канал Redis pub/sub для алертов.
// Подписчики (WebSocket handler) получают все алерты в реальном времени.
//
// ПОЧЕМУ один канал, а не по одному на severity?
// Для 20 станков поток алертов невелик. Один канал проще:
// - Подписчик фильтрует на клиенте (JavaScript)
// - Меньше подписок = меньше overhead в Redis
// - При масштабировании можно разделить: alerts:critical, alerts:warning
const RedisChannel = "alerts"

// RedisTelemetryChannel — канал Redis pub/sub для телеметрии.
// Дашборд оператора показывает ТЕКУЩИЕ показания каждого станка:
// температуру, вибрацию, давление — в реальном времени.
//
// ПОЧЕМУ отдельный канал, а не тот же "alerts"?
// 1. Телеметрия идёт каждую секунду (6 записей/с), алерты — редко
// 2. Разделение позволяет подписываться избирательно
// 3. На клиенте проще различать типы сообщений по каналу
const RedisTelemetryChannel = "telemetry"

// Notifier — отправляет алерты в Redis pub/sub и сохраняет critical в БД.
//
// ПОЧЕМУ два канала доставки (Redis + БД)?
// 1. Redis pub/sub — fire-and-forget: мгновенная доставка на дашборд через WebSocket.
//    Если дашборд закрыт — сообщение потеряно. Это ОК для info/warning.
// 2. БД (таблица alerts) — персистентное хранение. Critical алерты ДОЛЖНЫ
//    быть видны оператору даже если он открыл дашборд через час.
//    На дашборде: "5 нерешённых critical алертов" (из БД).
//
// На собеседовании: "Почему не только Redis / не только БД?"
// - Только Redis: потеряем critical алерты при отсутствии подписчиков
// - Только БД: polling вместо push = задержка + нагрузка на БД
// - Комбинация: push для real-time + persist для надёжности
type Notifier struct {
	rdb    *redis.Client
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewNotifier создаёт notifier с Redis и PostgreSQL.
func NewNotifier(rdb *redis.Client, pool *pgxpool.Pool, logger *slog.Logger) *Notifier {
	return &Notifier{
		rdb:    rdb,
		pool:   pool,
		logger: logger,
	}
}

// Notify обрабатывает список алертов:
// 1. Публикует ВСЕ алерты в Redis pub/sub (для WebSocket дашборда)
// 2. Сохраняет CRITICAL алерты в БД (таблица alerts)
//
// ПОЧЕМУ не возвращаем ошибку?
// Алертинг не должен блокировать основной pipeline (запись телеметрии).
// Если Redis или БД недоступны — логируем ошибку и продолжаем.
// Телеметрия важнее алертов: лучше потерять алерт, чем потерять данные.
func (n *Notifier) Notify(ctx context.Context, alerts []Alert) {
	for _, alert := range alerts {
		// 1. Публикуем в Redis pub/sub (все уровни)
		n.publishToRedis(ctx, alert)

		// 2. Сохраняем critical в БД
		if alert.Severity == SeverityCritical {
			n.saveToDB(ctx, alert)
		}
	}
}

// publishToRedis публикует алерт в Redis pub/sub канал.
//
// ФОРМАТ: JSON — универсальный, парсится JavaScript на дашборде.
// Redis PUBLISH — O(N+M) где N = подписчики, M = размер сообщения.
// Для наших объёмов (~20 станков) это мгновенно.
func (n *Notifier) publishToRedis(ctx context.Context, alert Alert) {
	data, err := json.Marshal(alert)
	if err != nil {
		n.logger.Error("ошибка сериализации алерта для Redis",
			"machine_id", alert.MachineID,
			"error", err,
		)
		return
	}

	if err := n.rdb.Publish(ctx, RedisChannel, data).Err(); err != nil {
		n.logger.Error("ошибка публикации алерта в Redis",
			"machine_id", alert.MachineID,
			"severity", alert.Severity,
			"error", err,
		)
		return
	}

	n.logger.Debug("алерт опубликован в Redis",
		"machine_id", alert.MachineID,
		"severity", alert.Severity,
		"channel", RedisChannel,
	)
}

// saveToDB сохраняет critical алерт в таблицу alerts.
//
// Таблица alerts создана в init-db/001_create_tables.sql:
// alert_id BIGSERIAL, machine_id TEXT, severity TEXT, message TEXT,
// resolved BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ.
//
// resolved=false — алерт не решён. Оператор закрывает алерт на дашборде.
func (n *Notifier) saveToDB(ctx context.Context, alert Alert) {
	query := `
		INSERT INTO alerts (machine_id, severity, message, created_at)
		VALUES ($1, $2, $3, $4)
	`

	_, err := n.pool.Exec(ctx, query,
		alert.MachineID,
		string(alert.Severity),
		alert.Message,
		alert.CreatedAt,
	)

	if err != nil {
		n.logger.Error("ошибка сохранения алерта в БД",
			"machine_id", alert.MachineID,
			"severity", alert.Severity,
			"error", err,
		)
		return
	}

	n.logger.Info("critical алерт сохранён в БД",
		"machine_id", alert.MachineID,
		"message", alert.Message,
	)
}

// TelemetryMessage — сообщение с показаниями станка для дашборда.
//
// Содержит текущие значения датчиков одного станка.
// Дашборд отображает таблицу: машина → температура/вибрация/давление.
// Обновляется каждую секунду через WebSocket.
type TelemetryMessage struct {
	Type        string  `json:"type"`        // "telemetry" — тип сообщения (для различения от алертов)
	MachineID   string  `json:"machine_id"`
	Temperature float64 `json:"temperature"`
	Vibration   float64 `json:"vibration"`
	Pressure    float64 `json:"pressure"`
	Timestamp   time.Time `json:"timestamp"`
}

// PublishTelemetry публикует показания станков в Redis для дашборда оператора.
//
// ПОЧЕМУ публикуем ВСЕ записи, а не только аномальные?
// Оператор должен видеть ТЕКУЩЕЕ состояние каждого станка:
// - Нормальные показания — зелёный индикатор, всё ОК
// - Приближение к порогу — жёлтый, стоит наблюдать
// - Превышение — красный, нужно действовать
// Без нормальных данных оператор не знает, работает ли станок вообще.
func (n *Notifier) PublishTelemetry(ctx context.Context, records []entity.Record) {
	for _, r := range records {
		msg := TelemetryMessage{
			Type:        "telemetry",
			MachineID:   r.MachineID,
			Temperature: r.Temperature,
			Vibration:   r.Vibration,
			Pressure:    r.Pressure,
			Timestamp:   r.Timestamp,
		}

		data, err := json.Marshal(msg)
		if err != nil {
			continue
		}

		if err := n.rdb.Publish(ctx, RedisTelemetryChannel, data).Err(); err != nil {
			// Не логируем каждую ошибку — телеметрия идёт каждую секунду,
			// засорим логи. Логируем только при первой ошибке (TODO: rate limiter).
			continue
		}
	}
}

// GetUnresolved возвращает нерешённые алерты из БД (для дашборда при открытии).
//
// ПОЧЕМУ нужен этот метод?
// Redis pub/sub — fire-and-forget. Если оператор открыл дашборд через час,
// он не увидит пропущенные алерты. GetUnresolved загружает их из БД.
// Дашборд вызывает этот endpoint при старте, затем переключается на WebSocket.
func (n *Notifier) GetUnresolved(ctx context.Context) ([]Alert, error) {
	query := `
		SELECT machine_id, severity, message, created_at
		FROM alerts
		WHERE resolved = FALSE
		ORDER BY created_at DESC
		LIMIT 100
	`

	rows, err := n.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки нерешённых алертов: %w", err)
	}
	defer rows.Close()

	var alerts []Alert
	for rows.Next() {
		var a Alert
		if err := rows.Scan(&a.MachineID, &a.Severity, &a.Message, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("ошибка чтения алерта: %w", err)
		}
		alerts = append(alerts, a)
	}

	return alerts, rows.Err()
}

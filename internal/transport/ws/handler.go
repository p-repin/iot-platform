// Package ws — WebSocket транспорт для real-time обновлений дашборда.
//
// АРХИТЕКТУРА:
// Redis pub/sub → WebSocket handler → браузер оператора
//
// ПОЧЕМУ WebSocket, а не SSE (Server-Sent Events)?
// 1. WebSocket — полный дуплекс: в будущем оператор сможет ОТПРАВЛЯТЬ
//    команды (отметить алерт как решённый, остановить станок)
// 2. SSE — только сервер → клиент, для управления нужен отдельный HTTP endpoint
// 3. WebSocket — стандарт для real-time дашбордов (Grafana, Kibana используют его)
//
// ПОЧЕМУ gorilla/websocket?
// Самая популярная Go-библиотека для WebSocket (~22k stars).
// Стабильная, хорошо документированная, используется в production.
// Альтернатива: nhooyr.io/websocket — более идиоматичная, но менее распространённая.
//
// На собеседовании: "Как вы реализовали real-time обновления на дашборде?"
// Ответ: Redis pub/sub → Go WebSocket handler → JavaScript EventListener.
// Redis агрегирует алерты от всех инстансов сервера,
// WebSocket пробрасывает их в браузер с минимальной задержкой.
package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/pzmash/iot-platform/internal/alerting"
	"github.com/pzmash/iot-platform/internal/metrics"
)

// upgrader конвертирует HTTP-соединение в WebSocket.
//
// CheckOrigin возвращает true — разрешаем подключения с любого origin.
// ПОЧЕМУ? В dev-окружении дашборд и сервер на разных портах (CORS).
// В проде нужно ограничить: CheckOrigin проверяет Origin header.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Handler — WebSocket handler, управляет подключёнными клиентами.
//
// ПАТТЕРН: Hub (центральный хаб).
// Один hub управляет всеми WebSocket-соединениями.
// Новое сообщение из Redis → hub рассылает всем подключённым клиентам.
// Клиент отключился → hub удаляет его из списка.
//
// ПОЧЕМУ не отдельная горутина на каждого клиента для чтения Redis?
// Если 10 операторов открыли дашборд, каждый создаст подписку на Redis.
// Hub подписывается ОДИН РАЗ и рассылает всем — эффективнее.
type Handler struct {
	rdb      *redis.Client
	notifier *alerting.Notifier
	logger   *slog.Logger

	// mu защищает clients от конкурентного доступа.
	// Горутина Redis subscriber добавляет сообщения,
	// HTTP handler добавляет/удаляет клиентов.
	mu      sync.RWMutex
	clients map[*websocket.Conn]bool
}

// NewHandler создаёт WebSocket handler.
func NewHandler(rdb *redis.Client, notifier *alerting.Notifier, logger *slog.Logger) *Handler {
	return &Handler{
		rdb:      rdb,
		notifier: notifier,
		logger:   logger,
		clients:  make(map[*websocket.Conn]bool),
	}
}

// Run запускает фоновую подписку на Redis pub/sub.
// Блокирует до отмены контекста. Вызывать в отдельной горутине.
//
// ЖИЗНЕННЫЙ ЦИКЛ:
// 1. Подписываемся на канал "alerts" в Redis
// 2. В бесконечном цикле читаем сообщения
// 3. Каждое сообщение рассылаем всем подключённым WebSocket-клиентам
// 4. При отмене контекста — отписываемся и выходим
func (h *Handler) Run(ctx context.Context) {
	// Подписываемся на ОБА канала: алерты + телеметрия.
	// Оператор видит и текущие показания станков, и сработавшие алерты.
	sub := h.rdb.Subscribe(ctx, alerting.RedisChannel, alerting.RedisTelemetryChannel)
	defer sub.Close()

	ch := sub.Channel()
	h.logger.Info("WebSocket hub: подписка на Redis",
		"channels", []string{alerting.RedisChannel, alerting.RedisTelemetryChannel},
	)

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("WebSocket hub: остановка")
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			h.broadcast([]byte(msg.Payload))
		}
	}
}

// broadcast рассылает сообщение всем подключённым WebSocket-клиентам.
//
// ПОЧЕМУ RLock для чтения, Lock для удаления?
// Чтение (broadcast) происходит часто, запись (disconnect) — редко.
// RWMutex позволяет нескольким горутинам читать одновременно.
//
// ПОЧЕМУ удаляем клиента при ошибке записи?
// Если WriteMessage вернул ошибку — клиент отключился (закрыл вкладку,
// потерял сеть). Держать мёртвое соединение бессмысленно.
func (h *Handler) broadcast(data []byte) {
	h.mu.RLock()
	clients := make([]*websocket.Conn, 0, len(h.clients))
	for conn := range h.clients {
		clients = append(clients, conn)
	}
	h.mu.RUnlock()

	var dead []*websocket.Conn

	for _, conn := range clients {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			h.logger.Debug("WebSocket: клиент отключился", "error", err)
			dead = append(dead, conn)
		}
	}

	// Удаляем отключившихся клиентов
	if len(dead) > 0 {
		h.mu.Lock()
		for _, conn := range dead {
			delete(h.clients, conn)
			conn.Close()
		}
		h.mu.Unlock()
	}
}

// ServeHTTP обрабатывает HTTP → WebSocket upgrade.
//
// ПОСЛЕДОВАТЕЛЬНОСТЬ:
// 1. Upgrade HTTP → WebSocket
// 2. Отправляем клиенту нерешённые алерты из БД (если есть)
// 3. Регистрируем клиента в hub
// 4. Читаем сообщения от клиента (пока он не отключится)
//
// Шаг 2 важен: если оператор открыл дашборд через час после critical алерта,
// он ДОЛЖЕН увидеть этот алерт. Redis pub/sub уже потерял это сообщение,
// но БД его хранит.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("WebSocket upgrade error", "error", err)
		return
	}

	h.logger.Info("WebSocket: новый клиент",
		"remote", r.RemoteAddr,
	)

	// Отправляем нерешённые алерты из БД при подключении
	if unresolved, err := h.notifier.GetUnresolved(r.Context()); err == nil && len(unresolved) > 0 {
		for _, alert := range unresolved {
			data, _ := json.Marshal(alert)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				h.logger.Error("WebSocket: ошибка отправки исторических алертов", "error", err)
				conn.Close()
				return
			}
		}
		h.logger.Debug("WebSocket: отправлены нерешённые алерты",
			"count", len(unresolved),
		)
	}

	// Регистрируем клиента
	h.mu.Lock()
	h.clients[conn] = true
	h.mu.Unlock()
	metrics.WebSocketClients.Inc()

	// Читаем сообщения от клиента (держим соединение открытым).
	// Когда клиент отключится — ReadMessage вернёт ошибку, и мы выходим.
	// В будущем здесь можно обрабатывать команды от оператора
	// (например, "resolve alert #42").
	defer func() {
		h.mu.Lock()
		delete(h.clients, conn)
		h.mu.Unlock()
		conn.Close()
		metrics.WebSocketClients.Dec()
		h.logger.Info("WebSocket: клиент отключился",
			"remote", r.RemoteAddr,
		)
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

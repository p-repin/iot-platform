// cmd/server/main.go — точка входа серверного приложения.
//
// COMPOSITION ROOT — единственное место, где собираются все зависимости.
// Здесь мы "склеиваем" слои чистой архитектуры:
//   pgxpool → PostgresRepo → DBService → gRPC Server
//   Redis → Notifier → DBService (алертинг)
//   Redis → WebSocket Handler (real-time дашборд)
//   HTTP: /metrics (Prometheus), /ws (WebSocket), / (дашборд)
//
// ПОЧЕМУ зависимости собираются в main, а не в каждом пакете?
// 1. Каждый пакет зависит от ИНТЕРФЕЙСОВ, а не от конкретных реализаций
// 2. main.go решает КАКУЮ реализацию подставить (PostgreSQL vs мок)
// 3. Все зависимости видны в одном месте — легко понять архитектуру
// 4. На собеседовании: "Как вы реализуете Dependency Injection в Go?"
//    Ответ: через конструкторы в Composition Root (main.go), без фреймворков.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pzmash/iot-platform/internal/alerting"
	"github.com/pzmash/iot-platform/internal/config"
	"github.com/pzmash/iot-platform/internal/events"
	"github.com/pzmash/iot-platform/internal/inference"
	"github.com/pzmash/iot-platform/internal/repository"
	"github.com/pzmash/iot-platform/internal/service"
	grpcTransport "github.com/pzmash/iot-platform/internal/transport/grpc"
	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
	"github.com/pzmash/iot-platform/internal/transport/ws"
)

func main() {
	// === 1. Structured logging (slog) ===
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Загружаем конфигурацию из переменных окружения
	cfg := config.Load()

	// === 2. Graceful shutdown ===
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// === 3. Подключение к TimescaleDB ===
	slog.Info("Подключение к TimescaleDB...", "host", cfg.DB.Host, "db", cfg.DB.DBName)

	repo, err := repository.NewPostgresRepo(ctx, cfg.DB.DSN())
	if err != nil {
		slog.Error("Не удалось подключиться к TimescaleDB", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	// === 4. Redis (Фаза 6: алертинг + WebSocket) ===
	// Redis используется для двух вещей:
	// 1. Pub/Sub — мгновенная доставка алертов на дашборд
	// 2. (В будущем) Кэширование — последние значения телеметрии для дашборда
	//
	// ПОЧЕМУ опционален?
	// Сервер должен работать даже без Redis (graceful degradation).
	// Просто алерты не будут приходить на дашборд в реальном времени.
	slog.Info("Подключение к Redis...", "addr", cfg.Redis.Addr)

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	// Проверяем подключение к Redis
	var alertEngine *alerting.Engine
	var alertNotifier *alerting.Notifier
	var wsHandler *ws.Handler

	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Warn("Redis недоступен, алертинг и WebSocket отключены",
			"addr", cfg.Redis.Addr,
			"error", err,
		)
	} else {
		slog.Info("Redis подключён", "addr", cfg.Redis.Addr)

		// Создаём движок алертинга с правилами по умолчанию
		alertEngine = alerting.NewEngine(alerting.DefaultRules())

		// Notifier: Redis pub/sub + сохранение critical в БД
		alertNotifier = alerting.NewNotifier(rdb, repo.Pool(), logger)

		// WebSocket handler: подписка на Redis → broadcast клиентам
		wsHandler = ws.NewHandler(rdb, alertNotifier, logger)
		go wsHandler.Run(ctx)

		slog.Info("Алертинг и WebSocket активированы")
	}
	defer rdb.Close()

	// === 5. ML-инференс ===
	slog.Info("Инициализация ML-инференса...",
		"model", cfg.Inference.ModelPath,
		"window_size", cfg.Inference.WindowSize,
		"threshold", cfg.Inference.Threshold,
	)

	var inferSvc *inference.Service
	predictor, err := inference.NewPredictor(
		cfg.Inference.SharedLibPath,
		cfg.Inference.ModelPath,
		cfg.Inference.WindowSize*3,
	)
	if err != nil {
		slog.Warn("ML-инференс недоступен, сервер работает без ML",
			"error", err,
		)
	} else {
		inferSvc = inference.NewService(predictor, cfg.Inference.WindowSize, cfg.Inference.Threshold, logger)
		defer inferSvc.Close()
		slog.Info("ML-инференс активирован",
			"window_size", cfg.Inference.WindowSize,
			"threshold", cfg.Inference.Threshold,
		)
	}

	// === 6. Event Store (Event Sourcing) ===
	eventStore := events.NewPostgresEventStore(repo.Pool(), logger)

	// Симулятор жизненного цикла деталей
	if cfg.PartSimulator.Enabled {
		simulator := events.NewPartSimulator(eventStore, cfg.PartSimulator.Interval, logger)
		go simulator.Run(ctx)
		slog.Info("Симулятор деталей включён",
			"interval", cfg.PartSimulator.Interval,
		)
	}

	// === 7. Собираем зависимости (Composition Root) ===
	// Цепочка: PostgresRepo → DBService (+ inference + eventStore + alerting) → gRPC Server
	svc := service.NewDBService(repo, inferSvc, eventStore, alertEngine, alertNotifier)
	telemetryServer := grpcTransport.NewServer(svc)

	// === 8. gRPC сервер ===
	grpcServer := grpc.NewServer()
	pb.RegisterTelemetryServiceServer(grpcServer, telemetryServer)
	reflection.Register(grpcServer)

	addr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("Не удалось открыть gRPC порт", "addr", addr, "error", err)
		os.Exit(1)
	}

	slog.Info("gRPC сервер запущен", "addr", addr)

	// === 9. HTTP сервер (Фаза 6: метрики + WebSocket + дашборд) ===
	// ПОЧЕМУ отдельный HTTP-сервер, а не добавлять к gRPC?
	// 1. gRPC использует HTTP/2 с бинарным протоколом — нельзя смешивать
	//    с обычными HTTP-эндпоинтами на одном порту (без grpc-gateway)
	// 2. Prometheus ожидает стандартный HTTP GET /metrics
	// 3. WebSocket требует HTTP/1.1 upgrade — несовместим с gRPC HTTP/2
	// 4. Разные порты = можно открыть metrics только для внутренней сети
	mux := http.NewServeMux()

	// /metrics — Prometheus scraper будет ходить сюда каждые 15с.
	// promhttp.Handler() экспортирует ВСЕ зарегистрированные метрики
	// (наши + стандартные Go runtime: goroutines, memory, GC).
	mux.Handle("/metrics", promhttp.Handler())

	// /ws — WebSocket endpoint для дашборда
	if wsHandler != nil {
		mux.Handle("/ws", wsHandler)
	}

	// / — дашборд оператора (статический HTML файл).
	// Раздаём из директории cmd/dashboard/ относительно рабочей директории.
	// ПОЧЕМУ http.FileServer, а не embed?
	// embed.FS не поддерживает пути с ".." (нельзя ссылаться из cmd/server
	// на cmd/dashboard). Для dev-окружения FileServer достаточен.
	// В Фазе 8 (Docker) скопируем index.html в образ рядом с бинарником.
	mux.Handle("/", http.FileServer(http.Dir("cmd/dashboard")))

	httpAddr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	slog.Info("HTTP сервер запущен",
		"addr", httpAddr,
		"endpoints", []string{"/metrics", "/ws", "/"},
	)

	// === 10. Запуск с graceful shutdown ===
	errCh := make(chan error, 2)

	// gRPC сервер
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- fmt.Errorf("gRPC: %w", err)
		}
	}()

	// HTTP сервер
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("HTTP: %w", err)
		}
	}()

	// Ждём ЛИБО сигнал завершения, ЛИБО ошибку сервера
	select {
	case <-ctx.Done():
		slog.Info("Получен сигнал завершения, останавливаю серверы...")
		grpcServer.GracefulStop()
		httpServer.Shutdown(context.Background())
		slog.Info("Серверы остановлены корректно")
	case err := <-errCh:
		slog.Error("Сервер упал", "error", err)
		os.Exit(1)
	}
}

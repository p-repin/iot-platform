// cmd/server/main.go — точка входа серверного приложения.
//
// COMPOSITION ROOT — единственное место, где собираются все зависимости.
// Здесь мы "склеиваем" слои чистой архитектуры:
//   pgxpool → PostgresRepo → DBService → gRPC Server
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
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pzmash/iot-platform/internal/config"
	"github.com/pzmash/iot-platform/internal/repository"
	"github.com/pzmash/iot-platform/internal/service"
	grpcTransport "github.com/pzmash/iot-platform/internal/transport/grpc"
	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
)

func main() {
	// === 1. Structured logging (slog) ===
	// ПОЧЕМУ slog, а не zerolog/zap?
	// slog — стандартная библиотека Go (с 1.21). Не нужна внешняя зависимость.
	// zerolog/zap быстрее на ~20-30%, но для нашей нагрузки (20 станков) разницы нет.
	// На собеседовании: "Мы используем slog из stdlib, при необходимости можно
	// заменить handler на zerolog/zap без изменения вызывающего кода."
	//
	// ПОЧЕМУ JSON, а не Text?
	// DevOps-требование: JSON-логи парсятся автоматически.
	// Promtail/Loki/Elasticsearch читают JSON без кастомных regex-парсеров.
	// TextHandler удобнее для локальной разработки, но в проде — только JSON.
	//
	// УРОВЕНЬ LevelDebug:
	// В проде обычно LevelInfo (меньше шума). В dev — LevelDebug для отладки.
	// Можно вынести в конфиг: LOG_LEVEL=debug|info|warn|error.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Загружаем конфигурацию из переменных окружения
	cfg := config.Load()

	// === 2. Graceful shutdown ===
	// signal.NotifyContext — создаёт контекст, отменяемый по SIGINT/SIGTERM.
	// При docker stop контейнер получает SIGTERM → контекст отменяется →
	// закрываем соединения и завершаемся корректно.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// === 3. Подключение к TimescaleDB ===
	// ПОРЯДОК ИНИЦИАЛИЗАЦИИ: БД первая, потому что без неё сервер бесполезен.
	// Если БД недоступна — падаем сразу (fail fast), а не через минуту
	// когда придёт первый запрос от агента.
	slog.Info("Подключение к TimescaleDB...", "host", cfg.DB.Host, "db", cfg.DB.DBName)

	repo, err := repository.NewPostgresRepo(ctx, cfg.DB.DSN())
	if err != nil {
		slog.Error("Не удалось подключиться к TimescaleDB", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	// === 4. Собираем зависимости (Composition Root) ===
	// Цепочка: PostgresRepo → DBService → gRPC Server
	// Каждый слой получает зависимости через конструктор (DI без фреймворков).
	svc := service.NewDBService(repo)
	telemetryServer := grpcTransport.NewServer(svc)

	// === 5. gRPC сервер ===
	grpcServer := grpc.NewServer()
	pb.RegisterTelemetryServiceServer(grpcServer, telemetryServer)

	// gRPC Reflection — инструменты (grpcurl, Postman) могут узнать
	// список сервисов и методов БЕЗ .proto файла.
	reflection.Register(grpcServer)

	addr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("Не удалось открыть порт", "addr", addr, "error", err)
		os.Exit(1)
	}

	slog.Info("gRPC сервер запущен", "addr", addr)

	// === 6. Запуск с graceful shutdown ===
	// Запускаем gRPC сервер в отдельной горутине, чтобы main мог
	// ждать сигнал завершения в select.
	//
	// ПОЧЕМУ горутина, а не просто Serve?
	// grpcServer.Serve() блокирует. Если вызвать его в main —
	// мы не сможем обработать SIGTERM (контекст отменится, но никто не вызовет
	// GracefulStop). Горутина позволяет параллельно слушать ctx.Done().
	errCh := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- err
		}
	}()

	// Ждём ЛИБО сигнал завершения, ЛИБО ошибку сервера.
	select {
	case <-ctx.Done():
		// Получили SIGINT/SIGTERM → корректно завершаемся.
		// GracefulStop:
		// 1. Перестаёт принимать НОВЫЕ соединения
		// 2. Ждёт завершения ТЕКУЩИХ запросов
		// 3. Закрывает listener
		// Это гарантирует что ни один запрос не оборвётся на середине.
		slog.Info("Получен сигнал завершения, останавливаю gRPC сервер...")
		grpcServer.GracefulStop()
		slog.Info("Сервер остановлен корректно")
	case err := <-errCh:
		slog.Error("gRPC сервер упал", "error", err)
		os.Exit(1)
	}
}

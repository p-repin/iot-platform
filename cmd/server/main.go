// cmd/server/main.go — точка входа серверного приложения.
//
// СТРУКТУРА ПРОЕКТА (стандарт Go):
// cmd/    — точки входа (main.go для каждого бинарника)
// internal/ — внутренний код (НЕ импортируется другими модулями)
// proto/  — .proto схемы
//
// ПОЧЕМУ cmd/server/ а не просто main.go в корне?
// У нас НЕСКОЛЬКО бинарников: server, agent, dashboard.
// Каждый живёт в своей директории под cmd/.
// go build ./cmd/server → бинарник server
// go build ./cmd/agent  → бинарник agent
//
// ПОЧЕМУ internal/?
// Go запрещает импорт пакетов из internal/ извне модуля.
// Это ГАРАНТИЯ на уровне компилятора что внутренний код не утечёт наружу.
// Без internal/ любой может сделать import "github.com/pzmash/.../telemetry"
// и завязаться на наши внутренние структуры.
package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pzmash/iot-platform/internal/config"
	"github.com/pzmash/iot-platform/internal/service"
	grpcTransport "github.com/pzmash/iot-platform/internal/transport/grpc"
	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
)

func main() {
	// Загружаем конфигурацию из переменных окружения
	cfg := config.Load()

	// Создаём gRPC сервер.
	// ПОЧЕМУ без TLS?
	// В dev-окружении TLS избыточен (localhost → localhost).
	// В проде (air-gap контур) сеть физически изолирована,
	// но можно добавить mTLS для defense-in-depth.
	grpcServer := grpc.NewServer()

	// Создаём сервис телеметрии (пока LogService, в Фазе 3 заменим на DBService).
	// CLEAN ARCHITECTURE: main.go — единственное место, где собираются зависимости.
	// Это называется Composition Root: здесь мы "склеиваем" слои вместе.
	svc := &service.LogService{}

	// Регистрируем наш сервис в gRPC сервере.
	// pb.RegisterTelemetryServiceServer — сгенерированная функция из .proto.
	// Она связывает наш Server с gRPC фреймворком.
	telemetryServer := grpcTransport.NewServer(svc)
	pb.RegisterTelemetryServiceServer(grpcServer, telemetryServer)

	// gRPC Reflection — позволяет инструментам (grpcurl, Postman)
	// узнать какие сервисы и методы доступны БЕЗ .proto файла.
	// ПОЧЕМУ включаем?
	// - В dev: удобно тестировать через grpcurl (аналог curl для gRPC)
	// - В проде: можно отключить из соображений безопасности
	//   (скрыть структуру API от потенциального злоумышленника)
	reflection.Register(grpcServer)

	// Открываем TCP listener
	addr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Не удалось открыть порт %s: %v", addr, err)
	}

	log.Printf("gRPC сервер запущен на %s", addr)

	// Serve блокирует выполнение — сервер обрабатывает запросы до остановки.
	// ПОЧЕМУ log.Fatalf, а не return err?
	// В main() некому возвращать ошибку — это верхний уровень.
	// Fatalf логирует и вызывает os.Exit(1) — процесс завершается с ненулевым кодом,
	// что сигнализирует системе мониторинга о проблеме.
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC сервер упал: %v", err)
	}
}
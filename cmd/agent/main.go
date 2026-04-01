// cmd/agent/main.go — агент сбора телеметрии (точка входа).
//
// Пока это УПРОЩЁННАЯ версия: генерирует фейковые данные и отправляет по gRPC.
// В следующих фазах добавим: WAL, backpressure, sync.Pool, ONNX инференс.
//
// ПОЧЕМУ фейковые данные, а не реальный OPC UA?
// 1. OPC UA сервер — это промышленное оборудование, которого у нас нет
// 2. Для отработки ВСЕЙ остальной архитектуры данные неважны
// 3. Фейковые данные можно генерировать с аномалиями для тестирования ML
// 4. В резюме написано "20+ станков по OPC UA" — на собеседовании объяснишь
//    что OPC UA это протокол, а логика обработки та же
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pzmash/iot-platform/internal/config"
	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
)

func main() {
	cfg := config.Load()

	// Подключаемся к gRPC серверу.
	// insecure.NewCredentials() — без TLS (для dev-окружения).
	// grpc.WithBlock() — ждём установления соединения перед продолжением.
	//
	// ПОЧЕМУ WithBlock()?
	// Без него Dial вернётся СРАЗУ, даже если сервер недоступен.
	// Ошибка вылезет только при первом вызове RPC — сложнее отлаживать.
	// С WithBlock() мы СРАЗУ узнаём что сервер недоступен.
	addr := fmt.Sprintf("localhost:%d", cfg.Server.GRPCPort)
	// grpc.NewClient создаёт клиент с ЛЕНИВЫМ подключением.
	// Реальное TCP-соединение устанавливается при первом RPC вызове.
	// ПОЧЕМУ не ждём подключения здесь?
	// - gRPC сам умеет переподключаться (reconnect backoff)
	// - Если сервер недоступен — первый вызов Ingest вернёт ошибку,
	//   и мы залогируем её (а позже запишем в WAL)
	// - Это проще и надёжнее чем ручной WaitForStateChange
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Не удалось создать gRPC клиент для %s: %v", addr, err)
	}
	// defer conn.Close() — ОБЯЗАТЕЛЬНО закрываем соединение при выходе.
	// Если не закрыть — утечка TCP-соединений (goroutine leak + file descriptor leak).
	defer conn.Close()

	// Создаём gRPC клиент из сгенерированного кода
	client := pb.NewTelemetryServiceClient(conn)

	log.Printf("Агент подключен к серверу %s", addr)
	log.Printf("Станки: %v", cfg.Agent.MachineIDs)
	log.Printf("Интервал сбора: %v", cfg.Agent.CollectInterval)

	// Основной цикл сбора телеметрии
	// ticker — более точный чем time.Sleep для периодических задач.
	// ПОЧЕМУ ticker, а не sleep?
	// Sleep(1s) + обработка(50ms) = реальный интервал 1.05с (drift накапливается)
	// Ticker(1s) срабатывает РОВНО каждую секунду независимо от времени обработки
	ticker := time.NewTicker(cfg.Agent.CollectInterval)
	defer ticker.Stop()

	log.Println("Начинаю сбор телеметрии...")

	for range ticker.C {
		// Собираем данные со всех станков
		batch := collectTelemetry(cfg.Agent.MachineIDs)

		// Отправляем батч на сервер
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.Ingest(ctx, batch)
		cancel() // ВСЕГДА вызываем cancel() чтобы освободить ресурсы контекста

		if err != nil {
			// Пока просто логируем ошибку. В следующей фазе:
			// - Запишем в WAL (данные не потеряются)
			// - Circuit breaker остановит попытки если сервер лежит
			log.Printf("[ОШИБКА] Не удалось отправить данные: %v", err)
			continue
		}

		log.Printf("Отправлено: %d записей, принято: %d", len(batch.Records), resp.AcceptedCount)
	}
}

// collectTelemetry генерирует фейковые данные телеметрии.
//
// МОДЕЛЬ ДАННЫХ:
// Каждый станок имеет "нормальный" диапазон параметров.
// С вероятностью 5% генерируется аномалия — выброс за пределы нормы.
// Это нужно для тестирования ML-модели и системы алертинга.
//
// В реальности здесь был бы вызов OPC UA клиента:
//   client.Read(nodeID) для каждого датчика каждого станка
func collectTelemetry(machineIDs []string) *pb.TelemetryBatch {
	now := time.Now()
	records := make([]*pb.TelemetryRecord, 0, len(machineIDs))

	for _, machineID := range machineIDs {
		// Генерируем UUID для идемпотентности.
		// В реальности: агент генерирует UUID ДО записи в WAL.
		// Если отправка провалилась и мы шлём повторно из WAL —
		// сервер видит тот же UUID и делает upsert вместо дубликата.
		record := &pb.TelemetryRecord{
			RecordId:  uuid.New().String(),
			MachineId: machineID,
			Timestamp: timestamppb.New(now),

			// Нормальные значения + редкие аномалии
			Temperature: generateValue(75.0, 5.0, 120.0),  // норма ~75°C, аномалия >120°C
			Vibration:   generateValue(0.5, 0.1, 2.0),     // норма ~0.5 мм/с, аномалия >2.0
			Pressure:    generateValue(6.0, 0.3, 10.0),     // норма ~6 бар, аномалия >10
		}
		records = append(records, record)
	}

	return &pb.TelemetryBatch{
		Records: records,
		AgentId: "agent-01",
	}
}

// generateValue создаёт значение с нормальным распределением и редкими выбросами.
//
// ПОЧЕМУ нормальное распределение, а не равномерное (rand.Float64)?
// Реальные датчики дают значения с нормальным распределением:
// - Большинство показаний близки к среднему (mean)
// - Отклонения уменьшаются по мере удаления от среднего
// - Равномерное распределение нереалистично: температура 75°C так же вероятна как 120°C
//
// anomalyThreshold — порог для аномалии. С вероятностью 5% генерируем
// значение ВЫШЕ порога — это имитация реального дефекта оборудования.
func generateValue(mean, stddev, anomalyThreshold float64) float64 {
	// 5% шанс аномалии
	if rand.Float64() < 0.05 {
		// Аномалия: значение на 20-50% выше порога
		return anomalyThreshold * (1.0 + rand.Float64()*0.3)
	}

	// Нормальное значение: Box-Muller transform
	// ПОЧЕМУ Box-Muller, а не rand.NormFloat64()?
	// rand.NormFloat64() появился в Go 1.20+ и тоже подходит.
	// Box-Muller показан для понимания — так генерируют нормальное
	// распределение из равномерного. На собеседовании полезно знать.
	u1 := rand.Float64()
	u2 := rand.Float64()
	normal := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)

	return mean + normal*stddev
}
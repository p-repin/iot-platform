// Package agent — логика агента сбора телеметрии.
//
// ПОЧЕМУ отдельный пакет, а не internal/telemetry?
// Раньше collector.go и handler.go лежали в одном пакете telemetry.
// Но это смешивало серверную логику (handler) с агентской (collector).
// При чистой архитектуре серверная логика переехала в service/,
// а агентская — сюда, в agent/.
//
// Агент — это data pipeline: собрать → сериализовать → WAL → отправить.
// Он работает с protobuf-типами напрямую, и это КОРРЕКТНО:
// агент IS транспорт (его задача — доставить данные на сервер).
//
// ПОТОКОВАЯ МОДЕЛЬ:
// Collector запускает ДВЕ горутины:
// - collectLoop: собирает данные → пишет в WAL (периодически по тикеру)
// - sendLoop: читает из WAL → отправляет на сервер (с backoff при ошибках)
//
// ПОЧЕМУ две горутины, а не одна?
// Если сервер недоступен и мы ждём backoff — сбор данных НЕ ДОЛЖЕН останавливаться.
// Разделение на "писатель" и "отправитель" позволяет копить данные в WAL
// пока идёт ожидание восстановления сервера.
package agent

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/pzmash/iot-platform/internal/transport/grpc/pb"
	"github.com/pzmash/iot-platform/internal/wal"
)

// Константы backpressure.
// - initialBackoff 1с: первая ошибка может быть случайной, не ждём долго
// - maxBackoff 30с: при длительном offline не шлём чаще чем раз в 30с
// - backoffMultiplier 2.0: удвоение — стандарт (используется в gRPC, TCP, AWS SDK)
const (
	initialBackoff    = 1 * time.Second
	maxBackoff        = 30 * time.Second
	backoffMultiplier = 2.0
)

// bufPool — пул буферов для protobuf-сериализации.
//
// КАК РАБОТАЕТ sync.Pool:
// 1. Get() — берём буфер из пула (или создаём новый если пул пуст)
// 2. Используем буфер для сериализации
// 3. Put() — возвращаем буфер в пул для переиспользования
//
// ПОЧЕМУ sync.Pool, а не просто var buf []byte?
// - sync.Pool потокобезопасен (две горутины могут одновременно брать буферы)
// - GC может освободить неиспользуемые буферы из пула (нет утечки памяти)
// - Стандартный паттерн Go для high-throughput систем
var bufPool = sync.Pool{
	New: func() any {
		// Начальный размер 256 байт — типичный размер сериализованного
		// TelemetryBatch с 6 станками.
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// Sender — интерфейс для отправки батча на сервер.
//
// ПОЧЕМУ интерфейс, а не прямая зависимость от gRPC клиента?
// 1. Тестируемость: в тестах подставляем мок
// 2. Гибкость: можно заменить gRPC на NATS/HTTP без изменения Collector
// 3. Принцип инверсии зависимостей (SOLID)
type Sender interface {
	SendBatch(ctx context.Context, batch *pb.TelemetryBatch) error
}

// Collector — коллектор телеметрии с WAL и backpressure.
type Collector struct {
	wal    *wal.WAL
	sender Sender

	agentID    string
	machineIDs []string
	interval   time.Duration

	// collectFn — функция генерации телеметрии.
	// В Фазе 1 — фейковые данные. В будущем — реальный OPC UA клиент.
	// Через функцию Collector не привязан к конкретному источнику данных.
	collectFn func(machineIDs []string) *pb.TelemetryBatch
}

// NewCollector создаёт новый коллектор.
func NewCollector(
	w *wal.WAL,
	sender Sender,
	agentID string,
	machineIDs []string,
	interval time.Duration,
	collectFn func([]string) *pb.TelemetryBatch,
) *Collector {
	return &Collector{
		wal:        w,
		sender:     sender,
		agentID:    agentID,
		machineIDs: machineIDs,
		interval:   interval,
		collectFn:  collectFn,
	}
}

// Run запускает сбор и отправку телеметрии.
//
// Запускаются две горутины:
// 1. collectLoop — тикер → сбор данных → запись в WAL
// 2. sendLoop — чтение из WAL → отправка на сервер → при ошибке backoff
//
// sync.WaitGroup гарантирует что Run вернётся ТОЛЬКО когда обе горутины
// завершились. Это нужно для graceful shutdown.
func (c *Collector) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		c.collectLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		c.sendLoop(ctx)
	}()

	wg.Wait()
}

// collectLoop — цикл сбора телеметрии и записи в WAL.
func (c *Collector) collectLoop(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Collector] Остановка сбора телеметрии")
			return
		case <-ticker.C:
			batch := c.collectFn(c.machineIDs)
			batch.AgentId = c.agentID

			data, err := marshalWithPool(batch)
			if err != nil {
				log.Printf("[Collector] Ошибка сериализации: %v", err)
				continue
			}

			if err := c.wal.Write(data); err != nil {
				log.Printf("[Collector] Ошибка записи в WAL: %v", err)
				continue
			}

			log.Printf("[Collector] Записано в WAL: %d записей от %d станков",
				len(batch.Records), len(c.machineIDs))
		}
	}
}

// sendLoop — цикл отправки данных из WAL на сервер.
//
// АЛГОРИТМ:
// 1. Читаем все записи из WAL
// 2. Если пусто — ждём 1 секунду и повторяем
// 3. Отправляем каждый батч на сервер
// 4. Если ошибка — backoff (1с → 2с → 4с → ... → 30с) и повторяем
// 5. Если все отправлены — очищаем WAL (Truncate)
// 6. Сбрасываем backoff к начальному значению
func (c *Collector) sendLoop(ctx context.Context) {
	backoff := initialBackoff

	for {
		select {
		case <-ctx.Done():
			log.Println("[Collector] Остановка отправки телеметрии")
			return
		default:
		}

		entries, err := c.wal.ReadAll()
		if err != nil {
			log.Printf("[Collector] Ошибка чтения WAL: %v", err)
			sleepCtx(ctx, backoff)
			continue
		}

		if len(entries) == 0 {
			sleepCtx(ctx, 1*time.Second)
			continue
		}

		allSent := true
		for _, entry := range entries {
			var batch pb.TelemetryBatch
			if err := proto.Unmarshal(entry, &batch); err != nil {
				log.Printf("[Collector] Ошибка десериализации из WAL, пропускаю: %v", err)
				continue
			}

			if err := c.sender.SendBatch(ctx, &batch); err != nil {
				log.Printf("[Collector] Ошибка отправки (backoff %v): %v", backoff, err)
				allSent = false
				sleepCtx(ctx, backoff)
				backoff = nextBackoff(backoff)
				break
			}
		}

		if allSent {
			if err := c.wal.Truncate(); err != nil {
				log.Printf("[Collector] Ошибка очистки WAL: %v", err)
			} else {
				log.Printf("[Collector] WAL очищен, отправлено %d батчей", len(entries))
			}
			backoff = initialBackoff
		}
	}
}

// marshalWithPool сериализует protobuf с использованием sync.Pool.
func marshalWithPool(batch *pb.TelemetryBatch) ([]byte, error) {
	bufPtr := bufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	opts := proto.MarshalOptions{}
	data, err := opts.MarshalAppend(buf, batch)
	if err != nil {
		*bufPtr = buf
		bufPool.Put(bufPtr)
		return nil, fmt.Errorf("ошибка сериализации protobuf: %w", err)
	}

	result := make([]byte, len(data))
	copy(result, data)

	*bufPtr = data
	bufPool.Put(bufPtr)

	return result, nil
}

// nextBackoff вычисляет следующий интервал backoff.
// Пример: 1с → 2с → 4с → 8с → 16с → 30с → 30с...
func nextBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * backoffMultiplier)
	if next > maxBackoff {
		next = maxBackoff
	}
	return next
}

// sleepCtx — ожидание с поддержкой отмены контекста.
// Просыпается ИЛИ по таймеру, ИЛИ при отмене контекста — что раньше.
func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

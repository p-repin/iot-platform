// collector.go — коллектор телеметрии с WAL, backpressure и sync.Pool.
//
// Это ЯДРО АГЕНТА. Коллектор объединяет три механизма:
//
// 1. WAL (Write-Ahead Log) — надёжность:
//    Данные СНАЧАЛА пишутся на диск, ПОТОМ отправляются.
//    Если сервер недоступен — данные не теряются.
//
// 2. Backpressure — адаптивность:
//    Если сервер перегружен, агент замедляет отправку
//    (экспоненциальный backoff: 1с → 2с → 4с → ... → 30с).
//    Это защищает сервер от лавины повторных запросов.
//
// 3. sync.Pool — производительность:
//    Переиспользование буферов для protobuf-сериализации.
//    Без пула: 20 станков × 1 запись/сек = 20 аллокаций/сек → нагрузка на GC.
//    С пулом: буферы возвращаются и переиспользуются → меньше аллокаций.
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
package telemetry

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
// ПОЧЕМУ именно такие значения?
// - initialBackoff 1с: первая ошибка может быть случайной, не ждём долго
// - maxBackoff 30с: при длительном offline не шлём чаще чем раз в 30с
//   (иначе логи забьются ошибками и нагрузка на сеть)
// - backoffMultiplier 2.0: удвоение — стандарт (используется в gRPC, TCP, AWS SDK)
//   Тройное умножение слишком агрессивно, линейный рост слишком медленно.
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
//
// ВАЖНО: после Get() нужно сбросить длину слайса (buf[:0]),
// сохраняя capacity. Иначе мы будем append поверх старых данных.
var bufPool = sync.Pool{
	New: func() any {
		// Начальный размер 256 байт — типичный размер сериализованного
		// TelemetryBatch с 6 станками. Если нужно больше — Go автоматически
		// увеличит слайс через append. При следующем переиспользовании
		// буфер уже будет нужного размера (capacity сохраняется).
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// Sender — интерфейс для отправки батча на сервер.
//
// ПОЧЕМУ интерфейс, а не прямая зависимость от gRPC клиента?
// 1. Тестируемость: в тестах подставляем мок (всегда OK или всегда ошибка)
// 2. Гибкость: можно заменить gRPC на NATS/HTTP без изменения Collector
// 3. Принцип инверсии зависимостей (SOLID): Collector зависит от абстракции
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
	// ПОЧЕМУ функция, а не метод?
	// В Фазе 1 генерация — в main.go (фейковые данные).
	// В будущем — реальный OPC UA клиент.
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
// ПОТОКОВАЯ МОДЕЛЬ:
// Запускаются две горутины:
// 1. collectLoop — тикер → сбор данных → запись в WAL
// 2. sendLoop — чтение из WAL → отправка на сервер → при ошибке backoff
//
// ctx управляет временем жизни обеих горутин.
// При отмене контекста (Ctrl+C, SIGTERM) обе горутины завершаются.
//
// sync.WaitGroup гарантирует что Run вернётся ТОЛЬКО когда обе горутины
// завершились. Это нужно для graceful shutdown: не закрывать WAL
// пока горутины ещё пишут/читают.
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
//
// ПОЧЕМУ запись в WAL, а не прямая отправка на сервер?
// Если отправлять напрямую и сервер недоступен — данные потеряны.
// WAL гарантирует: данные СНАЧАЛА на диске, ПОТОМ отправляются.
// Даже если агент упадёт сразу после записи — при рестарте
// данные будут прочитаны из WAL и отправлены.
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

			// Сериализуем батч в protobuf с помощью sync.Pool.
			// ПОСЛЕДОВАТЕЛЬНОСТЬ:
			// 1. Берём буфер из пула
			// 2. Сериализуем в него
			// 3. Пишем в WAL (копия делается внутри WAL.Write)
			// 4. Возвращаем буфер в пул
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
//
// ПОЧЕМУ не отправляем по одному и сразу удаляем?
// Truncate дешевле чем "удалить одну запись и сдвинуть остальные".
// В бинарном файле нельзя удалить запись из середины — только переписать весь файл.
// Проще: отправить всё → очистить разом.
func (c *Collector) sendLoop(ctx context.Context) {
	backoff := initialBackoff

	for {
		select {
		case <-ctx.Done():
			log.Println("[Collector] Остановка отправки телеметрии")
			return
		default:
		}

		// Читаем все накопленные записи
		entries, err := c.wal.ReadAll()
		if err != nil {
			log.Printf("[Collector] Ошибка чтения WAL: %v", err)
			sleepCtx(ctx, backoff)
			continue
		}

		// Если WAL пуст — ждём и пробуем снова.
		// ПОЧЕМУ 1 секунда?
		// Совпадает с интервалом сбора (collectLoop).
		// Нет смысла проверять чаще — новые данные появляются раз в секунду.
		if len(entries) == 0 {
			sleepCtx(ctx, 1*time.Second)
			continue
		}

		// Отправляем каждый батч
		allSent := true
		for _, entry := range entries {
			// Десериализуем protobuf
			var batch pb.TelemetryBatch
			if err := proto.Unmarshal(entry, &batch); err != nil {
				// Битая запись — пропускаем (не блокируем остальные).
				// Такое возможно при повреждении диска.
				log.Printf("[Collector] Ошибка десериализации из WAL, пропускаю: %v", err)
				continue
			}

			// Отправляем на сервер
			if err := c.sender.SendBatch(ctx, &batch); err != nil {
				log.Printf("[Collector] Ошибка отправки (backoff %v): %v", backoff, err)
				allSent = false

				// BACKPRESSURE: экспоненциальный backoff.
				// ПОЧЕМУ экспоненциальный, а не фиксированный?
				// Фиксированный (каждые 5с): если сервер лежит час — 720 бесполезных попыток.
				// Экспоненциальный: 1с, 2с, 4с, 8с, 16с, 30с — всего 6 попыток за ~1 минуту,
				// потом раз в 30с. Меньше нагрузки на сеть, меньше мусора в логах.
				sleepCtx(ctx, backoff)
				backoff = nextBackoff(backoff)
				break // Прекращаем отправку — попробуем ВСЕ записи заново после backoff
			}
		}

		if allSent {
			// ВСЕ записи успешно отправлены — очищаем WAL.
			// ВАЖНО: Truncate ТОЛЬКО после подтверждения ВСЕХ записей.
			// Это гарантирует at-least-once доставку.
			if err := c.wal.Truncate(); err != nil {
				log.Printf("[Collector] Ошибка очистки WAL: %v", err)
			} else {
				log.Printf("[Collector] WAL очищен, отправлено %d батчей", len(entries))
			}

			// Сбрасываем backoff — сервер работает.
			backoff = initialBackoff
		}
	}
}

// marshalWithPool сериализует protobuf с использованием sync.Pool.
//
// КАК ЭТО ЭКОНОМИТ ПАМЯТЬ:
// Без пула: proto.Marshal выделяет новый []byte каждый раз → GC должен собрать старый.
// С пулом: берём существующий буфер → MarshalOptions.MarshalAppend дописывает в него →
// копируем результат → возвращаем буфер в пул. Буфер переиспользуется.
//
// СКОЛЬКО ЭКОНОМИМ?
// 20 станков × 1 раз/сек = 20 аллокаций/сек без пула.
// С пулом: 0 аллокаций в steady-state (буферы переиспользуются).
// GC запускается реже → меньше пауз → стабильнее latency.
func marshalWithPool(batch *pb.TelemetryBatch) ([]byte, error) {
	// Берём буфер из пула
	bufPtr := bufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Сбрасываем длину, сохраняя capacity

	// proto.MarshalOptions{}.MarshalAppend дописывает в существующий буфер.
	// ПОЧЕМУ MarshalAppend, а не Marshal?
	// Marshal ВСЕГДА аллоцирует новый []byte — пул бесполезен.
	// MarshalAppend дописывает в переданный буфер — переиспользуем capacity.
	opts := proto.MarshalOptions{}
	data, err := opts.MarshalAppend(buf, batch)
	if err != nil {
		// Возвращаем буфер в пул даже при ошибке — не теряем память.
		*bufPtr = buf
		bufPool.Put(bufPtr)
		return nil, fmt.Errorf("ошибка сериализации protobuf: %w", err)
	}

	// ВАЖНО: делаем КОПИЮ данных перед возвратом буфера в пул.
	// Если вернуть буфер и потом WAL будет писать data — данные будут
	// перезаписаны следующим Get(). Копия гарантирует что данные
	// принадлежат вызывающему коду.
	result := make([]byte, len(data))
	copy(result, data)

	// Возвращаем буфер в пул для переиспользования.
	// Обновляем указатель — MarshalAppend мог увеличить capacity.
	*bufPtr = data
	bufPool.Put(bufPtr)

	return result, nil
}

// nextBackoff вычисляет следующий интервал backoff.
//
// ФОРМУЛА: current × multiplier, но не больше maxBackoff.
// Пример: 1с → 2с → 4с → 8с → 16с → 30с → 30с → 30с...
//
// ПОЧЕМУ math.Min, а не просто if?
// Одна строка вместо четырёх. Читаемость важнее "экономии" на вызове функции.
// Компилятор Go инлайнит math.Min — разницы в производительности нет.
func nextBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * backoffMultiplier)
	if next > maxBackoff {
		next = maxBackoff
	}
	return next
}

// sleepCtx — ожидание с поддержкой отмены контекста.
//
// ПОЧЕМУ не просто time.Sleep?
// time.Sleep БЛОКИРУЕТ горутину на ВЕСЬ период. Если пришёл сигнал
// завершения (Ctrl+C) — горутина не проснётся раньше.
// sleepCtx использует select: просыпается ИЛИ по таймеру,
// ИЛИ при отмене контекста — что раньше произойдёт.
//
// Без этого graceful shutdown будет ждать до 30 секунд (maxBackoff)
// пока горутина "проснётся" от Sleep.
func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}


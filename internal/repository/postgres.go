// postgres.go — реализация TelemetryRepository для TimescaleDB (PostgreSQL).
//
// Использует pgxpool — пул соединений к PostgreSQL.
//
// ПОЧЕМУ pgxpool, а не одно соединение (pgx.Conn)?
// gRPC сервер обрабатывает запросы ПАРАЛЛЕЛЬНО (в отдельных горутинах).
// Если все горутины используют одно соединение — они будут ждать в очереди.
// pgxpool поддерживает пул соединений: каждая горутина берёт свободное
// соединение из пула, выполняет запрос, возвращает обратно.
//
// ПОЧЕМУ pgx, а не database/sql?
// 1. database/sql — универсальный интерфейс для ЛЮБОЙ БД (MySQL, SQLite, etc.)
//    pgx — специализированный драйвер для PostgreSQL.
// 2. pgx напрямую работает с PostgreSQL wire protocol — меньше накладных расходов.
// 3. pgx поддерживает Batch API — отправляет несколько запросов за один roundtrip.
//    database/sql отправляет каждый запрос отдельно.
// 4. pgx нативно работает с PostgreSQL-типами (UUID, JSONB, TIMESTAMPTZ).
//
// На собеседовании: "Почему pgx, а не стандартный database/sql?"
// Ответ: производительность (native protocol), batch API, нативные типы PostgreSQL.
package repository

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pzmash/iot-platform/internal/entity"
)

// PostgresRepo — реализация TelemetryRepository для TimescaleDB.
type PostgresRepo struct {
	pool *pgxpool.Pool
}

// NewPostgresRepo создаёт пул соединений к PostgreSQL и проверяет подключение.
//
// ПОЧЕМУ Ping при создании?
// Fail fast: если БД недоступна — узнаём СРАЗУ при старте сервера,
// а не через минуту когда придёт первый запрос от агента.
// Оператор видит ошибку в логах и чинит до того, как потеряются данные.
func NewPostgresRepo(ctx context.Context, dsn string) (*PostgresRepo, error) {
	// pgxpool.New парсит DSN и создаёт пул соединений.
	// Пул по умолчанию: min=0, max=4 (pgx v5 default).
	// Для нашей нагрузки (20 станков × 1 батч/сек) хватает с запасом.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("не удалось создать пул соединений: %w", err)
	}

	// Ping — проверяем что БД отвечает.
	// Под капотом: берёт соединение из пула → отправляет простой запрос → возвращает.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("БД не отвечает: %w", err)
	}

	slog.Info("Подключение к TimescaleDB установлено",
		"dsn_host", pool.Config().ConnConfig.Host,
		"dsn_db", pool.Config().ConnConfig.Database,
		"max_conns", pool.Config().MaxConns,
	)

	return &PostgresRepo{pool: pool}, nil
}

// InsertBatch записывает пакет телеметрии в TimescaleDB одной транзакцией.
//
// ИДЕМПОТЕНТНОСТЬ через ON CONFLICT:
// Агент использует at-least-once доставку — одна и та же запись может
// прийти дважды (после перезапуска, при повторной отправке из WAL).
// ON CONFLICT (record_id, time) DO UPDATE гарантирует:
// - Первая вставка: INSERT (новая строка)
// - Повторная вставка: UPDATE (перезапись значений, не дубликат)
//
// ПОЧЕМУ pgx.Batch, а не одна строка с многострочным VALUES?
// pgx.Batch отправляет ВСЕ запросы за ОДИН network roundtrip.
// Альтернатива — собрать один INSERT с VALUES ($1,$2,...),($3,$4,...):
// - Плюс: одна транзакция, один запрос
// - Минус: нужно динамически собирать SQL-строку с $N параметрами
// pgx.Batch проще, и при небольших батчах (6-100 записей) разница минимальна.
//
// ТРАНЗАКЦИЯ:
// Весь батч записывается в одной транзакции.
// Если одна запись упадёт — откатываются ВСЕ. Это нормально:
// агент пришлёт весь батч повторно (at-least-once).
func (r *PostgresRepo) InsertBatch(ctx context.Context, records []entity.Record) error {
	// Начинаем транзакцию.
	// ПОЧЕМУ транзакция, а не просто Batch без неё?
	// Без транзакции каждый запрос в Batch выполняется независимо.
	// Если 3-й из 6 запросов упадёт — первые 2 уже записаны, остальные нет.
	// Это partial write: часть данных в БД, часть потеряна.
	// С транзакцией: или ВСЕ записаны, или НИЧЕГО. Агент пришлёт повторно.
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("не удалось начать транзакцию: %w", err)
	}
	// defer Rollback — БЕЗОПАСНАЯ сетка.
	// Если tx.Commit() выполнится успешно — Rollback ничего не сделает (tx уже закрыта).
	// Если где-то выше произойдёт return err — Rollback откатит незавершённую транзакцию.
	// Без defer Rollback: при панике или раннем return транзакция "повиснет",
	// заблокирует строки и съест соединение из пула.
	defer tx.Rollback(ctx)

	// pgx.Batch — контейнер для нескольких SQL-запросов.
	// Все запросы отправляются на сервер за ОДИН roundtrip,
	// но выполняются последовательно на стороне PostgreSQL.
	batch := &pgx.Batch{}

	for _, rec := range records {
		// ON CONFLICT (record_id, time) DO UPDATE SET ...
		// ПОЧЕМУ DO UPDATE, а не DO NOTHING?
		// DO NOTHING: если запись уже есть — просто игнорируем.
		//   Проблема: если агент отправил данные с опечаткой, потом исправил
		//   и отправил заново — исправление не применится.
		// DO UPDATE: перезаписываем значения — последняя версия побеждает.
		//   Это корректное поведение для at-least-once: данные всегда актуальны.
		batch.Queue(
			`INSERT INTO telemetry (time, machine_id, temperature, vibration, pressure, record_id)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (record_id, time) DO UPDATE SET
			   temperature = EXCLUDED.temperature,
			   vibration   = EXCLUDED.vibration,
			   pressure    = EXCLUDED.pressure`,
			rec.Timestamp,   // $1
			rec.MachineID,   // $2
			rec.Temperature, // $3
			rec.Vibration,   // $4
			rec.Pressure,    // $5
			rec.RecordID,    // $6
		)
	}

	// SendBatch отправляет все запросы за один roundtrip.
	// br (BatchResults) позволяет прочитать результаты каждого запроса.
	br := tx.SendBatch(ctx, batch)

	// Читаем результаты КАЖДОГО запроса из батча.
	// ПОЧЕМУ нужно читать, а не просто Close()?
	// 1. br.Close() без чтения результатов может скрыть ошибки
	// 2. Если один INSERT упал — мы узнаем какой именно
	for i := 0; i < len(records); i++ {
		_, err := br.Exec()
		if err != nil {
			br.Close()
			return fmt.Errorf("ошибка записи record_id=%s: %w", records[i].RecordID, err)
		}
	}

	// Закрываем BatchResults ДО коммита.
	// pgx требует закрыть BatchResults перед следующими операциями на транзакции.
	if err := br.Close(); err != nil {
		return fmt.Errorf("ошибка закрытия batch results: %w", err)
	}

	// Коммитим транзакцию — все записи атомарно записаны в БД.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	return nil
}

// Close закрывает пул соединений к PostgreSQL.
// Вызывается при graceful shutdown сервера.
// После Close все попытки выполнить запрос вернут ошибку.
func (r *PostgresRepo) Close() {
	r.pool.Close()
}

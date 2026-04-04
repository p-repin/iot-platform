// Package wal — Write-Ahead Log для надёжной доставки телеметрии.
//
// ЗАЧЕМ WAL?
// Агент собирает данные с 20+ станков каждую секунду.
// Если сервер недоступен (сеть упала, сервер перезапускается) —
// данные НЕ ДОЛЖНЫ теряться. WAL записывает данные НА ДИСК
// перед отправкой на сервер. При восстановлении связи —
// отправляем из WAL.
//
// ФОРМАТ ЗАПИСИ:
// +----------------+----------------+-------------------+
// | Length (4B, BE) | CRC32 (4B, BE) | Payload (protobuf) |
// +----------------+----------------+-------------------+
//
// - Length: размер payload в байтах (big-endian uint32)
// - CRC32: контрольная сумма payload (IEEE, big-endian) — для обнаружения повреждений
// - Payload: protobuf-сериализованный TelemetryBatch
//
// ПОЧЕМУ бинарный формат, а не JSON?
// 1. Компактность: protobuf в 3-10 раз меньше JSON
// 2. Скорость: нет парсинга строк, прямое чтение по смещениям
// 3. CRC32: гарантирует целостность (если питание пропало посередине записи —
//    CRC не совпадёт и мы пропустим битую запись)
//
// ПОЧЕМУ один файл, а не сегменты?
// Для 20 станков × 1 запись/сек × ~100 байт = ~2 КБ/сек = ~170 МБ/сутки.
// Один файл справляется. В продакшене при больших объёмах разбивают на сегменты
// (как в Kafka/PostgreSQL WAL), но для нашего масштаба — overkill.
//
// ГАРАНТИЯ ДОСТАВКИ: at-least-once.
// Запись удаляется из WAL ТОЛЬКО после подтверждения сервером.
// Если подтверждение потерялось — отправим повторно (сервер сделает upsert).
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// headerSize — размер заголовка записи: 4 байта длина + 4 байта CRC.
const headerSize = 8

// WAL — Write-Ahead Log для буферизации телеметрии на диске.
//
// Потокобезопасен: защищён мьютексом.
// ПОЧЕМУ мьютекс, а не каналы?
// WAL — это файловая операция (seek + read/write). Мьютекс проще и эффективнее
// для защиты доступа к файлу. Каналы хороши для передачи данных между горутинами,
// а мьютекс — для защиты разделяемого ресурса (файл).
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// Open открывает или создаёт WAL файл.
//
// ПОЧЕМУ Open, а не New?
// - New обычно создаёт объект в памяти
// - Open подчёркивает что мы работаем с файловой системой
//   (по аналогии с os.Open, sql.Open)
//
// os.O_CREATE — создать файл если не существует
// os.O_RDWR — чтение И запись (читаем при восстановлении, пишем новые записи)
// 0644 — владелец читает/пишет, остальные только читают (стандартные права для файлов данных)
//
// ПОЧЕМУ нет O_APPEND?
// На Windows файл с O_APPEND нельзя Truncate — ОС возвращает "Access is denied".
// Вместо O_APPEND мы вручную делаем Seek(0, io.SeekEnd) перед каждой записью.
// Мьютекс гарантирует что между Seek и Write никто не вклинится.
func Open(dir string) (*WAL, error) {
	// Создаём директорию если не существует.
	// MkdirAll — рекурсивно создаёт все вложенные директории.
	// ПОЧЕМУ MkdirAll, а не Mkdir?
	// Mkdir создаёт ОДНУ директорию. Если dir = "./data/wal",
	// а ./data не существует — Mkdir упадёт. MkdirAll создаст оба уровня.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("не удалось создать директорию WAL %s: %w", dir, err)
	}

	path := filepath.Join(dir, "telemetry.wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("не удалось открыть WAL файл %s: %w", path, err)
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// Write записывает данные в WAL.
//
// ПОСЛЕДОВАТЕЛЬНОСТЬ:
// 1. Вычисляем CRC32 от payload
// 2. Записываем заголовок: [length][CRC32]
// 3. Записываем payload
// 4. Вызываем Sync() — ПРИНУДИТЕЛЬНО сбрасываем на диск
//
// ПОЧЕМУ Sync()?
// Без Sync() данные могут остаться в буфере ОС (page cache).
// Если питание пропадёт — данные потеряются. Sync() гарантирует
// что данные физически на диске. Это дорого (ждём запись на диск),
// но для WAL — обязательно. Без этого WAL не выполняет свою функцию.
//
// Параметр buf — сериализованный protobuf (TelemetryBatch).
// Вызывающий код отвечает за сериализацию — WAL не знает про protobuf,
// он работает с сырыми байтами. Это делает WAL переиспользуемым.
func (w *WAL) Write(buf []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Перемещаем курсор в конец файла перед записью.
	// Заменяет O_APPEND, который на Windows конфликтует с Truncate.
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("ошибка перемотки WAL в конец: %w", err)
	}

	// Заголовок: 4 байта длина + 4 байта CRC32
	var header [headerSize]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(len(buf)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(buf))

	// Записываем заголовок + payload.
	// ПОЧЕМУ два вызова Write, а не один (append header+buf)?
	// Можно было бы: combined := append(header[:], buf...)
	// Но это аллоцирует НОВЫЙ слайс (header[:] + buf).
	// Два вызова Write — без аллокаций. Ядро ОС буферизует оба вызова
	// и отправит одним syscall при Sync().
	if _, err := w.file.Write(header[:]); err != nil {
		return fmt.Errorf("ошибка записи заголовка WAL: %w", err)
	}
	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("ошибка записи payload WAL: %w", err)
	}

	// Sync — КРИТИЧЕСКИ ВАЖЕН для WAL.
	// Это единственное место где мы ГАРАНТИРУЕМ что данные на диске.
	return w.file.Sync()
}

// ReadAll читает ВСЕ записи из WAL.
//
// Используется при старте агента для отправки данных,
// накопленных за время простоя (crash recovery).
//
// ОБРАБОТКА БИТЫХ ЗАПИСЕЙ:
// Если CRC не совпадает или запись обрезана (питание пропало
// посередине записи) — пропускаем и прекращаем чтение.
// Это безопасно: мы теряем максимум ОДНУ последнюю запись,
// а не весь WAL. at-least-once гарантия сохраняется для
// всех полностью записанных записей.
func (w *WAL) ReadAll() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Перематываем в начало файла для чтения.
	// ПОЧЕМУ Seek(0, io.SeekStart)?
	// После предыдущих Write курсор файла стоит В КОНЦЕ.
	// Для чтения нужно вернуться в начало.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("ошибка перемотки WAL: %w", err)
	}

	var entries [][]byte
	var header [headerSize]byte

	for {
		// Читаем заголовок (8 байт)
		if _, err := io.ReadFull(w.file, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Конец файла или обрезанный заголовок — нормальное завершение
				break
			}
			return nil, fmt.Errorf("ошибка чтения заголовка WAL: %w", err)
		}

		length := binary.BigEndian.Uint32(header[0:4])
		expectedCRC := binary.BigEndian.Uint32(header[4:8])

		// Читаем payload
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Обрезанный payload — запись не была полностью записана
				// (питание пропало посередине). Пропускаем.
				break
			}
			return nil, fmt.Errorf("ошибка чтения payload WAL: %w", err)
		}

		// Проверяем CRC — защита от битых данных
		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != expectedCRC {
			// CRC не совпал — данные повреждены. Прекращаем чтение.
			// ПОЧЕМУ break, а не continue?
			// Если одна запись повреждена, все последующие смещения
			// будут неверными (мы не знаем где начинается следующая запись).
			// Безопаснее остановиться.
			break
		}

		entries = append(entries, payload)
	}

	return entries, nil
}

// Truncate очищает WAL после успешной отправки всех записей.
//
// ПОЧЕМУ Truncate(0), а не удаление файла?
// - Удаление + создание = два syscall + потенциальная гонка
// - Truncate = один syscall, файловый дескриптор остаётся валидным
// - Не нужно заново открывать файл
//
// КОГДА вызывать?
// ТОЛЬКО после того как сервер ПОДТВЕРДИЛ приём всех записей из WAL.
// Если вызвать до подтверждения — потеряем данные.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Truncate(0) — обрезаем файл до 0 байт (очищаем).
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("ошибка очистки WAL: %w", err)
	}

	// Перемещаем курсор в начало.
	// ПОЧЕМУ нужен Seek после Truncate?
	// Truncate меняет РАЗМЕР файла, но НЕ двигает курсор.
	// Если курсор был на позиции 1000 (после нескольких Write),
	// следующий Write запишет на позицию 1000, а файл размером 0 —
	// ОС заполнит промежуток нулями. Seek(0) возвращает курсор в начало.
	_, err := w.file.Seek(0, io.SeekStart)
	return err
}

// Size возвращает текущий размер WAL файла в байтах.
//
// Используется для мониторинга backpressure:
// если размер растёт — сервер не успевает принимать данные.
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Close закрывает WAL файл.
// ВСЕГДА вызывай defer wal.Close() после Open.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}
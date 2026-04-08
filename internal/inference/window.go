// Package inference — ML-инференс для обнаружения аномалий в телеметрии.
//
// Пакет реализует real-time предсказание вероятности брака
// по данным датчиков с производственного оборудования.
package inference

import (
	"sync"

	"github.com/pzmash/iot-platform/internal/entity"
)

// SlidingWindow — скользящее окно замеров для каждого станка.
//
// ЗАЧЕМ скользящее окно?
// ML-модель принимает на вход не один замер, а последовательность.
// Это позволяет модели видеть ТРЕНДЫ:
// - Температура росла 10 замеров подряд → перегрев
// - Вибрация резко подскочила после стабильных значений → износ подшипника
// Один замер не даёт контекста — модель не поймёт, 75°C это нагрев или охлаждение.
//
// ПОЧЕМУ sync.RWMutex?
// SlidingWindow вызывается из gRPC обработчиков, которые работают
// в разных горутинах. RWMutex позволяет:
// - Несколько горутин читают одновременно (маловероятно, но безопасно)
// - Запись (Push) блокирует остальных — гарантия целостности окна
type SlidingWindow struct {
	mu   sync.RWMutex
	// windows хранит последние N записей для каждого станка.
	// Ключ — machineID ("CNC-01"), значение — циклический буфер записей.
	windows map[string]*machineWindow
	// size — размер окна (количество замеров для одного предсказания).
	// По умолчанию 10 — экспериментально подобранное значение:
	// при частоте замеров 1/сек это 10 секунд контекста.
	size int
}

// machineWindow — циклический буфер замеров для одного станка.
//
// ПОЧЕМУ циклический буфер, а не slice с append/shift?
// - Нет аллокаций: буфер создаётся один раз, затем перезаписывается
// - O(1) добавление: просто двигаем указатель pos
// - Нет копирования: slice append/shift копирует данные при каждом сдвиге
type machineWindow struct {
	buf   []entity.Record // фиксированный буфер размером size
	pos   int             // текущая позиция записи (0..size-1)
	count int             // сколько записей добавлено (для проверки заполненности)
}

// NewSlidingWindow создаёт новое скользящее окно с указанным размером.
func NewSlidingWindow(size int) *SlidingWindow {
	return &SlidingWindow{
		windows: make(map[string]*machineWindow),
		size:    size,
	}
}

// Push добавляет запись в окно станка и возвращает вектор фич для ML-модели.
//
// Возвращает nil, если окно ещё не заполнено (недостаточно контекста).
// Когда окно заполнено — возвращает flatten-вектор:
// [t1_temp, t1_vib, t1_pres, t2_temp, t2_vib, t2_pres, ..., tN_temp, tN_vib, tN_pres]
//
// ПОЧЕМУ flatten, а не 2D?
// ONNX Runtime принимает 1D-тензор. Flatten-порядок должен совпадать
// с тем, как модель обучалась (train_anomaly.py генерирует данные в том же порядке).
func (sw *SlidingWindow) Push(record entity.Record) []float64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	mw, ok := sw.windows[record.MachineID]
	if !ok {
		// Первый замер от этого станка — создаём буфер
		mw = &machineWindow{
			buf: make([]entity.Record, sw.size),
		}
		sw.windows[record.MachineID] = mw
	}

	// Записываем в циклический буфер
	mw.buf[mw.pos] = record
	mw.pos = (mw.pos + 1) % sw.size
	if mw.count < sw.size {
		mw.count++
	}

	// Окно ещё не заполнено — недостаточно данных для предсказания
	if mw.count < sw.size {
		return nil
	}

	// Окно заполнено — формируем вектор фич.
	// Читаем от самого старого замера (pos) к самому новому (pos-1),
	// чтобы модель видела хронологический порядок.
	features := make([]float64, 0, sw.size*3)
	for i := 0; i < sw.size; i++ {
		idx := (mw.pos + i) % sw.size // pos указывает на самый старый элемент
		r := mw.buf[idx]
		features = append(features, r.Temperature, r.Vibration, r.Pressure)
	}

	return features
}

// Size возвращает размер окна.
func (sw *SlidingWindow) Size() int {
	return sw.size
}

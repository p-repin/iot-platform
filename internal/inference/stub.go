//go:build !cgo

// stub.go — статистический детектор аномалий (без CGO/ONNX Runtime).
//
// Реализует тот же интерфейс Predictor, что и ONNXPredictor.
// Используется когда CGO недоступен (Windows без GCC, CI без ONNX Runtime).
//
// АЛГОРИТМ: Z-score по каждому параметру.
// Z-score = (значение - среднее) / стандартное_отклонение
// Если Z-score хотя бы одного параметра > 2.0 — это аномалия.
//
// ПОЧЕМУ Z-score, а не Isolation Forest?
// Z-score — простейший метод обнаружения аномалий:
// - Не требует обучения (нормальные диапазоны зашиты в код)
// - Работает без внешних зависимостей
// - Достаточен для демонстрации архитектуры
// На собеседовании: "В проде используем ONNX Runtime (Isolation Forest),
// а для dev/CI — Z-score stub с тем же интерфейсом Predictor."

package inference

import (
	"math"
)

// StubPredictor — статистический детектор аномалий.
//
// Реализует Predictor interface без внешних зависимостей.
// Нормальные диапазоны совпадают с train_anomaly.py:
// - temperature: 40-80°C
// - vibration: 0.5-3.0 mm/s
// - pressure: 4.0-7.0 bar
type StubPredictor struct {
	// nFeatures — количество фич на входе (window_size * 3)
	nFeatures int
}

// NewStubPredictor создаёт статистический предиктор.
//
// Аргументы:
// - inputSize: количество входных фич (window_size * 3)
//
// ПОЧЕМУ не нужен путь к модели?
// StubPredictor не загружает модель — нормальные диапазоны зашиты в код.
// Это упрощение для dev-окружения.
func NewStubPredictor(inputSize int) *StubPredictor {
	return &StubPredictor{
		nFeatures: inputSize,
	}
}

// Нормальные диапазоны параметров (mean ± std).
// Совпадают с NORMAL_RANGES из train_anomaly.py.
var paramStats = [3]struct {
	mean   float64
	stddev float64
}{
	{mean: 60.0, stddev: 12.0},  // temperature: (40+80)/2=60, (80-40)/3.3≈12
	{mean: 1.75, stddev: 0.75},  // vibration: (0.5+3.0)/2=1.75, (3.0-0.5)/3.3≈0.75
	{mean: 5.5, stddev: 0.9},    // pressure: (4.0+7.0)/2=5.5, (7.0-4.0)/3.3≈0.9
}

// Predict вычисляет anomaly score на основе Z-score.
//
// features — flatten-вектор фич из SlidingWindow.Push():
// [t1_temp, t1_vib, t1_pres, t2_temp, t2_vib, t2_pres, ...]
//
// Алгоритм:
// 1. Для каждого замера в окне вычисляем Z-score по каждому параметру
// 2. Берём максимальный |Z-score| по всем замерам
// 3. Нормализуем в диапазон 0..1 через sigmoid-подобную функцию
//
// Возвращает score (0..1): чем ближе к 1, тем вероятнее аномалия.
func (p *StubPredictor) Predict(features []float64) (float64, error) {
	maxZ := 0.0

	// Перебираем все замеры в окне (каждый = 3 фичи)
	for i := 0; i < len(features); i += 3 {
		for j := 0; j < 3 && i+j < len(features); j++ {
			val := features[i+j]
			stat := paramStats[j]

			// Z-score: насколько значение отклоняется от нормы (в стандартных отклонениях)
			z := math.Abs(val-stat.mean) / stat.stddev
			if z > maxZ {
				maxZ = z
			}
		}
	}

	// Нормализуем Z-score в диапазон 0..1.
	// Используем sigmoid-подобную функцию: score = 1 - exp(-z²/8)
	// - Z=0 (идеально нормально) → score ≈ 0
	// - Z=2 (граница нормы, ~95%) → score ≈ 0.39
	// - Z=3 (аномалия, ~99.7%) → score ≈ 0.67
	// - Z=4 (сильная аномалия) → score ≈ 0.86
	// - Z=5+ → score → 1.0
	score := 1.0 - math.Exp(-maxZ*maxZ/8.0)

	return score, nil
}

// Close — no-op для StubPredictor (нет внешних ресурсов для освобождения).
func (p *StubPredictor) Close() {}

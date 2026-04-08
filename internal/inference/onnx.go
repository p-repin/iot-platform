//go:build cgo

// onnx.go — реальная обёртка ONNX Runtime (требует CGO + GCC).
//
// Для сборки с реальным ONNX Runtime:
//   CGO_ENABLED=1 go build ./...
//
// Без CGO используется StubPredictor из stub.go —
// статистический детектор аномалий с той же архитектурой.

package inference

import (
	"fmt"
	"os"

	ort "github.com/yalue/onnxruntime_go"
)

// ONNXPredictor — обёртка над ONNX Runtime для инференса модели аномалий.
//
// ONNX Runtime — универсальный движок для запуска ML-моделей.
// Преимущества перед вызовом Python:
// - Нативная скорость (C++ под капотом)
// - Нет зависимости от Python в продакшне
// - Поддержка CPU/GPU без изменения кода
//
// ПОЧЕМУ yalue/onnxruntime_go?
// Это единственная зрелая Go-библиотека для ONNX Runtime.
// Она оборачивает C API через CGO, предоставляя Go-idiomatic интерфейс.
// Требует наличия shared library (onnxruntime.dll / libonnxruntime.so).
type ONNXPredictor struct {
	session *ort.AdvancedSession
	// inputShape — форма входного тензора [1, N] где N = window_size * 3.
	// Batch size = 1, т.к. мы предсказываем по одному окну за раз.
	inputShape ort.Shape
	// outputShapeLabels — форма выхода с метками (1/-1).
	outputShapeLabels ort.Shape
	// outputShapeScores — форма выхода со скорами.
	// Isolation Forest возвращает scores для двух классов: [-1, 1].
	outputShapeScores ort.Shape
}

// NewONNXPredictor загружает ONNX-модель и создаёт сессию для инференса.
//
// sharedLibPath — путь к onnxruntime.dll (Windows) или libonnxruntime.so (Linux).
// modelPath — путь к .onnx файлу модели.
// inputSize — количество входных фич (window_size * 3).
//
// ПОЧЕМУ отдельный sharedLibPath?
// ONNX Runtime shared library не поставляется с Go-пакетом.
// Её нужно скачать отдельно с github.com/microsoft/onnxruntime/releases
// и указать путь. Это позволяет:
// - Выбирать версию (CPU/GPU/DirectML)
// - Контролировать размер артефакта (DLL ~30 МБ)
// - Работать в air-gap окружении (скопировать DLL заранее)
func NewONNXPredictor(sharedLibPath, modelPath string, inputSize int) (*ONNXPredictor, error) {
	// Проверяем наличие файлов до инициализации —
	// ошибки ONNX Runtime бывают криптичными, лучше упасть с понятным сообщением
	if _, err := os.Stat(sharedLibPath); err != nil {
		return nil, fmt.Errorf("ONNX Runtime shared library не найдена: %s: %w", sharedLibPath, err)
	}
	if _, err := os.Stat(modelPath); err != nil {
		return nil, fmt.Errorf("ONNX модель не найдена: %s: %w", modelPath, err)
	}

	// Инициализация ONNX Runtime environment.
	// SetSharedLibraryPath нужно вызвать ДО InitializeEnvironment.
	ort.SetSharedLibraryPath(sharedLibPath)
	if err := ort.InitializeEnvironment(); err != nil {
		return nil, fmt.Errorf("ошибка инициализации ONNX Runtime: %w", err)
	}

	// Формы тензоров:
	// Вход: [1, inputSize] — один пример, inputSize фич
	// Выход labels: [1] — метка (-1 аномалия, 1 норма)
	// Выход scores: [1, 2] — вероятности для классов [-1, 1]
	inputShape := ort.NewShape(1, int64(inputSize))
	outputShapeLabels := ort.NewShape(1)
	outputShapeScores := ort.NewShape(1, 2)

	// Создаём входной и выходные тензоры.
	// AdvancedSession позволяет явно указать имена входов/выходов —
	// это надёжнее чем полагаться на порядок (который может измениться при переобучении).
	inputTensor, err := ort.NewEmptyTensor[float32](inputShape)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания входного тензора: %w", err)
	}
	defer inputTensor.Destroy()

	outputLabels, err := ort.NewEmptyTensor[int64](outputShapeLabels)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания выходного тензора labels: %w", err)
	}
	defer outputLabels.Destroy()

	outputScores, err := ort.NewEmptyTensor[float32](outputShapeScores)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания выходного тензора scores: %w", err)
	}
	defer outputScores.Destroy()

	// Имена входов/выходов берём из модели (видны в train_anomaly.py → sess.get_inputs/outputs).
	// skl2onnx для Isolation Forest генерирует:
	// Вход: "X" — входные фичи
	// Выходы: "label" — метка, "probabilities" — скоры
	session, err := ort.NewAdvancedSession(
		modelPath,
		[]string{"X"},                          // имена входов
		[]string{"label", "probabilities"},     // имена выходов
		[]ort.ArbitraryTensor{inputTensor},     // входные тензоры
		[]ort.ArbitraryTensor{outputLabels, outputScores}, // выходные тензоры
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания ONNX сессии: %w", err)
	}

	return &ONNXPredictor{
		session:           session,
		inputShape:        inputShape,
		outputShapeLabels: outputShapeLabels,
		outputShapeScores: outputShapeScores,
	}, nil
}

// Predict выполняет инференс модели и возвращает anomaly score (0..1).
//
// features — flatten-вектор фич из SlidingWindow.Push().
// Возвращает score: чем ближе к 1, тем вероятнее аномалия.
//
// ПОЧЕМУ возвращаем score, а не bool?
// Score (0..1) позволяет настраивать порог на уровне сервиса:
// - threshold=0.5 — чувствительный (больше ложных срабатываний)
// - threshold=0.8 — строгий (пропустит часть аномалий, но меньше ложных)
// На собеседовании: "как вы настраиваете чувствительность ML-модели?"
func (p *ONNXPredictor) Predict(features []float64) (float64, error) {
	// Конвертируем float64 → float32 (ONNX работает с float32 для скорости)
	input := make([]float32, len(features))
	for i, v := range features {
		input[i] = float32(v)
	}

	// Создаём тензоры для этого вызова.
	// Каждый вызов Predict создаёт новые тензоры — это проще и потокобезопаснее,
	// чем переиспользовать (AdvancedSession не потокобезопасен).
	inputTensor, err := ort.NewTensor(p.inputShape, input)
	if err != nil {
		return 0, fmt.Errorf("ошибка создания входного тензора: %w", err)
	}
	defer inputTensor.Destroy()

	outputLabels, err := ort.NewEmptyTensor[int64](p.outputShapeLabels)
	if err != nil {
		return 0, fmt.Errorf("ошибка создания выходного тензора: %w", err)
	}
	defer outputLabels.Destroy()

	outputScores, err := ort.NewEmptyTensor[float32](p.outputShapeScores)
	if err != nil {
		return 0, fmt.Errorf("ошибка создания выходного тензора: %w", err)
	}
	defer outputScores.Destroy()

	// Запускаем инференс.
	// Session.Run() — синхронный вызов, блокирует до получения результата.
	// На CPU инференс Isolation Forest занимает ~0.1-1мс — приемлемо для real-time.
	err = p.session.Run(
		[]ort.ArbitraryTensor{inputTensor},
		[]ort.ArbitraryTensor{outputLabels, outputScores},
	)
	if err != nil {
		return 0, fmt.Errorf("ошибка инференса: %w", err)
	}

	// Читаем scores — массив [score_anomaly, score_normal].
	// Isolation Forest через skl2onnx возвращает "probabilities":
	// [0] — score для класса -1 (аномалия)
	// [1] — score для класса 1 (норма)
	// Берём score аномалии [0] как anomaly score.
	scores := outputScores.GetData()
	if len(scores) < 2 {
		return 0, fmt.Errorf("неожиданный формат выхода: ожидали 2 скора, получили %d", len(scores))
	}

	// scores[0] — вероятность аномалии (класс -1)
	anomalyScore := float64(scores[0])

	return anomalyScore, nil
}

// Close освобождает ресурсы ONNX Runtime.
//
// ВАЖНО: вызывать при graceful shutdown сервера.
// ONNX Runtime держит память GPU/CPU — утечка при многократном создании сессий.
func (p *ONNXPredictor) Close() {
	if p.session != nil {
		_ = p.session.Destroy()
	}
	// DestroyEnvironment очищает глобальное состояние ONNX Runtime.
	// Вызывается один раз при завершении программы.
	_ = ort.DestroyEnvironment()
}

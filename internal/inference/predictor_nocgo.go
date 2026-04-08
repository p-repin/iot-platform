//go:build !cgo

package inference

import "log/slog"

// NewPredictor создаёт статистический предиктор (без ONNX Runtime).
//
// При сборке без CGO (CGO_ENABLED=0, Windows без GCC) используется
// StubPredictor на основе Z-score. Архитектура и интерфейс идентичны
// реальному ONNX-предиктору.
//
// sharedLibPath и modelPath игнорируются — StubPredictor не использует файлы.
func NewPredictor(sharedLibPath, modelPath string, inputSize int) (Predictor, error) {
	slog.Info("CGO отключен: используется StubPredictor (Z-score) вместо ONNX Runtime",
		"input_size", inputSize,
	)
	return NewStubPredictor(inputSize), nil
}

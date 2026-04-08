//go:build cgo

package inference

// NewPredictor создаёт предиктор на основе ONNX Runtime (требует CGO).
//
// При сборке с CGO_ENABLED=1 используется реальный ONNX Runtime.
// Это промышленный вариант — Isolation Forest обучена на данных станков.
func NewPredictor(sharedLibPath, modelPath string, inputSize int) (Predictor, error) {
	return NewONNXPredictor(sharedLibPath, modelPath, inputSize)
}

package inference

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pzmash/iot-platform/internal/entity"
)

// Predictor — интерфейс для ML-предсказаний.
//
// ЗАЧЕМ интерфейс, если есть ONNXPredictor?
// Dependency Inversion: сервис зависит от абстракции, а не от реализации.
// Это позволяет:
// - Тестировать без ONNX Runtime (мок-предиктор)
// - Заменить модель (например, на TensorFlow Lite) без изменения сервиса
// - Запускать сервер без ML (stub-предиктор для разработки)
type Predictor interface {
	// Predict принимает вектор фич и возвращает anomaly score (0..1).
	Predict(features []float64) (float64, error)
	// Close освобождает ресурсы (ONNX Runtime сессия, GPU-память).
	Close()
}

// AnomalyResult — результат анализа одного замера.
//
// Используется для:
// 1. Логирования (structured logging с полями machine_id, score)
// 2. Записи в events таблицу (InferenceCompleted)
// 3. Алертинга (в будущей фазе 6)
type AnomalyResult struct {
	MachineID string    // станок, с которого пришёл замер
	Score     float64   // anomaly score (0..1): чем выше, тем вероятнее аномалия
	IsAnomaly bool      // score > threshold — аномалия обнаружена
	Timestamp time.Time // время замера (для Event Sourcing)
}

// Service — сервис ML-инференса.
//
// Архитектурно находится рядом с TelemetryService:
// TelemetryService записывает данные → InferenceService анализирует.
// Связь через вызов в DBService (composition), а не через наследование.
type Service struct {
	window    *SlidingWindow
	predictor Predictor
	// threshold — порог для классификации аномалий.
	// Подбирается эмпирически. Типичные значения:
	// 0.5 — чувствительный (больше ложных, но не пропускает реальные)
	// 0.7 — сбалансированный
	// 0.9 — строгий (мало ложных, но может пропустить начало деградации)
	threshold float64
	logger    *slog.Logger
}

// NewService создаёт сервис инференса.
//
// windowSize — количество замеров в скользящем окне (обычно 10).
// threshold — порог anomaly score для классификации как аномалия.
func NewService(predictor Predictor, windowSize int, threshold float64, logger *slog.Logger) *Service {
	return &Service{
		window:    NewSlidingWindow(windowSize),
		predictor: predictor,
		threshold: threshold,
		logger:    logger,
	}
}

// Analyze анализирует батч записей и возвращает результаты инференса.
//
// Для каждой записи:
// 1. Push в скользящее окно → получаем вектор фич (или nil если окно не заполнено)
// 2. Если вектор есть → вызываем Predict → получаем anomaly score
// 3. Сравниваем с threshold → определяем аномалию
//
// ПОЧЕМУ возвращаем []AnomalyResult, а не []error?
// Результат инференса — это не ошибка. Аномалия — это полезная информация,
// которую нужно залогировать и сохранить в events.
// Ошибки инференса (ONNX Runtime упал) логируются, но не прерывают обработку —
// данные уже записаны в БД, ML — это дополнение, а не критический путь.
func (s *Service) Analyze(ctx context.Context, records []entity.Record) []AnomalyResult {
	var results []AnomalyResult

	for _, rec := range records {
		// Проверяем отмену контекста (graceful shutdown)
		if ctx.Err() != nil {
			s.logger.Warn("инференс прерван: контекст отменён",
				"processed", len(results),
				"total", len(records),
			)
			break
		}

		// Push в окно — получаем фичи только когда окно заполнено
		features := s.window.Push(rec)
		if features == nil {
			// Окно ещё не заполнено для этого станка — пропускаем.
			// Это нормально при старте: первые windowSize-1 замеров
			// нужны для "разогрева" окна.
			continue
		}

		// Инференс модели
		score, err := s.predictor.Predict(features)
		if err != nil {
			// Ошибка ML не должна ломать основной pipeline.
			// Данные уже в БД — ML это бонус, а не обязательство.
			s.logger.Error("ошибка инференса",
				"machine_id", rec.MachineID,
				"error", err,
			)
			continue
		}

		isAnomaly := score > s.threshold

		result := AnomalyResult{
			MachineID: rec.MachineID,
			Score:     score,
			IsAnomaly: isAnomaly,
			Timestamp: rec.Timestamp,
		}
		results = append(results, result)

		// Логируем каждый результат.
		// На уровне Info — чтобы видеть в structured logs и Grafana.
		// IsAnomaly и Score — ключевые поля для фильтрации и дашбордов.
		logLevel := slog.LevelInfo
		if isAnomaly {
			// Аномалии логируем на уровне Warn — легче найти в потоке логов
			logLevel = slog.LevelWarn
		}
		s.logger.Log(ctx, logLevel, "inference result",
			"machine_id", rec.MachineID,
			"score", fmt.Sprintf("%.4f", score),
			"is_anomaly", isAnomaly,
			"threshold", s.threshold,
		)
	}

	return results
}

// Close освобождает ресурсы предиктора.
func (s *Service) Close() {
	if s.predictor != nil {
		s.predictor.Close()
	}
}

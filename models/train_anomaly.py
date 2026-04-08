"""
Обучение модели обнаружения аномалий для IoT-телеметрии.

Модель: Isolation Forest (sklearn)
Вход: скользящее окно из 10 замеров × 3 параметра (temperature, vibration, pressure) = 30 фич
Выход: anomaly score (чем ближе к 1 — тем вероятнее аномалия)

Isolation Forest хорошо подходит для обнаружения аномалий в промышленной телеметрии:
- Не требует размеченных данных (unsupervised) — на реальном производстве аномалии редки
- Быстрый инференс — O(log n), подходит для real-time
- Хорошо работает с многомерными данными (наш случай: 30 фич)

Экспорт в ONNX позволяет запускать инференс на Go без Python-зависимостей.
"""

import numpy as np
from sklearn.ensemble import IsolationForest
from skl2onnx import to_onnx
import onnxruntime as ort
import os

# ============================================================
# Параметры
# ============================================================
WINDOW_SIZE = 10          # количество замеров в скользящем окне
N_FEATURES = 3            # temperature, vibration, pressure
INPUT_SIZE = WINDOW_SIZE * N_FEATURES  # 30 фич на входе модели

N_NORMAL = 5000           # количество нормальных примеров для обучения
N_ANOMALY = 200           # количество аномальных примеров для проверки
RANDOM_SEED = 42
CONTAMINATION = 0.05      # доля аномалий, которую модель ожидает в данных

# Диапазоны нормальных значений для каждого станка (типичные для CNC/LATHE/MILL):
# temperature: 40-80°C (рабочая температура шпинделя/резца)
# vibration:   0.5-3.0 mm/s (нормальная вибрация)
# pressure:    4.0-7.0 bar (давление СОЖ/пневматики)
NORMAL_RANGES = {
    "temperature": (40.0, 80.0),
    "vibration":   (0.5, 3.0),
    "pressure":    (4.0, 7.0),
}

# Аномальные значения — выходят за нормальный диапазон:
# Перегрев шпинделя, повышенная вибрация (износ подшипника), падение давления
ANOMALY_RANGES = {
    "temperature": (95.0, 130.0),   # перегрев
    "vibration":   (5.0, 12.0),     # сильная вибрация
    "pressure":    (1.0, 3.0),      # утечка давления
}


def generate_window(ranges: dict, rng: np.random.Generator) -> np.ndarray:
    """
    Генерирует одно скользящее окно телеметрии (10 замеров × 3 параметра).

    Каждый замер — это [temperature, vibration, pressure].
    Окно flatten'ится в вектор длиной 30:
    [t1_temp, t1_vib, t1_pres, t2_temp, t2_vib, t2_pres, ..., t10_temp, t10_vib, t10_pres]

    Добавляем небольшой тренд внутри окна (±5% от диапазона) —
    это имитирует реальное поведение станка, где параметры плавно меняются.
    """
    window = np.zeros((WINDOW_SIZE, N_FEATURES))

    for i, (param, (lo, hi)) in enumerate(ranges.items()):
        # Базовое значение — случайная точка в диапазоне
        base = rng.uniform(lo, hi)
        # Тренд — небольшое смещение от замера к замеру
        trend = rng.uniform(-0.05, 0.05) * (hi - lo)
        # Шум — имитация реальных датчиков
        noise = rng.normal(0, (hi - lo) * 0.02, size=WINDOW_SIZE)

        window[:, i] = base + np.arange(WINDOW_SIZE) * trend + noise

    return window.flatten()


def main():
    rng = np.random.default_rng(RANDOM_SEED)

    # ============================================================
    # 1. Генерация обучающих данных (только нормальные)
    # ============================================================
    # Isolation Forest обучается на "нормальных" данных.
    # contamination=0.05 говорит модели, что ~5% данных могут быть аномальными —
    # это делает границу решения более жёсткой.
    print(f"Генерация {N_NORMAL} нормальных примеров для обучения...")
    X_train = np.array([generate_window(NORMAL_RANGES, rng) for _ in range(N_NORMAL)])
    print(f"  Форма: {X_train.shape}")  # (5000, 30)

    # ============================================================
    # 2. Обучение Isolation Forest
    # ============================================================
    # n_estimators=200 — количество деревьев (больше = точнее, но медленнее)
    # max_samples=256 — подвыборка для каждого дерева (стандарт для IF)
    # contamination — ожидаемая доля аномалий
    print("Обучение Isolation Forest...")
    model = IsolationForest(
        n_estimators=200,
        max_samples=256,
        contamination=CONTAMINATION,
        random_state=RANDOM_SEED,
    )
    model.fit(X_train)
    print("  Модель обучена.")

    # ============================================================
    # 3. Проверка на тестовых данных
    # ============================================================
    print(f"\nПроверка: {N_NORMAL // 5} нормальных + {N_ANOMALY} аномальных...")
    X_test_normal = np.array([generate_window(NORMAL_RANGES, rng) for _ in range(N_NORMAL // 5)])
    X_test_anomaly = np.array([generate_window(ANOMALY_RANGES, rng) for _ in range(N_ANOMALY)])

    # decision_function: чем меньше значение — тем вероятнее аномалия
    # predict: 1 = нормально, -1 = аномалия
    scores_normal = model.decision_function(X_test_normal)
    scores_anomaly = model.decision_function(X_test_anomaly)

    pred_normal = model.predict(X_test_normal)
    pred_anomaly = model.predict(X_test_anomaly)

    tp = np.sum(pred_anomaly == -1)  # аномалии, верно определённые как аномалии
    fn = np.sum(pred_anomaly == 1)   # аномалии, пропущенные моделью
    fp = np.sum(pred_normal == -1)   # нормальные, ложно помеченные как аномалии
    tn = np.sum(pred_normal == 1)    # нормальные, верно определённые как нормальные

    print(f"  Нормальные:  avg_score={scores_normal.mean():.3f}, "
          f"ложные срабатывания={fp}/{len(X_test_normal)} ({fp/len(X_test_normal)*100:.1f}%)")
    print(f"  Аномальные:  avg_score={scores_anomaly.mean():.3f}, "
          f"обнаружено={tp}/{len(X_test_anomaly)} ({tp/len(X_test_anomaly)*100:.1f}%)")

    # ============================================================
    # 4. Экспорт в ONNX
    # ============================================================
    # to_onnx конвертирует sklearn-модель в ONNX-формат.
    # ONNX (Open Neural Network Exchange) — открытый формат для ML-моделей,
    # поддерживаемый ONNX Runtime на любом языке (Go, C++, Java, C#).
    print("\nЭкспорт в ONNX...")

    # initial_types описывает входной тензор: имя и форму (batch, 30 фич)
    onnx_model = to_onnx(
        model,
        X_train[:1].astype(np.float32),  # пример входа для определения формы
        target_opset=15,                  # версия ONNX операторов
    )

    output_path = os.path.join(os.path.dirname(__file__), "anomaly_detector.onnx")
    with open(output_path, "wb") as f:
        f.write(onnx_model.SerializeToString())

    file_size = os.path.getsize(output_path)
    print(f"  Сохранено: {output_path} ({file_size / 1024:.1f} KB)")

    # ============================================================
    # 5. Верификация ONNX-модели через onnxruntime
    # ============================================================
    # Проверяем, что ONNX-модель даёт те же результаты, что и sklearn.
    # Это важно — конвертация может потерять точность.
    print("\nВерификация ONNX-модели...")
    sess = ort.InferenceSession(output_path)

    # Смотрим входы и выходы модели — пригодится для Go-обёртки
    print(f"  Входы:  {[(inp.name, inp.shape, inp.type) for inp in sess.get_inputs()]}")
    print(f"  Выходы: {[(out.name, out.shape, out.type) for out in sess.get_outputs()]}")

    # Прогоняем те же тестовые данные через ONNX Runtime
    onnx_input = {sess.get_inputs()[0].name: X_test_anomaly[:5].astype(np.float32)}
    onnx_labels, onnx_scores = sess.run(None, onnx_input)
    sklearn_scores = model.decision_function(X_test_anomaly[:5])

    print(f"\n  Сравнение sklearn vs ONNX (5 аномальных примеров):")
    print(f"  {'sklearn score':>15} {'ONNX score':>15} {'ONNX label':>12}")
    for i in range(5):
        # ONNX Isolation Forest возвращает:
        # - label: 1 (нормально) или -1 (аномалия)
        # - scores: dict с ключами -1 и 1 (вероятности классов)
        onnx_score = onnx_scores[i][1]  # score для класса "аномалия" (label=1, т.е. нормальный)
        print(f"  {sklearn_scores[i]:>15.4f} {onnx_score:>15.4f} {onnx_labels[i]:>12}")

    print("\n✅ Модель обучена и экспортирована в ONNX!")
    print(f"   Файл: {output_path}")
    print(f"   Вход: float32[batch_size, {INPUT_SIZE}]")
    print(f"   Выход: label (1/-1) + scores (вероятности)")


if __name__ == "__main__":
    main()

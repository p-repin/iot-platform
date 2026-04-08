// Package config — централизованная конфигурация приложения.
//
// ПОЧЕМУ отдельный пакет для конфигурации?
// 1. Единая точка входа: все параметры в одном месте, а не разбросаны по коду
// 2. Валидация при старте: если конфиг невалиден, приложение упадёт СРАЗУ,
//    а не через час когда попытается подключиться к несуществующей БД
// 3. Тестируемость: можно подставить тестовый конфиг без изменения кода сервисов
//
// ПОЧЕМУ переменные окружения, а не YAML/TOML файл?
// - В контейнерах (Docker/K8s) переменные окружения — стандарт (12-factor app)
// - Секреты (пароли БД) безопаснее передавать через env, а не хранить в файле
// - В air-gap контуре конфиг может отличаться от dev — env позволяет менять
//   без пересборки бинарника
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config — корневая структура конфигурации всего приложения.
// Каждый вложенный struct = один инфраструктурный компонент.
type Config struct {
	DB            DatabaseConfig
	Redis         RedisConfig
	NATS          NATSConfig
	Agent         AgentConfig
	Server        ServerConfig
	Inference     InferenceConfig
	PartSimulator PartSimulatorConfig
}

// PartSimulatorConfig — настройки симулятора жизненного цикла деталей.
//
// Симулятор генерирует синтетические события Event Sourcing:
// PartCreated → PartMachined → InferenceCompleted → PartShipped.
// Полезен для демонстрации и отладки Event Sourcing.
// В проде выключается: PART_SIMULATOR_ENABLED=false.
type PartSimulatorConfig struct {
	// Enabled — запускать ли симулятор.
	// По умолчанию true для dev-окружения.
	Enabled bool
	// Interval — интервал между генерацией деталей.
	// 10 секунд — достаточно для наблюдения в логах.
	Interval time.Duration
}

// DatabaseConfig — подключение к TimescaleDB.
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// DSN возвращает строку подключения в формате PostgreSQL.
// ПОЧЕМУ метод, а не просто поле?
// - DSN собирается из нескольких полей — логика инкапсулирована
// - Если формат изменится (например, добавим sslmode), менять в одном месте
func (c DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.User, c.Password, c.Host, c.Port, c.DBName,
	)
}

// RedisConfig — подключение к Redis.
type RedisConfig struct {
	Addr     string // host:port
	Password string
	DB       int
}

// NATSConfig — подключение к NATS.
type NATSConfig struct {
	URL string // nats://host:port
}

// AgentConfig — настройки агента сбора телеметрии.
type AgentConfig struct {
	// CollectInterval — как часто агент снимает показания со станков.
	// ПОЧЕМУ 1 секунда по умолчанию?
	// - Слишком редко (10с): можем пропустить кратковременный скачок вибрации
	// - Слишком часто (100мс): огромный объём данных, нагрузка на сеть и БД
	// - 1с — разумный баланс для промышленного оборудования
	CollectInterval time.Duration

	// WALDir — директория для Write-Ahead Log.
	// ПОЧЕМУ отдельная директория?
	// WAL файлы могут быть большими при длительном offline.
	// Выделенная директория позволяет мониторить использование диска
	// и настроить отдельные права доступа.
	WALDir string

	// MachineIDs — список станков, с которых собираем телеметрию.
	// В реальном проекте это приходит из конфига или service discovery.
	MachineIDs []string
}

// ServerConfig — настройки серверной части (gRPC + HTTP).
type ServerConfig struct {
	GRPCPort int
	HTTPPort int
}

// InferenceConfig — настройки ML-инференса.
//
// ПОЧЕМУ конфигурируемый threshold?
// Порог аномалии зависит от условий эксплуатации:
// - Критичное производство (авиация): threshold=0.5 (лучше ложный алерт, чем пропуск)
// - Массовое производство: threshold=0.8 (меньше ложных остановок)
// Конфиг через env позволяет менять порог без пересборки.
type InferenceConfig struct {
	// ModelPath — путь к .onnx файлу модели.
	// Если файл не найден — сервер работает без ML (graceful degradation).
	ModelPath string
	// SharedLibPath — путь к onnxruntime.dll (Windows) / libonnxruntime.so (Linux).
	// Скачивается с github.com/microsoft/onnxruntime/releases.
	SharedLibPath string
	// WindowSize — размер скользящего окна (количество замеров).
	// Должен совпадать с window_size при обучении модели.
	WindowSize int
	// Threshold — порог anomaly score для классификации как аномалия (0..1).
	Threshold float64
}

// Load читает конфигурацию из переменных окружения с дефолтными значениями.
//
// ПОЧЕМУ не используем библиотеку (viper, envconfig)?
// - Для нашего масштаба stdlib достаточно
// - Меньше зависимостей = проще аудит в air-gap контуре
// - На собеседовании ты сможешь объяснить каждую строку, а не "viper магически парсит"
//
// В продакшене можно добавить viper/envconfig когда конфиг станет сложнее.
func Load() *Config {
	return &Config{
		DB: DatabaseConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnvInt("DB_PORT", 5433),
			User:     getEnv("DB_USER", "pzmash"),
			Password: getEnv("DB_PASSWORD", "pzmash_secret"),
			DBName:   getEnv("DB_NAME", "pzmash_iot"),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvInt("REDIS_DB", 0),
		},
		NATS: NATSConfig{
			URL: getEnv("NATS_URL", "nats://localhost:4222"),
		},
		Agent: AgentConfig{
			CollectInterval: getEnvDuration("AGENT_COLLECT_INTERVAL", time.Second),
			WALDir:          getEnv("AGENT_WAL_DIR", "./data/wal"),
			MachineIDs: []string{
				"CNC-01", "CNC-02", "CNC-03",
				"LATHE-01", "LATHE-02",
				"MILL-01",
			},
		},
		Server: ServerConfig{
			GRPCPort: getEnvInt("GRPC_PORT", 50051),
			HTTPPort: getEnvInt("HTTP_PORT", 8080),
		},
		Inference: InferenceConfig{
			ModelPath:     getEnv("INFERENCE_MODEL_PATH", "./models/anomaly_detector.onnx"),
			SharedLibPath: getEnv("INFERENCE_SHARED_LIB", "./onnxruntime.dll"),
			WindowSize:    getEnvInt("INFERENCE_WINDOW_SIZE", 10),
			Threshold:     getEnvFloat("INFERENCE_THRESHOLD", 0.7),
		},
		PartSimulator: PartSimulatorConfig{
			Enabled:  getEnvBool("PART_SIMULATOR_ENABLED", true),
			Interval: getEnvDuration("PART_SIMULATOR_INTERVAL", 10*time.Second),
		},
	}
}

// --- Вспомогательные функции ---
// ПОЧЕМУ не экспортированы (строчная буква)?
// Это внутренние хелперы пакета config. Другим пакетам незачем знать
// о деталях парсинга env-переменных — им нужен только Config struct.

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	// Если значение невалидно — возвращаем дефолт.
	// АЛЬТЕРНАТИВА: можно было бы возвращать error и падать при старте.
	// Для конфигурации "падать при старте" даже лучше (fail fast),
	// но для учебного проекта — дефолт удобнее при разработке.
	n, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return n
}

func getEnvFloat(key string, defaultVal float64) float64 {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return defaultVal
	}
	return f
}

func getEnvBool(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return defaultVal
	}
	return b
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return defaultVal
	}
	return d
}
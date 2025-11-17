package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTP       HTTPConfig
	DB         DBConfig
	Redis      RedisConfig
	Etcd       EtcdConfig
	StreamName string
	GroupName  string
}

type HTTPConfig struct {
	Addr string
}

type DBConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	URL          string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type EtcdConfig struct {
	Endpoints   []string
	DialTimeout time.Duration
	SessionTTL  int
}

func Load(ctx context.Context) (*Config, error) {
	slog.InfoContext(ctx, "Loading configuration...")

	var missing []string
	dbDSN, ok := os.LookupEnv("POSTGRES_DSN")
	if !ok {
		missing = append(missing, "POSTGRES_DSN")
	}

	redisURL, ok := os.LookupEnv("REDIS_URL")
	if !ok {
		missing = append(missing, "REDIS_URL")
	}

	etcdEndpointsStr, ok := os.LookupEnv("ETCD_ENDPOINTS")
	if !ok {
		etcdEndpointsStr, ok = os.LookupEnv("ETCD_ENDPOINT")
		if !ok {
			missing = append(missing, "ETCD_ENDPOINTS (or ETCD_ENDPOINT)")
		}
	}

	if len(missing) > 0 {
		err := fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
		slog.ErrorContext(ctx, "Configuration error", "error", err)
		return nil, err
	}

	cfg := &Config{
		HTTP: HTTPConfig{
			Addr: getEnv("HTTP_ADDR", ":8080"),
		},
		DB: DBConfig{
			DSN:             dbDSN,
			MaxOpenConns:    getEnvAsInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getEnvAsInt("DB_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime: time.Duration(getEnvAsDuration("DB_CONN_MAX_LIFETIME_MINS", 60)) * time.Minute,
		},
		Redis: RedisConfig{
			URL:          redisURL,
			DialTimeout:  time.Duration(getEnvAsDuration("REDIS_DIAL_TIMEOUT_SECS", 5)) * time.Second,
			ReadTimeout:  time.Duration(getEnvAsDuration("REDIS_READ_TIMEOUT_SECS", 3)) * time.Second,
			WriteTimeout: time.Duration(getEnvAsDuration("REDIS_WRITE_TIMEOUT_SECS", 3)) * time.Second,
		},
		Etcd: EtcdConfig{
			Endpoints:   strings.Split(etcdEndpointsStr, ","),
			DialTimeout: time.Duration(getEnvAsDuration("ETCD_DIAL_TIMEOUT_SECS", 5)) * time.Second,
			SessionTTL:  getEnvAsInt("ETCD_SESSION_TTL_SECS", 10),
		},
		StreamName: getEnv("REDIS_STREAM_NAME", "jobs"),
		GroupName:  getEnv("REDIS_GROUP_NAME", "workers"),
	}

	slog.InfoContext(ctx, "Configuration loaded successfully",
		slog.Group("http", "addr", cfg.HTTP.Addr),
		slog.Group("db", "max_open", cfg.DB.MaxOpenConns, "max_idle", cfg.DB.MaxIdleConns),
		slog.Group("redis", "url", cfg.Redis.URL),
		slog.Group("etcd", "endpoints", cfg.Etcd.Endpoints, "ttl_secs", cfg.Etcd.SessionTTL),
		slog.Group("app", "stream", cfg.StreamName, "group", cfg.GroupName),
	)

	return cfg, nil
}

func LoadWorkerID(ctx context.Context) (string, error) {
	workerID, ok := os.LookupEnv("WORKER_ID")
	if !ok || workerID == "" {
		err := errors.New("WORKER_ID environment variable is required and cannot be empty")
		slog.ErrorContext(ctx, "Configuration error", "error", err)
		return "", err
	}
	slog.InfoContext(ctx, "Loaded worker configuration", "worker_id", workerID)
	return workerID, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	slog.Debug("Env var not set, using default", "key", key, "default", fallback)
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	slog.Debug("Env var not valid int, using default", "key", key, "default", fallback)
	return fallback
}

func getEnvAsDuration(key string, fallback int64) int64 {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
		return value
	}
	slog.Debug("Env var not valid duration unit, using default", "key", key, "default", fallback)
	return fallback
}

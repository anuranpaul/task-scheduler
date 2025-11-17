package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type contextKey int

const loggerKey contextKey = 0

func Init() {
	level := os.Getenv("LOG_LEVEL")
	format := os.Getenv("LOG_FORMAT")

	var lvl slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level:     lvl,
		AddSource: true,
	}

	if strings.ToUpper(format) == "JSON" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func fromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

func With(ctx context.Context, args ...any) context.Context {
	l := fromContext(ctx).With(args...)
	return context.WithValue(ctx, loggerKey, l)
}

func Info(ctx context.Context, msg string, args ...any) {
	fromContext(ctx).Info(msg, args...)
}

func Debug(ctx context.Context, msg string, args ...any) {
	fromContext(ctx).Debug(msg, args...)
}

func Warn(ctx context.Context, msg string, args ...any) {
	fromContext(ctx).Warn(msg, args...)
}

func Error(ctx context.Context, msg string, err error, args ...any) {
	allArgs := append([]any{slog.String("error", err.Error())}, args...)
	fromContext(ctx).Error(msg, allArgs...)
}
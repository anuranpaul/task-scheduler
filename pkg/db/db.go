package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/anuranpaul/task-scheduler/pkg/config"
	"github.com/anuranpaul/task-scheduler/pkg/logger"
	_ "github.com/lib/pq"
)

type DB struct {
	*sql.DB
}

func New(ctx context.Context, cfg config.DBConfig) (*DB, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	logger.Info(ctx, "Database connection pooling configured",
		"max_open", cfg.MaxOpenConns,
		"max_idle", cfg.MaxIdleConns,
		"max_lifetime", cfg.ConnMaxLifetime,
	)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info(ctx, "Successfully connected to PostgreSQL")

	return &DB{DB: db}, nil
}

func (d *DB) InitSchema(ctx context.Context) error {
	// Schema creation/migrations are handled by `init.sql` (or an external migration
	// process). Keep this method as a no-op for compatibility so callers can still
	// invoke `InitSchema` without creating DDL here.
	logger.Info(ctx, "InitSchema skipped: schema is managed via init.sql/migrations")
	return nil
}

func (d *DB) Close(ctx context.Context) error {
	logger.Info(ctx, "Closing database connection...")
	if err := d.DB.Close(); err != nil {
		logger.Error(ctx, "Database close error", err)
		return err
	}
	logger.Info(ctx, "Database connection closed")
	return nil
}
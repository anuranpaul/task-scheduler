package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anuranpaul/task-scheduler/pkg/config"
	"github.com/anuranpaul/task-scheduler/pkg/logger"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	id       string
	db       *sql.DB
	rdb      *redis.Client
	stream   string
	group    string
	shutdown chan struct{}
}

func NewWorker(cfg *config.Config, id string) (*Worker, error) {
	db, err := sql.Open("postgres", cfg.DB.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres connection failed: %w", err)
	}

	// Test connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	db.SetMaxOpenConns(cfg.DB.MaxOpenConns)
	db.SetMaxIdleConns(cfg.DB.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.DB.ConnMaxLifetime)

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.URL,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		db.Close()
		rdb.Close()
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &Worker{
		id:       id,
		db:       db,
		rdb:      rdb,
		stream:   cfg.StreamName,
		group:    cfg.GroupName,
		shutdown: make(chan struct{}),
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, w.stream, w.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx = logger.With(ctx, "worker", w.id)
	logger.Info(ctx, "worker started")

	go w.recoverPendingJobs(ctx)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "shutting down gracefully")
			return nil
		case <-w.shutdown:
			return nil
		case <-ticker.C:
			if err := w.processNextJob(ctx); err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				logger.Error(ctx, "error processing job", err)
			}
		}
	}
}

func (w *Worker) processNextJob(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    w.group,
		Consumer: w.id,
		Streams:  []string{w.stream, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}).Result()

	if err != nil {
		return err
	}

	for _, s := range streams {
		for _, msg := range s.Messages {
			if err := w.processMessage(ctx, msg); err != nil {
				logger.Error(ctx, "failed to process message", err, "message_id", msg.ID)
				return err
			}

			if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID).Err(); err != nil {
				logger.Error(ctx, "failed to ack message", err, "message_id", msg.ID)
				return err
			}
		}
	}

	return nil
}

func (w *Worker) processMessage(ctx context.Context, msg redis.XMessage) error {
	jobID, ok := msg.Values["job_id"].(string)
	if !ok {
		return fmt.Errorf("invalid job_id in message")
	}
	payload := msg.Values["payload"]

	ctx = logger.With(ctx, "job_id", jobID)
	logger.Info(ctx, "processing job", "payload", payload)

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error(ctx, "failed to begin transaction", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		"UPDATE jobs SET status='running', worker_id=$1, started_at=NOW() WHERE id=$2 AND status='pending'",
		w.id, jobID)
	if err != nil {
		logger.Error(ctx, "failed to update job status", err)
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		logger.Error(ctx, "failed to commit status update", err)
		return fmt.Errorf("failed to commit status update: %w", err)
	}

	if err := w.doWork(ctx, payload); err != nil {
		if _, dbErr := w.db.ExecContext(ctx,
			"UPDATE jobs SET status='failed', error=$1, completed_at=NOW() WHERE id=$2",
			err.Error(), jobID); dbErr != nil {
			logger.Error(ctx, "failed to mark job as failed", dbErr)
		}
		logger.Error(ctx, "job processing failed", err)
		return fmt.Errorf("job processing failed: %w", err)
	}

	_, err = w.db.ExecContext(ctx,
		"UPDATE jobs SET status='completed', completed_at=NOW() WHERE id=$1",
		jobID)
	if err != nil {
		logger.Error(ctx, "failed to mark job as completed", err)
		return fmt.Errorf("failed to mark job as completed: %w", err)
	}

	logger.Info(ctx, "completed job")
	return nil
}

func (w *Worker) doWork(ctx context.Context, payload interface{}) error {
	_ = payload
	select {
	case <-time.After(2 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) recoverPendingJobs(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.claimStalledJobs(ctx)
		}
	}
}

func (w *Worker) claimStalledJobs(ctx context.Context) {
	pending, err := w.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: w.stream,
		Group:  w.group,
		Start:  "-",
		End:    "+",
		Count:  10,
		Idle:   5 * time.Minute,
	}).Result()

	if err != nil {
		logger.Error(ctx, "failed to get pending jobs", err)
		return
	}

	for _, p := range pending {
		claimed, err := w.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   w.stream,
			Group:    w.group,
			Consumer: w.id,
			MinIdle:  5 * time.Minute,
			Messages: []string{p.ID},
		}).Result()

		if err != nil {
			logger.Error(ctx, "failed to claim message", err, "message_id", p.ID)
			continue
		}

		for _, msg := range claimed {
			logger.Info(ctx, "claimed stalled job", "message_id", msg.ID)
			if err := w.processMessage(ctx, msg); err != nil {
				logger.Error(ctx, "failed to process claimed job", err, "message_id", msg.ID)
			} else {
				w.rdb.XAck(ctx, w.stream, w.group, msg.ID)
			}
		}
	}
}

func (w *Worker) Close() error {
	close(w.shutdown)

	if err := w.db.Close(); err != nil {
		logger.Error(context.Background(), "error closing database", err)
	}

	if err := w.rdb.Close(); err != nil {
		logger.Error(context.Background(), "error closing redis", err)
	}

	return nil
}

func main() {
	logger.Init()
	cfg, err := config.Load(context.Background())
	if err != nil {
		logger.Error(context.Background(), "failed to load config", err)
		os.Exit(1)
	}

	workerID, err := config.LoadWorkerID(context.Background())
	if err != nil {
		logger.Error(context.Background(), "failed to load worker id", err)
		os.Exit(1)
	}

	worker, err := NewWorker(cfg, workerID)
	if err != nil {
		logger.Error(context.Background(), "failed to create worker", err)
		os.Exit(1)
	}
	defer worker.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info(context.Background(), "received shutdown signal")
		cancel()
	}()

	if err := worker.Start(ctx); err != nil {
		logger.Error(context.Background(), "worker error", err)
		os.Exit(1)
	}

	logger.Info(context.Background(), "worker shut down successfully")
}
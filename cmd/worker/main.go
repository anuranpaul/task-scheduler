package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/anuranpaul/task-scheduler/pkg/config"
	"github.com/anuranpaul/task-scheduler/pkg/coordination"
	"github.com/anuranpaul/task-scheduler/pkg/logger"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	id           string
	db           *sql.DB
	rdb          *redis.Client
	coord        *coordinator.Coordinator
	serviceReg   *coordinator.ServiceRegistry
	stream       string
	group        string
	shutdownCh     chan struct{}
	httpServer   *http.Server
	httpAddr     string
	
	// Metrics
	jobsProcessed   int64
	jobsSucceeded   int64
	jobsFailed      int64
	lastJobTime     time.Time
	metricsMu       sync.RWMutex
}

func NewWorker(cfg *config.Config, id string) (*Worker, error) {
	db, err := sql.Open("postgres", cfg.DB.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres connection failed: %w", err)
	}

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

	// Initialize coordination layer
	coord, err := coordinator.NewCoordinator(ctx, coordinator.CoordinatorConfig{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: cfg.Etcd.DialTimeout,
		InstanceID:  id,
		Namespace:   "/task-scheduler",
	})
	if err != nil {
		db.Close()
		rdb.Close()
		return nil, fmt.Errorf("coordinator initialization failed: %w", err)
	}

	httpAddr := os.Getenv("WORKER_HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":9090"
	}

	return &Worker{
		id:       id,
		db:       db,
		rdb:      rdb,
		coord:    coord,
		stream:   cfg.StreamName,
		group:    cfg.GroupName,
		httpAddr: httpAddr,
		shutdownCh: make(chan struct{}),
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, w.stream, w.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx = logger.With(ctx, "worker", w.id)
	
	// Register worker in service discovery
	metadata := map[string]string{
		"type":       "worker",
		"version":    "1.0.0",
		"http_addr":  w.httpAddr,
		"started_at": time.Now().Format(time.RFC3339),
	}
	
	serviceReg, err := w.coord.NewServiceRegistry(ctx, "worker", metadata, 10)
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}
	w.serviceReg = serviceReg

	// Setup HTTP server for health checks and metrics
	w.setupHTTPServer()
	go func() {
		logger.Info(ctx, "starting worker http server", "addr", w.httpAddr)
		if err := w.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(ctx, "http server error", err)
		}
	}()

	logger.Info(ctx, "worker started")

	// Start background tasks
	go w.recoverPendingJobs(ctx)
	go w.metricsReporter(ctx)

	// Main processing loop
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "shutting down gracefully")
			return w.shutdown(ctx)
		case <-w.shutdownCh:
			return w.shutdown(ctx)
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
				atomic.AddInt64(&w.jobsFailed, 1)
				return err
			}

			if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID).Err(); err != nil {
				logger.Error(ctx, "failed to ack message", err, "message_id", msg.ID)
				return err
			}
			
			atomic.AddInt64(&w.jobsSucceeded, 1)
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

	atomic.AddInt64(&w.jobsProcessed, 1)
	w.updateLastJobTime()

	// Use distributed lock for critical job processing
	// This prevents duplicate processing during network partitions
	lock, err := w.coord.NewDistributedLock(ctx, fmt.Sprintf("job-%s", jobID), 30)
	if err != nil {
		return fmt.Errorf("failed to create job lock: %w", err)
	}
	defer lock.Close()

	acquired, err := lock.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire job lock: %w", err)
	}
	if !acquired {
		logger.Info(ctx, "job already being processed by another worker")
		return nil // Job is being processed elsewhere
	}
	defer lock.Unlock(ctx)

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error(ctx, "failed to begin transaction", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if job is still in pending/queued state
	var currentStatus string
	err = tx.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id=$1 FOR UPDATE", jobID).Scan(&currentStatus)
	if err != nil {
		logger.Error(ctx, "failed to query job status", err)
		return fmt.Errorf("failed to query job status: %w", err)
	}

	if currentStatus != "pending" && currentStatus != "queued" {
		logger.Info(ctx, "job already processed", "status", currentStatus)
		return nil
	}

	_, err = tx.ExecContext(ctx,
		"UPDATE jobs SET status='running', worker_id=$1, started_at=NOW() WHERE id=$2",
		w.id, jobID)
	if err != nil {
		logger.Error(ctx, "failed to update job status", err)
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		logger.Error(ctx, "failed to commit status update", err)
		return fmt.Errorf("failed to commit status update: %w", err)
	}

	// Process the actual work
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
	// Simulate work with cancellation support
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
				atomic.AddInt64(&w.jobsFailed, 1)
			} else {
				w.rdb.XAck(ctx, w.stream, w.group, msg.ID)
				atomic.AddInt64(&w.jobsSucceeded, 1)
			}
		}
	}
}

func (w *Worker) metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics := w.collectMetrics()
			if err := w.reportMetrics(ctx, metrics); err != nil {
				logger.Error(ctx, "failed to report metrics", err)
			}
		}
	}
}

func (w *Worker) collectMetrics() map[string]interface{} {
	w.metricsMu.RLock()
	defer w.metricsMu.RUnlock()

	return map[string]interface{}{
		"timestamp":       time.Now().Unix(),
		"worker_id":       w.id,
		"jobs_processed":  atomic.LoadInt64(&w.jobsProcessed),
		"jobs_succeeded":  atomic.LoadInt64(&w.jobsSucceeded),
		"jobs_failed":     atomic.LoadInt64(&w.jobsFailed),
		"last_job_time":   w.lastJobTime.Unix(),
	}
}

func (w *Worker) reportMetrics(ctx context.Context, metrics map[string]interface{}) error {
	metricsJSON, err := json.Marshal(metrics)
	if err != nil {
		return err
	}
	
	key := fmt.Sprintf("metrics/worker/%s", w.id)
	return w.coord.SetKey(ctx, key, string(metricsJSON))
}

func (w *Worker) updateLastJobTime() {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.lastJobTime = time.Now()
}

func (w *Worker) setupHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", w.handleHealth)
	mux.HandleFunc("/metrics", w.handleMetrics)

	w.httpServer = &http.Server{
		Addr:         w.httpAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
}

func (w *Worker) handleHealth(wr http.ResponseWriter, r *http.Request) {
	wr.Header().Set("Content-Type", "application/json")
	
	coordHealthy := w.coord.Health(r.Context()) == nil
	
	status := map[string]interface{}{
		"status":             "healthy",
		"worker_id":          w.id,
		"coordinator_health": coordHealthy,
		"timestamp":          time.Now().Unix(),
	}

	json.NewEncoder(wr).Encode(status)
}

func (w *Worker) handleMetrics(wr http.ResponseWriter, r *http.Request) {
	wr.Header().Set("Content-Type", "application/json")
	
	metrics := w.collectMetrics()
	json.NewEncoder(wr).Encode(metrics)
}

func (w *Worker) shutdown(ctx context.Context) error {
	logger.Info(ctx, "shutting down worker", "worker_id", w.id)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Deregister from service discovery
	if w.serviceReg != nil {
		if err := w.serviceReg.Deregister(shutdownCtx); err != nil {
			logger.Error(ctx, "failed to deregister service", err)
		}
	}

	if w.httpServer != nil {
		if err := w.httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error(ctx, "http server shutdown error", err)
		}
	}

	if err := w.db.Close(); err != nil {
		logger.Error(ctx, "error closing database", err)
	}

	if err := w.rdb.Close(); err != nil {
		logger.Error(ctx, "error closing redis", err)
	}

	if err := w.coord.Close(); err != nil {
		logger.Error(ctx, "error closing coordinator", err)
	}

	logger.Info(ctx, "worker shut down successfully")
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
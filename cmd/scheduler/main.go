package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/anuranpaul/task-scheduler/pkg/config"
	"github.com/anuranpaul/task-scheduler/pkg/logger"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Scheduler struct {
	db           *sql.DB
	rdb          *redis.Client
	etcdCli      *clientv3.Client
	instanceID   string
	leader       bool
	leaderMu     sync.RWMutex
	stream       string
	shutdownChan chan struct{}
	httpServer   *http.Server
	httpAddr     string
}

type JobRequest struct {
	Payload    string `json:"payload"`
	DedupeKey  string `json:"dedupe_key,omitempty"`
	Priority   int    `json:"priority"`           
	ScheduleAt string `json:"schedule_at,omitempty"` 
}

type JobResponse struct {
	ID        int    `json:"id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

func NewScheduler(cfg *config.Config) (*Scheduler, error) {
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

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: cfg.Etcd.DialTimeout,
	})
	if err != nil {
		db.Close()
		rdb.Close()
		return nil, fmt.Errorf("etcd connection failed: %w", err)
	}

	instanceID, _ := os.Hostname()
	if instanceID == "" {
		instanceID = fmt.Sprintf("scheduler-%d", time.Now().Unix())
	}

	return &Scheduler{
		db:           db,
		rdb:          rdb,
		etcdCli:      etcdCli,
		instanceID:   instanceID,
		stream:       cfg.StreamName,
		httpAddr:     cfg.HTTP.Addr,
		shutdownChan: make(chan struct{}),
	}, nil
}

func (s *Scheduler) Start(ctx context.Context) error {
	ctx = logger.With(ctx, "instance", s.instanceID)
	if err := s.initSchema(ctx); err != nil {
		return fmt.Errorf("schema init failed: %w", err)
	}

	s.setupHTTPServer()
	go func() {
		logger.Info(ctx, "starting http server", "addr", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(ctx, "http server error", err)
			os.Exit(1)
		}
	}()

	go s.runLeaderElection(ctx)

	<-ctx.Done()
	return s.shutdown()
}

func (s *Scheduler) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id SERIAL PRIMARY KEY,
		payload TEXT NOT NULL,
		dedupe_key VARCHAR(255) UNIQUE,
		status VARCHAR(50) DEFAULT 'pending',
		priority INTEGER DEFAULT 0,
		schedule_at TIMESTAMP,
		worker_id VARCHAR(255),
		started_at TIMESTAMP,
		completed_at TIMESTAMP,
		error TEXT,
		retry_count INTEGER DEFAULT 0,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
	CREATE INDEX IF NOT EXISTS idx_jobs_schedule_at ON jobs(schedule_at) WHERE schedule_at IS NOT NULL;
	`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Scheduler) runLeaderElection(ctx context.Context) {
	localCtx := logger.With(ctx, "instance", s.instanceID)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := s.campaignForLeader(ctx); err != nil {
				logger.Error(localCtx, "leader election error, retrying in 5s", err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func (s *Scheduler) campaignForLeader(ctx context.Context) error {
	sess, err := concurrency.NewSession(s.etcdCli, concurrency.WithTTL(10))
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer sess.Close()

	election := concurrency.NewElection(sess, "/scheduler/leader")

	if err := election.Campaign(ctx, s.instanceID); err != nil {
		return fmt.Errorf("campaign failed: %w", err)
	}

	logger.Info(ctx, "became leader", "instance", s.instanceID)
	s.setLeader(true)

	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go s.runJobDispatcher(leaderCtx)
	go s.runScheduledJobChecker(leaderCtx)

	select {
	case <-sess.Done():
		logger.Info(ctx, "lost leader session")
		s.setLeader(false)
		return nil
	case <-ctx.Done():
		s.setLeader(false)
		return election.Resign(context.Background())
	}
}

func (s *Scheduler) runJobDispatcher(ctx context.Context) {
	localCtx := logger.With(ctx, "instance", s.instanceID)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	logger.Info(localCtx, "job dispatcher started")

	for {
		select {
		case <-ctx.Done():
			logger.Info(localCtx, "job dispatcher stopped")
			return
		case <-ticker.C:
			if err := s.dispatchPendingJobs(ctx); err != nil {
				logger.Error(localCtx, "error dispatching jobs", err)
			}
		}
	}
}

func (s *Scheduler) dispatchPendingJobs(ctx context.Context) error {
	// Get pending jobs that are ready to run
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, payload, priority 
		FROM jobs 
		WHERE status = 'pending' 
		AND (schedule_at IS NULL OR schedule_at <= NOW())
		ORDER BY priority DESC, created_at ASC 
		LIMIT 100
	`)
	if err != nil {
		return fmt.Errorf("failed to query pending jobs: %w", err)
	}
	defer rows.Close()

	dispatched := 0
	for rows.Next() {
		var id int
		var payload string
		var priority int

		if err := rows.Scan(&id, &payload, &priority); err != nil {
			logger.Error(ctx, "failed to scan job", err)
			continue
		}

		if err := s.dispatchJob(ctx, id, payload, priority); err != nil {
			logger.Error(ctx, "failed to dispatch job", err, "job_id", id)
			continue
		}

		dispatched++
	}

	if dispatched > 0 {
		logger.Info(ctx, "dispatched jobs", "count", dispatched)
	}

	return rows.Err()
}

func (s *Scheduler) dispatchJob(ctx context.Context, jobID int, payload string, priority int) error {
	// Push to Redis stream
	streamID, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.stream,
		Values: map[string]interface{}{
			"job_id":   fmt.Sprintf("%d", jobID),
			"payload":  payload,
			"priority": priority,
		},
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to add to stream: %w", err)
	}

	// Update job status to queued
	_, err = s.db.ExecContext(ctx,
		"UPDATE jobs SET status='queued', updated_at=NOW() WHERE id=$1",
		jobID)
	if err != nil {
		// Try to remove from stream if DB update fails
		s.rdb.XDel(ctx, s.stream, streamID)
		return fmt.Errorf("failed to update job status: %w", err)
	}

	logger.Info(ctx, "dispatched job to stream", "job_id", jobID, "priority", priority)
	return nil
}

func (s *Scheduler) runScheduledJobChecker(ctx context.Context) {
	localCtx := logger.With(ctx, "instance", s.instanceID)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	logger.Info(localCtx, "scheduled job checker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info(localCtx, "scheduled job checker stopped")
			return
		case <-ticker.C:
			count, _ := s.getScheduledJobCount(ctx)
			if count > 0 {
				logger.Info(localCtx, "found scheduled jobs", "count", count)
			}
		}
	}
}

func (s *Scheduler) getScheduledJobCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM jobs 
		WHERE status = 'pending' AND schedule_at IS NOT NULL AND schedule_at > NOW()
	`).Scan(&count)
	return count, err
}

func (s *Scheduler) setupHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/jobs", s.handleJobs)
	mux.HandleFunc("/jobs/", s.handleJobStatus)

	s.httpServer = &http.Server{
		Addr:         s.httpAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}

func (s *Scheduler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	status := map[string]interface{}{
		"status":      "healthy",
		"instance_id": s.instanceID,
		"is_leader":   s.isLeader(),
	}

	json.NewEncoder(w).Encode(status)
}

func (s *Scheduler) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handleCreateJob(w, r)
	case http.MethodGet:
		s.handleListJobs(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Scheduler) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Payload == "" {
		http.Error(w, "Payload is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	var scheduleAt *time.Time
	if req.ScheduleAt != "" {
		t, err := time.Parse(time.RFC3339, req.ScheduleAt)
		if err != nil {
			http.Error(w, "Invalid schedule_at format (use RFC3339)", http.StatusBadRequest)
			return
		}
		scheduleAt = &t
	}

	var jobID int
	var status string
	var createdAt time.Time

	query := `
		INSERT INTO jobs(payload, dedupe_key, priority, schedule_at, status) 
		VALUES($1, $2, $3, $4, 'pending') 
		ON CONFLICT (dedupe_key) DO UPDATE SET updated_at=NOW()
		RETURNING id, status, created_at
	`

	err := s.db.QueryRowContext(ctx, query, 
		req.Payload, 
		sql.NullString{String: req.DedupeKey, Valid: req.DedupeKey != ""},
		req.Priority,
		scheduleAt,
	).Scan(&jobID, &status, &createdAt)

	if err != nil {
		logger.Error(ctx, "db insert error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	response := JobResponse{
		ID:        jobID,
		Status:    status,
		CreatedAt: createdAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

	logger.Info(ctx, "created job", "job_id", jobID, "priority", req.Priority, "scheduled", scheduleAt != nil)
}

func (s *Scheduler) handleListJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	
	query := "SELECT id, payload, status, priority, created_at FROM jobs"
	args := []interface{}{}
	
	if status != "" {
		query += " WHERE status = $1"
		args = append(args, status)
	}
	
	query += " ORDER BY created_at DESC LIMIT 100"

	rows, err := s.db.QueryContext(r.Context(), query, args...)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	jobs := []JobResponse{}
	for rows.Next() {
		var job JobResponse
		var createdAt time.Time
		var priority int
		var payload string

		if err := rows.Scan(&job.ID, &payload, &job.Status, &priority, &createdAt); err != nil {
			continue
		}

		job.CreatedAt = createdAt.Format(time.RFC3339)
		jobs = append(jobs, job)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func (s *Scheduler) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var jobID int
	if _, err := fmt.Sscanf(r.URL.Path, "/jobs/%d", &jobID); err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	var status, workerID, errorMsg string
	var startedAt, completedAt sql.NullTime
	var createdAt time.Time

	err := s.db.QueryRowContext(r.Context(), `
		SELECT status, worker_id, started_at, completed_at, error, created_at
		FROM jobs WHERE id = $1
	`, jobID).Scan(&status, &workerID, &startedAt, &completedAt, &errorMsg, &createdAt)

	if err == sql.ErrNoRows {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"id":         jobID,
		"status":     status,
		"worker_id":  workerID,
		"created_at": createdAt.Format(time.RFC3339),
	}

	if startedAt.Valid {
		response["started_at"] = startedAt.Time.Format(time.RFC3339)
	}
	if completedAt.Valid {
		response["completed_at"] = completedAt.Time.Format(time.RFC3339)
	}
	if errorMsg != "" {
		response["error"] = errorMsg
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Scheduler) isLeader() bool {
	s.leaderMu.RLock()
	defer s.leaderMu.RUnlock()
	return s.leader
}

func (s *Scheduler) setLeader(leader bool) {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	s.leader = leader
}

func (s *Scheduler) shutdown() error {
	lctx := logger.With(context.Background(), "instance", s.instanceID)
	logger.Info(lctx, "shutting down scheduler")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		logger.Error(lctx, "http server shutdown error", err)
	}

	if err := s.db.Close(); err != nil {
		logger.Error(lctx, "database close error", err)
	}

	if err := s.rdb.Close(); err != nil {
		logger.Error(lctx, "redis close error", err)
	}

	if err := s.etcdCli.Close(); err != nil {
		logger.Error(lctx, "etcd close error", err)
	}

	logger.Info(lctx, "scheduler shut down successfully")
	return nil
}

func main() {
	logger.Init()
	cfg, err := config.Load(context.Background())
	if err != nil {
		logger.Error(context.Background(), "failed to load configuration", err)
		os.Exit(1)
	}

	scheduler, err := NewScheduler(cfg)
	if err != nil {
		logger.Error(context.Background(), "failed to create scheduler", err)
		os.Exit(1)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info(context.Background(), "received shutdown signal")
		cancel()
	}()

	// Start scheduler
	if err := scheduler.Start(ctx); err != nil {
		logger.Error(context.Background(), "scheduler error", err)
		os.Exit(1)
	}
}
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
	"github.com/anuranpaul/task-scheduler/pkg/coordination"
	"github.com/anuranpaul/task-scheduler/pkg/logger"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type Scheduler struct {
	db             *sql.DB
	rdb            *redis.Client
	coord          *coordinator.Coordinator
	instanceID     string
	leader         bool
	leaderMu       sync.RWMutex
	leaderElection *coordinator.LeaderElection
	serviceReg     *coordinator.ServiceRegistry
	stream         string
	shutdownChan   chan struct{}
	httpServer     *http.Server
	httpAddr       string
	
	// Metrics and state
	jobsDispatched   int64
	lastDispatchTime time.Time
	metricsMu        sync.RWMutex
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

	// Initialize coordination layer
	coord, err := coordinator.NewCoordinator(ctx, coordinator.CoordinatorConfig{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: cfg.Etcd.DialTimeout,
		InstanceID:  getInstanceID(),
		Namespace:   "/task-scheduler",
	})
	if err != nil {
		db.Close()
		rdb.Close()
		return nil, fmt.Errorf("coordinator initialization failed: %w", err)
	}

	return &Scheduler{
		db:           db,
		rdb:          rdb,
		coord:        coord,
		instanceID:   getInstanceID(),
		stream:       cfg.StreamName,
		httpAddr:     cfg.HTTP.Addr,
		shutdownChan: make(chan struct{}),
	}, nil
}

func getInstanceID() string {
	instanceID, _ := os.Hostname()
	if instanceID == "" {
		instanceID = fmt.Sprintf("scheduler-%d", time.Now().Unix())
	}
	return instanceID
}

func (s *Scheduler) Start(ctx context.Context) error {
	ctx = logger.With(ctx, "instance", s.instanceID)

	// Register service for discovery
	metadata := map[string]string{
		"type":       "scheduler",
		"version":    "1.0.0",
		"http_addr":  s.httpAddr,
		"started_at": time.Now().Format(time.RFC3339),
	}
	
	serviceReg, err := s.coord.NewServiceRegistry(ctx, "scheduler", metadata, 10)
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}
	s.serviceReg = serviceReg

	// Setup HTTP server
	s.setupHTTPServer()
	go func() {
		logger.Info(ctx, "starting http server", "addr", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(ctx, "http server error", err)
			os.Exit(1)
		}
	}()

	// Start leader election with callbacks
	go s.runLeaderElection(ctx)

	<-ctx.Done()
	return s.shutdown(ctx)
}

func (s *Scheduler) runLeaderElection(ctx context.Context) {
	localCtx := logger.With(ctx, "instance", s.instanceID)
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Create leader election with callbacks
			callbacks := coordinator.LeaderCallbacks{
				OnElected: func(electionCtx context.Context) error {
					logger.Info(electionCtx, "became leader - starting job dispatchers")
					s.setLeader(true)
					
					// Start background tasks as leader
					go s.runJobDispatcher(electionCtx)
					go s.runScheduledJobChecker(electionCtx)
					go s.runMetricsCollector(electionCtx)
					
					return nil
				},
				OnRevoked: func(revocationCtx context.Context) error {
					logger.Info(revocationCtx, "lost leadership - stopping dispatchers")
					s.setLeader(false)
					return nil
				},
				OnError: func(errorCtx context.Context, err error) {
					logger.Error(errorCtx, "leader election error", err)
				},
			}

			election, err := s.coord.NewLeaderElection(localCtx, "scheduler-leader", 10, callbacks)
			if err != nil {
				logger.Error(localCtx, "failed to create leader election", err)
				time.Sleep(5 * time.Second)
				continue
			}
			s.leaderElection = election

			// Campaign for leadership (blocking)
			if err := election.Campaign(localCtx); err != nil {
				logger.Error(localCtx, "campaign failed", err)
				election.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			// Observe leadership changes
			election.Observe(localCtx)
			
			// Clean up on exit
			election.Close()
		}
	}
}

func (s *Scheduler) runJobDispatcher(ctx context.Context) {
	localCtx := logger.With(ctx, "component", "dispatcher", "instance", s.instanceID)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	logger.Info(localCtx, "job dispatcher started")

	for {
		select {
		case <-ctx.Done():
			logger.Info(localCtx, "job dispatcher stopped")
			return
		case <-ticker.C:
			if !s.isLeader() {
				return 
			}
			
			if err := s.dispatchPendingJobs(ctx); err != nil {
				logger.Error(localCtx, "error dispatching jobs", err)
			}
		}
	}
}

func (s *Scheduler) dispatchPendingJobs(ctx context.Context) error {
	lock, err := s.coord.NewDistributedLock(ctx, "job-dispatch", 5)
	if err != nil {
		return fmt.Errorf("failed to create lock: %w", err)
	}
	defer lock.Close()

	acquired, err := lock.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("failed to try lock: %w", err)
	}
	if !acquired {
		return nil
	}
	defer lock.Unlock(ctx)
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
		s.updateMetrics(dispatched)
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

	_, err = s.db.ExecContext(ctx,
		"UPDATE jobs SET status='queued', updated_at=NOW() WHERE id=$1",
		jobID)
	if err != nil {
		s.rdb.XDel(ctx, s.stream, streamID)
		return fmt.Errorf("failed to update job status: %w", err)
	}

	logger.Info(ctx, "dispatched job to stream", "job_id", jobID, "priority", priority)
	return nil
}

func (s *Scheduler) runScheduledJobChecker(ctx context.Context) {
	localCtx := logger.With(ctx, "component", "scheduled-checker", "instance", s.instanceID)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	logger.Info(localCtx, "scheduled job checker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info(localCtx, "scheduled job checker stopped")
			return
		case <-ticker.C:
			if !s.isLeader() {
				return
			}
			
			count, _ := s.getScheduledJobCount(ctx)
			if count > 0 {
				logger.Info(localCtx, "found scheduled jobs", "count", count)
			}
		}
	}
}

func (s *Scheduler) runMetricsCollector(ctx context.Context) {
	localCtx := logger.With(ctx, "component", "metrics", "instance", s.instanceID)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	logger.Info(localCtx, "metrics collector started")

	for {
		select {
		case <-ctx.Done():
			logger.Info(localCtx, "metrics collector stopped")
			return
		case <-ticker.C:
			if !s.isLeader() {
				return
			}
			
			// Collect and store system metrics
			metrics := s.collectMetrics(ctx)
			if err := s.storeMetrics(ctx, metrics); err != nil {
				logger.Error(localCtx, "failed to store metrics", err)
			}
		}
	}
}

func (s *Scheduler) collectMetrics(ctx context.Context) map[string]interface{} {
	var pendingCount, queuedCount, runningCount, completedCount, failedCount int
	
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE status='pending'").Scan(&pendingCount)
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE status='queued'").Scan(&queuedCount)
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE status='running'").Scan(&runningCount)
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE status='completed'").Scan(&completedCount)
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE status='failed'").Scan(&failedCount)

	s.metricsMu.RLock()
	dispatched := s.jobsDispatched
	lastDispatch := s.lastDispatchTime
	s.metricsMu.RUnlock()

	return map[string]interface{}{
		"timestamp":       time.Now().Unix(),
		"pending_jobs":    pendingCount,
		"queued_jobs":     queuedCount,
		"running_jobs":    runningCount,
		"completed_jobs":  completedCount,
		"failed_jobs":     failedCount,
		"total_jobs":      pendingCount + queuedCount + runningCount + completedCount + failedCount,
		"jobs_dispatched": dispatched,
		"last_dispatch":   lastDispatch.Unix(),
	}
}

func (s *Scheduler) storeMetrics(ctx context.Context, metrics map[string]interface{}) error {
	metricsJSON, err := json.Marshal(metrics)
	if err != nil {
		return err
	}
	
	key := fmt.Sprintf("metrics/scheduler/%s", s.instanceID)
	return s.coord.SetKey(ctx, key, string(metricsJSON))
}

func (s *Scheduler) updateMetrics(dispatched int) {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()
	s.jobsDispatched += int64(dispatched)
	s.lastDispatchTime = time.Now()
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
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/leader", s.handleLeaderInfo)
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
	coordHealthy := s.coord.Health(r.Context()) == nil
	
	status := map[string]interface{}{
		"status":             "healthy",
		"instance_id":        s.instanceID,
		"is_leader":          s.isLeader(),
		"coordinator_health": coordHealthy,
		"timestamp":          time.Now().Unix(),
	}

	json.NewEncoder(w).Encode(status)
}

func (s *Scheduler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	metrics := s.collectMetrics(r.Context())
	json.NewEncoder(w).Encode(metrics)
}

func (s *Scheduler) handleLeaderInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	var leaderID string
	if s.leaderElection != nil {
		leaderID, _ = s.leaderElection.GetLeader(r.Context())
	}
	
	info := map[string]interface{}{
		"current_leader": leaderID,
		"is_leader":      s.isLeader(),
		"instance_id":    s.instanceID,
	}
	
	json.NewEncoder(w).Encode(info)
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

func (s *Scheduler) shutdown(ctx context.Context) error {
	lctx := logger.With(ctx, "instance", s.instanceID)
	logger.Info(lctx, "shutting down scheduler")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Resign from leadership gracefully
	if s.leaderElection != nil && s.isLeader() {
		if err := s.leaderElection.Resign(shutdownCtx); err != nil {
			logger.Error(lctx, "failed to resign leadership", err)
		}
		s.leaderElection.Close()
	}
	if s.serviceReg != nil {
		if err := s.serviceReg.Deregister(shutdownCtx); err != nil {
			logger.Error(lctx, "failed to deregister service", err)
		}
	}

	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error(lctx, "http server shutdown error", err)
	}

	if err := s.db.Close(); err != nil {
		logger.Error(lctx, "database close error", err)
	}

	if err := s.rdb.Close(); err != nil {
		logger.Error(lctx, "redis close error", err)
	}

	if err := s.coord.Close(); err != nil {
		logger.Error(lctx, "coordinator close error", err)
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
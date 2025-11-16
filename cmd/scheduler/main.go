package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

func NewScheduler(pgDSN, redisURL string, etcdEndpoints []string) (*Scheduler, error) {
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		return nil, fmt.Errorf("postgres connection failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisURL,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		db.Close()
		rdb.Close()
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
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
		stream:       "jobs",
		shutdownChan: make(chan struct{}),
	}, nil
}

func (s *Scheduler) Start(ctx context.Context) error {
	if err := s.initSchema(ctx); err != nil {
		return fmt.Errorf("schema init failed: %w", err)
	}

	s.setupHTTPServer()
	go func() {
		log.Printf("Starting HTTP server on %s", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
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
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := s.campaignForLeader(ctx); err != nil {
				log.Printf("Leader election error: %v, retrying in 5s", err)
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

	log.Printf("Instance %s became leader", s.instanceID)
	s.setLeader(true)

	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go s.runJobDispatcher(leaderCtx)
	go s.runScheduledJobChecker(leaderCtx)

	select {
	case <-sess.Done():
		log.Println("Lost leader session")
		s.setLeader(false)
		return nil
	case <-ctx.Done():
		s.setLeader(false)
		return election.Resign(context.Background())
	}
}

func (s *Scheduler) runJobDispatcher(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("Job dispatcher started")

	for {
		select {
		case <-ctx.Done():
			log.Println("Job dispatcher stopped")
			return
		case <-ticker.C:
			if err := s.dispatchPendingJobs(ctx); err != nil {
				log.Printf("Error dispatching jobs: %v", err)
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
			log.Printf("Failed to scan job: %v", err)
			continue
		}

		if err := s.dispatchJob(ctx, id, payload, priority); err != nil {
			log.Printf("Failed to dispatch job %d: %v", id, err)
			continue
		}

		dispatched++
	}

	if dispatched > 0 {
		log.Printf("Dispatched %d jobs", dispatched)
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

	log.Printf("Dispatched job %d to stream (priority: %d)", jobID, priority)
	return nil
}

func (s *Scheduler) runScheduledJobChecker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Println("Scheduled job checker started")

	for {
		select {
		case <-ctx.Done():
			log.Println("Scheduled job checker stopped")
			return
		case <-ticker.C:
			count, _ := s.getScheduledJobCount(ctx)
			if count > 0 {
				log.Printf("Found %d scheduled jobs", count)
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
		Addr:         ":8080",
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
		log.Printf("DB insert error: %v", err)
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

	log.Printf("Created job %d (priority: %d, scheduled: %v)", jobID, req.Priority, scheduleAt != nil)
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
	log.Println("Shutting down scheduler...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	if err := s.db.Close(); err != nil {
		log.Printf("Database close error: %v", err)
	}

	if err := s.rdb.Close(); err != nil {
		log.Printf("Redis close error: %v", err)
	}

	if err := s.etcdCli.Close(); err != nil {
		log.Printf("Etcd close error: %v", err)
	}

	log.Println("Scheduler shut down successfully")
	return nil
}

func main() {
	pgDSN := os.Getenv("POSTGRES_DSN")
	redisURL := os.Getenv("REDIS_URL")
	etcdEndpoint := os.Getenv("ETCD_ENDPOINT")

	if pgDSN == "" || redisURL == "" || etcdEndpoint == "" {
		log.Fatal("Missing required environment variables: POSTGRES_DSN, REDIS_URL, ETCD_ENDPOINT")
	}

	etcdEndpoints := []string{etcdEndpoint}

	scheduler, err := NewScheduler(pgDSN, redisURL, etcdEndpoints)
	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Start scheduler
	if err := scheduler.Start(ctx); err != nil {
		log.Fatalf("Scheduler error: %v", err)
	}
}
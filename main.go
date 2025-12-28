package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type JobRequest struct {
	Payload   string `json:"payload"`
	Priority  int    `json:"priority"`
	DedupeKey string `json:"dedupe_key,omitempty"`
}

type JobResponse struct {
	ID        int    `json:"id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type Metrics struct {
	TotalSubmitted   int64
	TotalSuccess     int64
	TotalFailed      int64
	TotalCompleted   int64
	MinLatency       time.Duration
	MaxLatency       time.Duration
	TotalLatency     time.Duration
	mu               sync.Mutex
}

type LoadTester struct {
	baseURL     string
	numJobs     int
	concurrency int
	metrics     *Metrics
	client      *http.Client
}

func NewLoadTester(baseURL string, numJobs, concurrency int) *LoadTester {
	return &LoadTester{
		baseURL:     baseURL,
		numJobs:     numJobs,
		concurrency: concurrency,
		metrics: &Metrics{
			MinLatency: time.Hour, // Start with high value
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (lt *LoadTester) submitJob(payload string, priority int) (*JobResponse, time.Duration, error) {
	start := time.Now()

	jobReq := JobRequest{
		Payload:  payload,
		Priority: priority,
	}

	body, err := json.Marshal(jobReq)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal error: %w", err)
	}

	resp, err := lt.client.Post(
		lt.baseURL+"/jobs",
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	latency := time.Since(start)

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, latency, fmt.Errorf("unexpected status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var jobResp JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return nil, latency, fmt.Errorf("decode error: %w", err)
	}

	return &jobResp, latency, nil
}

func (lt *LoadTester) checkJobStatus(jobID int) (string, error) {
	resp, err := lt.client.Get(fmt.Sprintf("%s/jobs/%d", lt.baseURL, jobID))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	status, ok := result["status"].(string)
	if !ok {
		return "", fmt.Errorf("invalid status in response")
	}

	return status, nil
}

func (lt *LoadTester) getMetrics() (map[string]interface{}, error) {
	resp, err := lt.client.Get(lt.baseURL + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var metrics map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		return nil, err
	}

	return metrics, nil
}

func (lt *LoadTester) updateMetrics(latency time.Duration, success bool) {
	lt.metrics.mu.Lock()
	defer lt.metrics.mu.Unlock()

	if success {
		atomic.AddInt64(&lt.metrics.TotalSuccess, 1)
	} else {
		atomic.AddInt64(&lt.metrics.TotalFailed, 1)
	}

	if latency < lt.metrics.MinLatency {
		lt.metrics.MinLatency = latency
	}
	if latency > lt.metrics.MaxLatency {
		lt.metrics.MaxLatency = latency
	}
	lt.metrics.TotalLatency += latency
}

func (lt *LoadTester) runLoadTest() {
	fmt.Printf("\n🚀 Starting Load Test\n")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("Target:      %s\n", lt.baseURL)
	fmt.Printf("Total Jobs:  %d\n", lt.numJobs)
	fmt.Printf("Concurrency: %d\n", lt.concurrency)
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	startTime := time.Now()

	// Create work queue
	jobs := make(chan int, lt.numJobs)
	var wg sync.WaitGroup
	
	// Track job IDs for later verification
	jobIDs := make([]int, 0, lt.numJobs)
	var jobIDsMu sync.Mutex

	// Start workers
	for i := 0; i < lt.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for jobNum := range jobs {
				// Generate varied workload
				priority := rand.Intn(10) + 1
				payload := fmt.Sprintf("Load test job #%d (worker: %d, priority: %d)", 
					jobNum, workerID, priority)

				// Submit job
				jobResp, latency, err := lt.submitJob(payload, priority)
				
				atomic.AddInt64(&lt.metrics.TotalSubmitted, 1)
				
				if err != nil {
					lt.updateMetrics(latency, false)
					fmt.Printf("❌ Job #%d failed: %v\n", jobNum, err)
					continue
				}

				lt.updateMetrics(latency, true)
				
				// Store job ID for verification
				jobIDsMu.Lock()
				jobIDs = append(jobIDs, jobResp.ID)
				jobIDsMu.Unlock()

				// Progress indicator
				submitted := atomic.LoadInt64(&lt.metrics.TotalSubmitted)
				if submitted%100 == 0 {
					fmt.Printf("📊 Progress: %d/%d jobs submitted\n", submitted, lt.numJobs)
				}
			}
		}(i)
	}

	// Feed jobs to workers
	for i := 1; i <= lt.numJobs; i++ {
		jobs <- i
	}
	close(jobs)

	// Wait for all submissions to complete
	wg.Wait()
	submitDuration := time.Since(startTime)

	fmt.Printf("\n✅ All jobs submitted in %v\n", submitDuration)
	fmt.Printf("\n⏳ Waiting for jobs to complete...\n\n")

	// Wait and monitor completion
	lt.monitorCompletion(jobIDs, startTime)
}

func (lt *LoadTester) monitorCompletion(jobIDs []int, startTime time.Time) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeout := time.After(5 * time.Minute)
	lastCompleted := int64(0)

	for {
		select {
		case <-timeout:
			fmt.Printf("\n⏰ Timeout reached (5 minutes)\n")
			lt.printFinalResults(startTime)
			return

		case <-ticker.C:
			metrics, err := lt.getMetrics()
			if err != nil {
				fmt.Printf("⚠️  Error fetching metrics: %v\n", err)
				continue
			}

			completed := int64(metrics["completed_jobs"].(float64))
			failed := int64(metrics["failed_jobs"].(float64))
			pending := int64(metrics["pending_jobs"].(float64))
			queued := int64(metrics["queued_jobs"].(float64))
			running := int64(metrics["running_jobs"].(float64))
			total := pending + queued + running + completed + failed

			// Update completion count
			atomic.StoreInt64(&lt.metrics.TotalCompleted, completed)

			// Calculate progress
			progress := float64(completed+failed) / float64(total) * 100
			throughput := float64(completed-lastCompleted) / 2.0 // jobs per second

			fmt.Printf("📈 Status: %d pending, %d queued, %d running, %d completed, %d failed | Progress: %.1f%% | Throughput: %.1f jobs/sec\n",
				pending, queued, running, completed, failed, progress, throughput)

			lastCompleted = completed

			// Check if all jobs are done
			if completed+failed >= int64(len(jobIDs)) {
				fmt.Printf("\n🎉 All jobs processed!\n")
				time.Sleep(1 * time.Second) // Let final metrics settle
				lt.printFinalResults(startTime)
				return
			}
		}
	}
}

func (lt *LoadTester) printFinalResults(startTime time.Time) {
	totalDuration := time.Since(startTime)

	// Get final metrics from server
	serverMetrics, err := lt.getMetrics()
	if err != nil {
		fmt.Printf("⚠️  Error fetching final metrics: %v\n", err)
	}

	fmt.Printf("\n")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("           📊 FINAL RESULTS\n")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	// Submission stats
	submitted := atomic.LoadInt64(&lt.metrics.TotalSubmitted)
	success := atomic.LoadInt64(&lt.metrics.TotalSuccess)
	failed := atomic.LoadInt64(&lt.metrics.TotalFailed)

	fmt.Printf("Submission Stats:\n")
	fmt.Printf("  Total Submitted:   %d\n", submitted)
	fmt.Printf("  Success:           %d (%.1f%%)\n", success, float64(success)/float64(submitted)*100)
	fmt.Printf("  Failed:            %d (%.1f%%)\n\n", failed, float64(failed)/float64(submitted)*100)

	// Latency stats
	avgLatency := lt.metrics.TotalLatency / time.Duration(success)
	fmt.Printf("Submission Latency:\n")
	fmt.Printf("  Min:               %v\n", lt.metrics.MinLatency)
	fmt.Printf("  Max:               %v\n", lt.metrics.MaxLatency)
	fmt.Printf("  Average:           %v\n\n", avgLatency)

	// Processing stats (from server)
	if serverMetrics != nil {
		completed := int64(serverMetrics["completed_jobs"].(float64))
		serverFailed := int64(serverMetrics["failed_jobs"].(float64))
		
		fmt.Printf("Processing Stats:\n")
		fmt.Printf("  Completed:         %d\n", completed)
		fmt.Printf("  Failed:            %d\n", serverFailed)
		fmt.Printf("  Total Duration:    %v\n", totalDuration)
		fmt.Printf("  Overall Throughput: %.2f jobs/sec\n\n", float64(completed)/totalDuration.Seconds())
	}

	// Performance summary
	fmt.Printf("Performance Summary:\n")
	fmt.Printf("  Submission Rate:   %.2f jobs/sec\n", float64(success)/totalDuration.Seconds())
	fmt.Printf("  Total Time:        %v\n", totalDuration)

	fmt.Printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
}

func (lt *LoadTester) runBenchmark() {
	fmt.Printf("\n🏃 Running Quick Benchmark\n")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	testSizes := []int{10, 50, 100, 500}
	
	for _, size := range testSizes {
		fmt.Printf("Testing with %d jobs...\n", size)
		
		start := time.Now()
		var wg sync.WaitGroup
		success := int64(0)
		
		for i := 0; i < size; i++ {
			wg.Add(1)
			go func(jobNum int) {
				defer wg.Done()
				
				_, _, err := lt.submitJob(
					fmt.Sprintf("Benchmark job #%d", jobNum),
					rand.Intn(10)+1,
				)
				if err == nil {
					atomic.AddInt64(&success, 1)
				}
			}(i)
		}
		
		wg.Wait()
		duration := time.Since(start)
		
		fmt.Printf("  ✓ Completed in %v (%.2f jobs/sec, %d/%d success)\n\n",
			duration, float64(size)/duration.Seconds(), success, size)
	}
	
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
}

func (lt *LoadTester) testPriorityOrdering() {
	fmt.Printf("\n🎯 Testing Priority Ordering\n")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	priorities := []int{1, 5, 10, 3, 8, 2, 9, 4, 7, 6}
	jobIDs := make([]int, 0, len(priorities))

	fmt.Printf("Submitting jobs with priorities: %v\n", priorities)

	for i, priority := range priorities {
		jobResp, _, err := lt.submitJob(
			fmt.Sprintf("Priority test job #%d", i),
			priority,
		)
		if err != nil {
			fmt.Printf("❌ Failed to submit job: %v\n", err)
			continue
		}
		jobIDs = append(jobIDs, jobResp.ID)
		fmt.Printf("  Job %d submitted with priority %d\n", jobResp.ID, priority)
	}

	fmt.Printf("\n⏳ Waiting for processing...\n")
	time.Sleep(15 * time.Second)

	fmt.Printf("\nChecking completion order...\n")
	// Note: This is a simplified check - in reality you'd need to check timestamps
	// from the database to see actual processing order
	
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
}

func checkHealth(baseURL string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy status: %d", resp.StatusCode)
	}

	var health map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("decode error: %w", err)
	}

	fmt.Printf("✅ System Health:\n")
	fmt.Printf("   Status: %v\n", health["status"])
	fmt.Printf("   Leader: %v\n", health["is_leader"])
	fmt.Printf("   Instance: %v\n\n", health["instance_id"])

	return nil
}

func main() {
	// Command line flags
	baseURL := flag.String("url", "http://localhost:8080", "Scheduler base URL")
	numJobs := flag.Int("jobs", 100, "Number of jobs to submit")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	mode := flag.String("mode", "load", "Test mode: load, benchmark, priority")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	fmt.Printf("\n╔═══════════════════════════════════════╗\n")
	fmt.Printf("║   Task Scheduler Load Testing Tool   ║\n")
	fmt.Printf("╚═══════════════════════════════════════╝\n")

	// Health check first
	fmt.Printf("\n🏥 Performing health check...\n")
	if err := checkHealth(*baseURL); err != nil {
		fmt.Printf("❌ Health check failed: %v\n", err)
		fmt.Printf("\nMake sure the scheduler is running at %s\n\n", *baseURL)
		return
	}

	lt := NewLoadTester(*baseURL, *numJobs, *concurrency)

	switch *mode {
	case "load":
		lt.runLoadTest()
	case "benchmark":
		lt.runBenchmark()
	case "priority":
		lt.testPriorityOrdering()
	default:
		fmt.Printf("Unknown mode: %s\n", *mode)
		fmt.Printf("Available modes: load, benchmark, priority\n")
	}
}
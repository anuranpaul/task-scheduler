-- ============================================================================
-- Distributed Task Scheduler Database Schema
-- ============================================================================
-- This schema supports a distributed task scheduling system with:
-- - Job deduplication
-- - Priority-based scheduling
-- - Deferred/scheduled execution
-- - Worker assignment and tracking
-- - Job retry capabilities
-- ============================================================================

-- Drop existing objects if they exist (for clean re-initialization)
DROP TABLE IF EXISTS jobs CASCADE;
DROP INDEX IF EXISTS idx_jobs_status;
DROP INDEX IF EXISTS idx_jobs_pending_schedule;
DROP INDEX IF EXISTS idx_jobs_schedule_at;
DROP INDEX IF EXISTS idx_jobs_dedupe_key;
DROP INDEX IF EXISTS idx_jobs_worker_id;
DROP INDEX IF EXISTS idx_jobs_priority;
DROP TRIGGER IF EXISTS set_updated_at ON jobs;
DROP FUNCTION IF EXISTS update_updated_at_column();

-- ============================================================================
-- Main Jobs Table
-- ============================================================================
CREATE TABLE jobs (
    -- Primary identifier
    id SERIAL PRIMARY KEY,
    
    -- Job content and deduplication
    payload TEXT NOT NULL,
    dedupe_key VARCHAR(255) UNIQUE,
    
    -- Job execution control
    status VARCHAR(50) NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'queued', 'running', 'completed', 'failed', 'cancelled')),
    priority INTEGER NOT NULL DEFAULT 0 
        CHECK (priority >= 0 AND priority <= 100),
    schedule_at TIMESTAMP WITH TIME ZONE,
    
    -- Worker tracking
    worker_id VARCHAR(255),
    
    -- Execution timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Error handling and retry logic
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 
        CHECK (retry_count >= 0),
    max_retries INTEGER NOT NULL DEFAULT 3 
        CHECK (max_retries >= 0),
    
    -- Optional result storage
    result TEXT,
    
    -- Additional metadata
    metadata JSONB DEFAULT '{}'::jsonb
);

-- ============================================================================
-- Indexes for Performance Optimization
-- ============================================================================

-- Core index for job status queries
CREATE INDEX idx_jobs_status ON jobs(status) 
WHERE status IN ('pending', 'queued', 'running');

-- Composite index for the dispatcher query (most critical path)
-- Optimizes: SELECT * FROM jobs WHERE status='pending' AND (schedule_at IS NULL OR schedule_at <= NOW()) ORDER BY priority DESC, created_at ASC
CREATE INDEX idx_jobs_pending_dispatch ON jobs (priority DESC, created_at ASC) 
WHERE status = 'pending' AND (schedule_at IS NULL OR schedule_at <= NOW());

-- Index for scheduled jobs checker
CREATE INDEX idx_jobs_scheduled ON jobs (schedule_at) 
WHERE status = 'pending' AND schedule_at IS NOT NULL;

-- Index for worker-specific queries
CREATE INDEX idx_jobs_worker_id ON jobs(worker_id) 
WHERE worker_id IS NOT NULL;

-- Partial index for failed jobs that might need retry
CREATE INDEX idx_jobs_failed_retryable ON jobs (created_at) 
WHERE status = 'failed' AND retry_count < max_retries;

-- Index for deduplication lookups (already has UNIQUE constraint, but explicit for clarity)
CREATE INDEX idx_jobs_dedupe_key ON jobs(dedupe_key) 
WHERE dedupe_key IS NOT NULL;

-- ============================================================================
-- Triggers and Functions
-- ============================================================================

-- Automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Helper Views
-- ============================================================================

-- View for monitoring active jobs
CREATE OR REPLACE VIEW active_jobs AS
SELECT 
    id,
    status,
    priority,
    worker_id,
    created_at,
    started_at,
    EXTRACT(EPOCH FROM (NOW() - started_at)) AS runtime_seconds
FROM jobs
WHERE status IN ('running', 'queued')
ORDER BY started_at DESC NULLS LAST;

-- View for failed jobs analysis
CREATE OR REPLACE VIEW failed_jobs_summary AS
SELECT 
    DATE_TRUNC('hour', completed_at) AS hour,
    COUNT(*) AS failed_count,
    AVG(retry_count) AS avg_retry_count
FROM jobs
WHERE status = 'failed'
GROUP BY DATE_TRUNC('hour', completed_at)
ORDER BY hour DESC;

-- View for job statistics
CREATE OR REPLACE VIEW job_stats AS
SELECT 
    status,
    COUNT(*) AS count,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) AS avg_duration_seconds,
    MAX(EXTRACT(EPOCH FROM (completed_at - started_at))) AS max_duration_seconds
FROM jobs
WHERE status IN ('completed', 'failed')
GROUP BY status;

-- ============================================================================
-- Utility Functions
-- ============================================================================

-- Function to reset stuck jobs (jobs running longer than expected)
CREATE OR REPLACE FUNCTION reset_stuck_jobs(timeout_minutes INTEGER DEFAULT 30)
RETURNS TABLE(job_id INTEGER, old_status VARCHAR, old_worker_id VARCHAR) AS $$
BEGIN
    RETURN QUERY
    UPDATE jobs
    SET 
        status = 'pending',
        worker_id = NULL,
        error = CONCAT('Reset from stuck state. Previous worker: ', worker_id),
        retry_count = retry_count + 1
    WHERE 
        status = 'running' 
        AND started_at < NOW() - (timeout_minutes || ' minutes')::INTERVAL
        AND retry_count < max_retries
    RETURNING 
        jobs.id, 
        'running'::VARCHAR, 
        jobs.worker_id;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old completed jobs
CREATE OR REPLACE FUNCTION cleanup_old_jobs(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM jobs
    WHERE 
        status IN ('completed', 'cancelled')
        AND completed_at < NOW() - (days_to_keep || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Comments for Documentation
-- ============================================================================

COMMENT ON TABLE jobs IS 'Main table for distributed task scheduling system';
COMMENT ON COLUMN jobs.id IS 'Unique job identifier';
COMMENT ON COLUMN jobs.payload IS 'Job payload/data to be processed';
COMMENT ON COLUMN jobs.dedupe_key IS 'Optional key to prevent duplicate job submission';
COMMENT ON COLUMN jobs.status IS 'Current job status: pending, queued, running, completed, failed, cancelled';
COMMENT ON COLUMN jobs.priority IS 'Job priority (0-100, higher = more important)';
COMMENT ON COLUMN jobs.schedule_at IS 'When the job should be executed (NULL = immediate)';
COMMENT ON COLUMN jobs.worker_id IS 'ID of the worker currently processing the job';
COMMENT ON COLUMN jobs.retry_count IS 'Number of retry attempts made';
COMMENT ON COLUMN jobs.max_retries IS 'Maximum number of retry attempts allowed';
COMMENT ON COLUMN jobs.metadata IS 'Additional JSON metadata for the job';

-- ============================================================================
-- Sample Data for Testing (Optional - comment out for production)
-- ============================================================================

-- INSERT INTO jobs (payload, priority, status) 
-- VALUES 
--     ('{"task": "send_email", "to": "user@example.com"}', 10, 'pending'),
--     ('{"task": "generate_report", "report_id": 123}', 50, 'pending'),
--     ('{"task": "cleanup", "resource": "temp_files"}', 5, 'pending');

-- ============================================================================
-- Grants (Adjust based on your user setup)
-- ============================================================================

-- Example: Grant appropriate permissions to application user
-- GRANT SELECT, INSERT, UPDATE, DELETE ON jobs TO task_scheduler_app;
-- GRANT USAGE, SELECT ON SEQUENCE jobs_id_seq TO task_scheduler_app;
-- GRANT EXECUTE ON FUNCTION reset_stuck_jobs TO task_scheduler_app;
-- GRANT EXECUTE ON FUNCTION cleanup_old_jobs TO task_scheduler_app;

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Verify table structure
-- SELECT column_name, data_type, is_nullable, column_default 
-- FROM information_schema.columns 
-- WHERE table_name = 'jobs' 
-- ORDER BY ordinal_position;

-- Verify indexes
-- SELECT indexname, indexdef 
-- FROM pg_indexes 
-- WHERE tablename = 'jobs';

-- ============================================================================
-- End of Schema
-- ============================================================================
# Distributed Task Scheduler

A production-grade, horizontally scalable distributed task scheduling system built with Go. This system demonstrates advanced distributed systems concepts including leader election, service discovery, distributed locking, and fault tolerance.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Distributed System                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  Scheduler 1 │    │  Scheduler 2 │    │  Scheduler N │  │
│  │   (Leader)   │    │  (Follower)  │    │  (Follower)  │  │
│  └──────┬───────┘    └──────────────┘    └──────────────┘  │
│         │                                                     │
│         │  Leader Election (etcd)                           │
│         └──────────────────────────────────────────────────┐│
│                                                              ││
│  ┌────────────────────────────────────────────────────────┐││
│  │            Coordination Layer (etcd)                    │││
│  │  • Leader Election                                      │││
│  │  • Distributed Locks                                    │││
│  │  • Service Discovery                                    │││
│  │  • Configuration Management                             │││
│  └────────────────────────────────────────────────────────┘││
│                         │                                    ││
│         ┌───────────────┼───────────────┐                   ││
│         │               │               │                   ││
│  ┌──────▼─────┐  ┌──────▼─────┐  ┌──────▼─────┐           ││
│  │  Worker 1  │  │  Worker 2  │  │  Worker N  │           ││
│  └────────────┘  └────────────┘  └────────────┘           ││
│         │               │               │                   ││
└─────────┼───────────────┼───────────────┼───────────────────┘│
          │               │               │                    │
   ┌──────▼───────────────▼───────────────▼──────┐            │
   │         Redis Stream (Job Queue)             │            │
   └──────────────────────────────────────────────┘            │
                          │                                     │
                 ┌────────▼────────┐                           │
                 │   PostgreSQL    │◄──────────────────────────┘
                 │  (Job Metadata) │
                 └─────────────────┘
```

## 🚀 Key Features

### Distributed Coordination
- **Leader Election**: Automatic leader election using etcd for high availability
- **Service Discovery**: Dynamic service registration and discovery
- **Distributed Locking**: Prevents duplicate job processing during network partitions
- **Fault Tolerance**: Automatic failover and session recovery

### Scalability
- **Horizontal Scaling**: Add schedulers and workers dynamically
- **Load Distribution**: Redis Streams consumer groups for fair load balancing
- **Priority Queuing**: Jobs processed based on priority levels
- **Scheduled Jobs**: Support for delayed job execution

### Reliability
- **At-Least-Once Processing**: Jobs guaranteed to be processed at least once
- **Stalled Job Recovery**: Automatic claiming and reprocessing of stuck jobs
- **Graceful Shutdown**: Clean shutdown with leadership resignation
- **Transaction Safety**: ACID guarantees for job state transitions

### Observability
- **Health Endpoints**: Real-time health status of all components
- **Metrics Collection**: Job statistics and system performance metrics
- **Structured Logging**: Context-aware logging with slog
- **Service Monitoring**: Track active schedulers and workers

## 📋 System Components

### 1. Coordination Layer (`pkg/coordinator/coordination.go`)
Abstracts distributed system primitives:
- Leader election with automatic failover
- Distributed mutex for critical sections
- Service registry for discovery
- Key-value storage for configuration

### 2. Scheduler
- Runs leader election
- Only leader dispatches jobs to workers
- Monitors scheduled jobs
- Provides REST API for job submission
- Reports metrics to coordination layer

### 3. Worker
- Consumes jobs from Redis streams
- Registers in service discovery
- Uses distributed locks to prevent duplicate processing
- Reports processing metrics
- Recovers stalled jobs

## 🛠️ Technology Stack

- **Go 1.21+**: Core language
- **PostgreSQL**: Job metadata and state persistence
- **Redis**: Job queue (Redis Streams)
- **etcd**: Distributed coordination and consensus
- **Docker**: Containerization

## 📦 Installation

### Prerequisites
```bash
# Install Go 1.21+
# Install Docker and Docker Compose
```

### Quick Start
```bash
# Clone the repository
git clone https://github.com/anuranpaul/task-scheduler.git
cd task-scheduler

# Start infrastructure
docker-compose up -d postgres redis etcd

# Set environment variables
export POSTGRES_DSN="postgres://user:password@localhost:5432/taskdb?sslmode=disable"
export REDIS_URL="localhost:6379"
export ETCD_ENDPOINTS="localhost:2379"

# Initialize database schema
psql $POSTGRES_DSN < migrations/init.sql

# Start scheduler
go run cmd/scheduler/main.go

# Start workers (in separate terminals)
WORKER_ID=worker-1 WORKER_HTTP_ADDR=:9091 go run cmd/worker/main.go
WORKER_ID=worker-2 WORKER_HTTP_ADDR=:9092 go run cmd/worker/main.go
```

## 🔧 Configuration

### Environment Variables

#### Scheduler
```bash
HTTP_ADDR=:8080                    # HTTP server address
POSTGRES_DSN=<connection-string>   # PostgreSQL DSN
REDIS_URL=localhost:6379           # Redis address
ETCD_ENDPOINTS=localhost:2379      # etcd endpoints (comma-separated)
REDIS_STREAM_NAME=jobs             # Redis stream name
LOG_LEVEL=INFO                     # Logging level
LOG_FORMAT=JSON                    # Log format (JSON or TEXT)
```

#### Worker
```bash
WORKER_ID=worker-1                 # Unique worker identifier
WORKER_HTTP_ADDR=:9090             # Worker HTTP server
POSTGRES_DSN=<connection-string>   # PostgreSQL DSN
REDIS_URL=localhost:6379           # Redis address
ETCD_ENDPOINTS=localhost:2379      # etcd endpoints
REDIS_GROUP_NAME=workers           # Redis consumer group
```

## 📚 API Reference

### Create Job
```bash
POST /jobs
Content-Type: application/json

{
  "payload": "task data",
  "priority": 5,
  "dedupe_key": "unique-key",
  "schedule_at": "2024-12-25T10:00:00Z"
}

Response: 201 Created
{
  "id": 123,
  "status": "pending",
  "created_at": "2024-01-15T10:00:00Z"
}
```

### Get Job Status
```bash
GET /jobs/{id}

Response: 200 OK
{
  "id": 123,
  "status": "completed",
  "worker_id": "worker-1",
  "created_at": "2024-01-15T10:00:00Z",
  "started_at": "2024-01-15T10:00:05Z",
  "completed_at": "2024-01-15T10:00:07Z"
}
```

### List Jobs
```bash
GET /jobs?status=pending

Response: 200 OK
[
  {
    "id": 123,
    "status": "pending",
    "created_at": "2024-01-15T10:00:00Z"
  }
]
```

### Health Check
```bash
GET /health

Response: 200 OK
{
  "status": "healthy",
  "instance_id": "scheduler-host-1",
  "is_leader": true,
  "coordinator_health": true,
  "timestamp": 1705315200
}
```

### Metrics
```bash
GET /metrics

Response: 200 OK
{
  "timestamp": 1705315200,
  "pending_jobs": 10,
  "queued_jobs": 5,
  "running_jobs": 3,
  "completed_jobs": 1000,
  "failed_jobs": 2,
  "jobs_dispatched": 1015,
  "last_dispatch": 1705315190
}
```

### Leader Information
```bash
GET /leader

Response: 200 OK
{
  "current_leader": "scheduler-host-1",
  "is_leader": true,
  "instance_id": "scheduler-host-1"
}
```

## 🧪 Testing

### Unit Tests
```bash
go test ./pkg/...
```

### Integration Tests
```bash
go test ./tests/integration/...
```

### Load Testing
```bash
# Using Apache Bench
ab -n 10000 -c 100 -p job.json -T application/json \
   http://localhost:8080/jobs
```

## 🎯 Design Patterns & Best Practices

### 1. Separation of Concerns
- Coordination logic isolated in separate package
- Clear boundaries between scheduler, worker, and coordination

### 2. Fault Tolerance
- Automatic leader re-election on failure
- Graceful degradation when coordinator is unavailable
- Retry logic with exponential backoff

### 3. Idempotency
- Deduplication using unique keys
- Distributed locks prevent duplicate processing
- Transactional updates with optimistic locking

### 4. Observability
- Structured logging throughout
- Metrics exported to coordination layer
- Health endpoints for monitoring

### 5. Scalability
- Stateless workers for easy horizontal scaling
- Single leader scheduler prevents coordination overhead
- Redis Streams for efficient message distribution

## 🔒 Production Considerations

### High Availability
- Run multiple scheduler instances (only one will be leader)
- Run multiple workers for redundancy
- Use managed services (RDS, ElastiCache, Kubernetes)

### Security
- Use TLS for etcd communication
- Implement authentication for REST API
- Use secrets management for credentials

### Monitoring
- Set up Prometheus metrics
- Configure alerting for leader changes
- Monitor job processing lag

### Backup & Recovery
- Regular PostgreSQL backups
- Redis persistence configuration
- etcd cluster backup strategy

## 🚦 Performance Characteristics

- **Job Submission**: ~1000 requests/second
- **Job Processing**: Limited by worker count and job duration
- **Latency**: <10ms job submission, <1s job start time
- **Scalability**: Linear with worker count

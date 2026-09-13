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
- **PostgreSQL**: Job metadata and state persistence (ACID guarantees)
- **Redis**: Job queue (Redis Streams with Consumer Groups)
- **etcd**: Distributed coordination, leader election, and distributed locking
- **Docker & Kubernetes**: Containerization and cloud-native orchestration (Deployments, StatefulSets, HPA, Kustomize)
- **kind**: Local multi-node Kubernetes development and testing

## 📦 Installation & Deployment

### Option A: Kubernetes Deployment with kind (Recommended)

Deploy the full stack (scheduler, autoscaled workers, 3-node etcd StatefulSet, PostgreSQL StatefulSet, Redis, and schema migration Job) to a local kind cluster:

```bash
# 1. Create multi-node kind cluster
kind create cluster --name task-scheduler --config kind-config.yaml

# 2. Build and load container images
docker build -t task-scheduler-scheduler:local -f Dockerfile.scheduler .
docker build -t task-scheduler-worker:local -f Dockerfile.worker .
kind load docker-image task-scheduler-scheduler:local --name task-scheduler
kind load docker-image task-scheduler-worker:local --name task-scheduler

# 3. Deploy all components via Kustomize
kubectl apply -k .

# 4. Wait for database migration and workload rollouts
kubectl -n task-scheduler wait --for=condition=complete job/db-init --timeout=180s
kubectl -n task-scheduler rollout status deployment/scheduler --timeout=180s
kubectl -n task-scheduler rollout status deployment/worker --timeout=180s

# 5. Access the scheduler API
kubectl -n task-scheduler port-forward svc/scheduler 8080:8080
```

> [!TIP]
> See [k8s/README.md](k8s/README.md) for full deployment instructions, failover testing, and [k8s/VALIDATION.md](k8s/VALIDATION.md) for empirical benchmark and HPA scaling results.

---

### Option B: Local Development with Docker Compose

Run dependencies in Docker and compile Go binaries locally:

```bash
# Clone the repository
git clone https://github.com/anuranpaul/task-scheduler.git
cd task-scheduler

# Start infrastructure (PostgreSQL, Redis, etcd)
docker-compose up -d postgres redis etcd

# Set environment variables
export POSTGRES_DSN="postgres://user:password@localhost:5432/taskdb?sslmode=disable"
export REDIS_URL="localhost:6379"
export ETCD_ENDPOINTS="localhost:2379"

# Initialize database schema
psql $POSTGRES_DSN < init.sql

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

### Kubernetes Cluster & Failover Validation
The repository includes a validation suite and procedures on a multi-node kind cluster:
- **Scheduler Failover**: Deleting the active leader pod demonstrates immediate promotion of the standby replica without job loss.
- **etcd Fault Tolerance**: Deleting `etcd-0` validates Raft quorum continuity and StatefulSet PVC persistence.
- **Worker Autoscaling (HPA)**: Verifies dynamic scaling from 2 to 8 worker replicas under CPU load.
- **Scaling Benchmark**: Observed a 3.69x throughput improvement (33.04s vs 8.96s for 32 jobs) when scaling from 2 to 8 workers.

For detailed test steps and validation data, see [k8s/VALIDATION.md](k8s/VALIDATION.md). For deep technical design, see [architecture.md](architecture.md).

## 🎯 Design Patterns & Best Practices

### 1. Separation of Concerns
- Coordination logic isolated in separate package
- Clear boundaries between scheduler, worker, and coordination
- Container orchestration (Kubernetes) decoupled from distributed data consistency (etcd/PostgreSQL/Redis)

### 2. Fault Tolerance
- Automatic leader re-election on failure via etcd lease
- Graceful shutdown (`terminationGracePeriodSeconds: 35` for workers > 30s job timeout)
- Kubernetes native health probes (startup, readiness, liveness)

### 3. Idempotency
- Deduplication using unique keys (`dedupe_key`)
- Distributed locks prevent duplicate processing
- Transactional updates with optimistic locking (`FOR UPDATE`)

### 4. Observability
- Structured logging throughout
- Metrics exported to coordination layer
- Health endpoints (`/health`) integrated with Kubernetes probes

### 5. Scalability
- Stateless workers with dynamic identity via Kubernetes Downward API
- Horizontal Pod Autoscaler (HPA) scaling worker deployment from 2 to 8 replicas
- Single active leader scheduler prevents coordination overhead
- Redis Streams with consumer groups for fair message distribution

## 🔒 Production Considerations

### High Availability
- Run multi-replica scheduler Deployments (active leader dispatches, standbys serve API and act as hot failover)
- Run autoscaled worker Deployments with HPA
- Deploy etcd as a 3-node StatefulSet with persistent volume claims
- Use managed cloud services (e.g. AWS EKS, RDS PostgreSQL, ElastiCache) for large-scale production deployments

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

## ⏱️ Service Level Agreements (SLAs)

### Job Processing SLAs

| Metric | Target | How We Achieve It |
|--------|--------|-------------------|
| Job Submission Latency | < 10ms (p99) | Direct PostgreSQL insert with connection pooling |
| Queue Wait Time | < 1s (p95) | Leader dispatches pending jobs every 1 second |
| Processing Start | < 5s (p95) | Workers poll Redis every 100ms |
| Stalled Job Recovery | < 5 min | Workers claim idle jobs exceeding 5 min threshold |
| Leadership Failover | < 10s | etcd session TTL of 10 seconds |

### Timeout Configuration

| Component | Timeout | Configurable Via |
|-----------|---------|------------------|
| Database Connection | 5s | `POSTGRES_DSN` (connect_timeout param) |
| Redis Read | 3s | `REDIS_READ_TIMEOUT_SECS` |
| Redis Write | 3s | `REDIS_WRITE_TIMEOUT_SECS` |
| etcd Dial | 5s | `ETCD_DIAL_TIMEOUT_SECS` |
| Job Processing | 30s | Hardcoded (context timeout in worker) |
| HTTP Server | 15s | Hardcoded in scheduler/worker |

### Retry Policy

| Scenario | Retry Strategy | Max Attempts |
|----------|----------------|--------------|
| Job Processing Failure | Immediate retry via stalled job recovery | 3 (configurable via `max_retries`) |
| Database Connection | Exponential backoff | Unlimited (reconnect on each operation) |
| Leader Election | 5s fixed delay | Unlimited |
| Coordinator Lock | Single attempt (TryLock) | 1 |

### Delivery Guarantees

- **At-Least-Once Delivery**: Jobs are guaranteed to be processed at least once
- **Idempotency Responsibility**: Job handlers should be idempotent
- **Deduplication**: Use `dedupe_key` field to prevent duplicate submissions

## 🔌 Extractor Layer

The extractor layer (`pkg/extractor/`) provides a pluggable mechanism for processing job payloads. Instead of hardcoding job processing logic, workers can delegate to extractors based on payload type.

### Built-in Extractors

| Extractor | Description | Payload Format |
|-----------|-------------|----------------|
| `NoopExtractor` | Simulates work (default) | Any string |
| `HTTPExtractor` | Makes HTTP requests | `{"url": "...", "method": "...", "body": {...}}` |

### Usage Example

```go
import "github.com/anuranpaul/task-scheduler/pkg/extractor"

// Create registry with extractors
registry := extractor.NewRegistry()
registry.Register(extractor.NewHTTPExtractor(nil))
registry.SetFallback(extractor.NewNoopExtractor(2 * time.Second))

// Process a job
result, err := registry.Process(ctx, jobID, payload)
```

### Custom Extractors

Implement the `Extractor` interface to add custom job processing:

```go
type Extractor interface {
    Name() string
    CanHandle(ctx context.Context, payload string) bool
    Extract(ctx context.Context, jobID, payload string) (*Result, error)
}
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

MIT License - see LICENSE file for details
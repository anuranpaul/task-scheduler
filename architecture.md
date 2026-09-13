# Architecture Deep Dive

## Table of Contents
1. [System Overview](#system-overview)
2. [Kubernetes Cluster & Deployment Architecture](#kubernetes-cluster--deployment-architecture)
3. [Coordination Layer](#coordination-layer)
4. [Leader Election](#leader-election)
5. [Job Lifecycle](#job-lifecycle)
6. [Fault Tolerance & Pod Lifecycle](#fault-tolerance--pod-lifecycle)
7. [Scalability & Autoscaling](#scalability--autoscaling)
8. [Data Consistency](#data-consistency)
9. [Performance Optimization](#performance-optimization)
10. [Monitoring & Observability](#monitoring--observability)
11. [Extractor Layer](#extractor-layer)
12. [Future Enhancements](#future-enhancements)

---

## System Overview

### Design Principles

1. **Single Responsibility**: Each component has a clear, focused purpose (scheduler dispatches, workers execute, coordinator provides consensus).
2. **High Availability**: No single point of failure; multi-replica scheduler deployment with active-passive leader election and multi-node etcd consensus.
3. **Horizontal Scalability**: Add capacity dynamically via Kubernetes replica scaling and Horizontal Pod Autoscaling (HPA).
4. **At-Least-Once Delivery**: Jobs are guaranteed to be processed with transactional state machines and idle job reclaim.
5. **Cloud-Native Resilience**: Native Kubernetes probe integration (startup, readiness, liveness), declarative configuration, and graceful termination.
6. **Observable**: Rich metrics, structured logging, and Kubernetes health probes at every layer.

### Two-Tier Architecture: Orchestration vs. Application Coordination

The system separates concerns across two complementary layers:
- **Kubernetes (Container Orchestrator)**: Manages container life-cycles, desired replica counts, pod scheduling, network routing, persistent volume attachments, and horizontal autoscaling.
- **Application Coordination Layer (etcd / Redis / Postgres)**: Manages application-level distributed consensus, active/passive leader status among scheduler replicas, distributed job locks, Redis Streams consumer group partitioning, and ACID state transitions.

```
                  ┌────────────────────────────────────────────────────────┐
                  │              Kubernetes Cluster (Namespace)            │
                  │                                                        │
Client Traffic ───┼─► [Service: scheduler]                                  │
                  │          │                                             │
                  │   ┌──────┴──────────────┐                              │
                  │   ▼                     ▼                              │
                  │ [Pod: scheduler-0]   [Pod: scheduler-1]                │
                  │ (Leader via etcd)    (Passive Standby)                 │
                  │   │        │            │                              │
                  │   │        └──────┬─────┘  Leader Election / Locks     │
                  │   │ Dispatch      ▼                                    │
                  │   │          [StatefulSet: etcd (3 Nodes)]             │
                  │   ▼          (etcd-0, etcd-1, etcd-2 + Headless Svc)   │
                  │ [Deployment / Service: redis]                          │
                  │ (Jobs Stream Queue)                                    │
                  │   │                                                    │
                  │   ├──────────────────────────────┐                     │
                  │   ▼                              ▼                     │
                  │ [Pod: worker-xxx]            [Pod: worker-yyy]         │
                  │ (Consumer Group)             (Consumer Group)          │
                  │ (HPA: 2-8 Replicas)          (HPA: 2-8 Replicas)       │
                  │   │                              │                     │
                  │   └──────────────┬───────────────┘                     │
                  │                  ▼ Status Updates                      │
                  │          [StatefulSet: postgres]                       │
                  │          (Persistent Volume Claim)                     │
                  │                                                        │
                  │   [Job: db-init] (Run-to-completion Schema Init)       │
                  └────────────────────────────────────────────────────────┘
```

---

## Kubernetes Cluster & Deployment Architecture

### Workload Classification & Controller Mapping

| Workload | K8s Controller | Replicas | Persistence / Storage | Network Exposure |
|---|---|---|---|---|
| **Scheduler** | `apps/v1 Deployment` | 2 (Active/Passive) | Stateless | `v1 Service` (ClusterIP `:8080`) |
| **Worker** | `apps/v1 Deployment` | 2–8 (HPA-driven) | Stateless | Internal only (Metrics port `:9090`) |
| **etcd Cluster** | `apps/v1 StatefulSet` | 3 (Quorum = 2) | PVC `500Mi` per pod | Headless Service `clusterIP: None` (`:2379`, `:2380`) |
| **PostgreSQL** | `apps/v1 StatefulSet` | 1 | PVC `1Gi` (`ReadWriteOnce`) | Headless Service `clusterIP: None` (`:5432`) |
| **Redis** | `apps/v1 Deployment` | 1 | Ephemeral / Append-Only | `v1 Service` (ClusterIP `:6379`) |
| **Database Migration** | `batch/v1 Job` | 1 (Run-to-completion) | None | None |

### Controller Design Details

#### 1. Scheduler Deployment (`k8s/base/scheduler-deployment.yaml`)
- Runs **2 replicas** for high availability. Both pods are ready and running, but only the pod holding the etcd campaign lease executes `dispatchPendingJobs()`. The standby pod serves read-only health checks and immediately promotes if the leader dies.
- Exposed via a Kubernetes ClusterIP Service (`scheduler:8080`) that routes client submissions to either scheduler pod (both can accept HTTP submissions since job insertion is an ACID insert into PostgreSQL).

#### 2. Worker Deployment & Autoscaler (`k8s/base/worker-deployment.yaml` & `worker-hpa.yaml`)
- Scaled dynamically between **2 and 8 replicas** based on workload.
- Injects Pod identity dynamically via the **Kubernetes Downward API**:
  ```yaml
  env:
    - name: WORKER_ID
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
  ```
  This eliminates central coordination or manual ID assignment; every worker pod automatically registers into Redis consumer groups and service discovery with its unique pod name (e.g., `worker-769d8b7bf-w5k2v`).

#### 3. etcd StatefulSet (`k8s/base/etcd-statefulset.yaml`)
- Deploys a 3-member cluster using `quay.io/coreos/etcd:v3.5.9`.
- Uses `podManagementPolicy: Parallel` for rapid quorum convergence.
- Configured with static bootstrap URLs using predictable StatefulSet DNS hostnames:
  ```
  etcd-0=http://etcd-0.etcd.task-scheduler.svc.cluster.local:2380,
  etcd-1=http://etcd-1.etcd.task-scheduler.svc.cluster.local:2380,
  etcd-2=http://etcd-2.etcd.task-scheduler.svc.cluster.local:2380
  ```
- Backed by dynamic `volumeClaimTemplates` (`500Mi`) so member state survives pod restarts.
- Accompanied by a headless service (`publishNotReadyAddresses: true`) allowing peer discovery during cluster formation.

#### 4. PostgreSQL StatefulSet (`k8s/base/postgres-statefulset.yaml`)
- Single replica deployed as a StatefulSet to guarantee persistent storage identity.
- Volume mounted to `/var/lib/postgresql/data` via a dedicated `1Gi` PersistentVolumeClaim.

#### 5. Database Initialization Job (`k8s/base/db-init-job.yaml`)
- Schema initialization runs as a decoupled Kubernetes Job (`batch/v1`) using `postgres:15-alpine`.
- Executes `psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f /sql/init.sql`.
- The SQL file is generated into a ConfigMap (`init-sql-file`) directly from repository root via Kustomize `configMapGenerator`.
- Application deployments can run `kubectl wait --for=condition=complete job/db-init` to guarantee schema readiness before starting.

### Configuration & Secret Management

Configuration is split cleanly between sensitive and non-sensitive values:
- **ConfigMap (`task-scheduler-config`)**: In-cluster DNS addresses and stream settings:
  ```yaml
  REDIS_URL: redis:6379
  ETCD_ENDPOINTS: etcd-0.etcd:2379,etcd-1.etcd:2379,etcd-2.etcd:2379
  REDIS_STREAM_NAME: jobs
  REDIS_GROUP_NAME: workers
  LOG_LEVEL: INFO
  LOG_FORMAT: JSON
  ```
- **Secret (`postgres-credentials`)**: Stores DB credentials and the fully constructed connection string:
  ```yaml
  POSTGRES_DSN: postgres://taskuser:taskpass@postgres:5432/taskdb?sslmode=disable
  ```
- Injected into pods using `envFrom: [configMapRef, secretRef]`, ensuring 12-factor application compliance.

---

## Coordination Layer

### Purpose
The coordination layer (`pkg/coordinator`) abstracts distributed system primitives, providing a clean interface for:
- Leader election
- Distributed locking
- Service discovery
- Configuration management

### Design Decisions

#### 1. Abstraction Over etcd
**Why**: Direct etcd usage couples the system to a specific technology. The coordinator provides:
```go
// High-level interface
election.Campaign(ctx)
lock.Lock(ctx)
registry.Register(ctx, metadata)
```

**Benefits**:
- Easy to swap underlying technology (Consul, Zookeeper, or Kubernetes Leases)
- Simplified testing with mock implementations
- Consistent error handling and retry logic

#### 2. Session Management
Each coordination primitive manages its own etcd session with appropriate TTL:
```go
session := concurrency.NewSession(etcdClient, concurrency.WithTTL(10))
```

**Why separate sessions?**
- Independent lifecycle management
- Granular control over lease renewal
- Isolation of failures

#### 3. Callback Architecture
Leader election uses callbacks for state transitions:
```go
callbacks := LeaderCallbacks{
    OnElected: func(ctx) error { /* Start leader tasks */ },
    OnRevoked: func(ctx) error { /* Stop leader tasks */ },
    OnError:   func(ctx, err) { /* Handle errors */ }
}
```

---

## Leader Election

### Algorithm & Mechanism
Uses etcd's built-in leader election based on Raft consensus:
1. **Campaign**: Instance attempts to acquire leadership under prefix `/scheduler/leader`.
2. **Session TTL**: Leader must keep lease renewed (10s TTL); if lease expires or pod dies, leadership is revoked.
3. **Observe**: Standby instances watch the leader key and campaign when it is deleted.

### Kubernetes Deployment Integration

In Kubernetes, multiple scheduler pods run under the same Deployment (`replicas: 2`).

```
Scheduler Pod 1 (Leader)          etcd Cluster (3 Nodes)       Scheduler Pod 2 (Standby)
       │                                   │                                │
       ├── Campaign() ────────────────────►│                                │
       │◄── Elected (Lease acquired) ──────┤                                │
       │    (Starts dispatch loop)         │◄── Campaign() (Queued) ────────┤
       │                                   │    (Serves /health, /jobs)     │
       │                                   │                                │
[Pod Killed / Crash]                       │                                │
       X (Lease expires in 10s)            │                                │
                                           ├── Promoted (Lease granted) ───►│
                                           │    (Starts dispatch loop)      │
```

#### Why Application Leader Election in Kubernetes?
While Kubernetes offers native `coordination.k8s.io` Leases, etcd-based coordination provides:
1. **Zero RBAC Overhead**: Scheduler pods do not require Kubernetes API cluster roles or service accounts with lease-writing permissions.
2. **Unified Consistency Model**: The same consensus cluster (etcd) provides leader election, distributed locking for workers, and dynamic service registration.
3. **Environment Portability**: The same codebase functions identically in local development, bare Docker Compose, or managed Kubernetes.

### Handling Split Brain

**Problem**: Network partition or transient network glitch creates temporary isolation.

**Solution**:
1. **etcd Quorum**: Leader election requires majority consensus (2 of 3 etcd nodes). A partitioned scheduler in the minority partition loses its lease.
2. **Distributed Locks**: Critical job state updates require acquiring a distributed lock in etcd.
3. **Database Transactions**: Status transitions enforce `WHERE status = 'queued'` under `FOR UPDATE`, ensuring that even if two dispatchers attempt to process the same job, the database serializes and rejects duplicates.

---

## Job Lifecycle

### State Machine

```
     ┌─────────┐
     │ PENDING │
     └────┬────┘
          │
          │ (Leader Scheduler dispatches)
          ▼
     ┌────────┐
     │ QUEUED │
     └────┬───┘
          │
          │ (Worker picks up from Redis Stream)
          ▼
     ┌─────────┐
     │ RUNNING │
     └────┬────┘
          │
          ├──────────┬──────────┐
          │          │          │
          ▼          ▼          ▼
    ┌───────────┐ ┌──────┐  ┌────────┐
    │ COMPLETED │ │FAILED│  │TIMEOUT │
    └───────────┘ └──────┘  └────────┘
```

### State Transitions

#### 1. PENDING → QUEUED
- **Actor**: Scheduler (Leader only)
- **Action**: Atomically pushes job ID and payload to Redis Stream `jobs` and updates status in PostgreSQL.
- **Guarantees**: Atomic and idempotent via transaction.

#### 2. QUEUED → RUNNING
- **Actor**: Worker pod
- **Action**: Reads from consumer group via `XReadGroup`, acquires job lock in etcd, and transitions job to `running` with its `worker_id` recorded.

#### 3. RUNNING → COMPLETED/FAILED
- **Actor**: Worker pod
- **Action**: Worker delegates execution to Extractor, updates PostgreSQL with final status, and calls `XAck` to acknowledge the stream message.

---

## Fault Tolerance & Pod Lifecycle

### Kubernetes Pod Lifecycle & Graceful Termination

Kubernetes provides automated pod restart and rescheduling, while the application ensures clean shutdown and zero lost jobs.

```
SIGTERM from K8s
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ Scheduler Pod: terminationGracePeriodSeconds: 20           │
│ 1. HTTP server stops accepting new connections              │
│ 2. Leadership resigned in etcd (session closed cleanly)     │
│ 3. Standby scheduler promoted immediately (no 10s wait)     │
└─────────────────────────────────────────────────────────────┘

SIGTERM from K8s
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ Worker Pod: terminationGracePeriodSeconds: 35              │
│ 1. Stop reading new messages from Redis Stream              │
│ 2. In-flight job allowed to complete (worker timeout: 30s)  │
│ 3. 35s grace period > 30s job timeout ensures clean exit    │
│ 4. If pod killed prematurely, Stalled Job Recovery claims   │
└─────────────────────────────────────────────────────────────┘
```

### Health Probes Architecture

All containers implement a three-tier Kubernetes probe strategy:

| Component | Probe Type | Mechanism | Interval | Threshold / Timeout | Purpose |
|---|---|---|---|---|---|
| **Scheduler** | Startup | `HTTP GET /health` | 5s | Failure: 30 (150s max) | Allows time for DB/etcd connection establishment |
| | Readiness | `HTTP GET /health` | 5s | Failure: 3 | Removes pod from Service routing if downstream fails |
| | Liveness | `HTTP GET /health` | 10s | Initial: 10s | Restarts deadlocked scheduler pod |
| **Worker** | Startup | `HTTP GET /health` | 5s | Failure: 30 (150s max) | Verifies Redis/DB connectivity |
| | Readiness | `HTTP GET /health` | 5s | Failure: 3 | Verifies worker is operational |
| | Liveness | `HTTP GET /health` | 10s | Initial: 10s | Restarts crashed or deadlocked worker |
| **PostgreSQL** | Readiness/Liveness | `pg_isready -U taskuser` | 5s / 10s | Initial: 5s / 10s | Ensures database is responding to SQL queries |
| **Redis** | Readiness/Liveness | `redis-cli ping` | 5s / 10s | Initial: 3s / 10s | Verifies Redis response loop |
| **etcd** | Readiness/Liveness | `etcdctl endpoint health` | 5s / 10s | Initial: 10s / 20s | Confirms quorum membership and Raft health |

### Failure Scenarios & Recovery Matrix

#### 1. Scheduler Leader Pod Deletion
- **Symptom**: Leader pod is deleted or evicted by Kubernetes.
- **Clean Shutdown**: Pod receives `SIGTERM`, releases etcd lease during the 20s grace period.
- **Failover**: Standby scheduler pod detects revocation/deletion and is elected leader in < 1 second.
- **K8s Self-Healing**: Kubernetes Deployment controller detects replica count = 1 and schedules a new scheduler pod to restore redundancy.

#### 2. Worker Pod Sudden Failure (OOM / Node Crash)
- **Symptom**: Worker pod crashes while holding an in-flight job without calling `XAck`.
- **Redis Consumer Group Protection**: The job message remains in Redis Stream's Pending Entries List (PEL).
- **Stalled Job Recovery**: Surviving or newly scheduled worker pods scan for idle jobs via `XPendingExt` (idle > 5m) and claim them using `XClaim`.
- **Duplicate Prevention**: If the old worker somehow resumes, database row locks (`FOR UPDATE`) prevent double completion.

#### 3. etcd Member Failure (`etcd-0`)
- **Symptom**: One etcd pod terminates or its node reboots.
- **Quorum Preservation**: etcd cluster has 3 members; quorum requires 2 nodes (`(3/2)+1 = 2`). The remaining 2 nodes continue without interruption.
- **Persistent Volume Reattachment**: StatefulSet recreates `etcd-0`, reattaches its PVC (`etcd-data`), and the node rejoins the Raft cluster automatically.

---

## Scalability & Autoscaling

### Horizontal Scaling in Kubernetes

Workload capacity is managed declaratively:

```bash
# Manually scale schedulers for regional redundancy
kubectl -n task-scheduler scale deployment/scheduler --replicas=3

# Manually scale workers for batch throughput
kubectl -n task-scheduler scale deployment/worker --replicas=10
```

### Horizontal Pod Autoscaler (HPA)

Workers are dynamically autoscaled using Kubernetes `HorizontalPodAutoscaler` (`autoscaling/v2`):

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: worker
  minReplicas: 2
  maxReplicas: 8
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60
```

### Downward API Identity Resolution

When HPA scales worker replicas up or down, each pod receives its identity automatically via `metadata.name`:
- Pod `worker-5d97f48cb-a1b2c` has `WORKER_ID="worker-5d97f48cb-a1b2c"`
- Pod `worker-5d97f48cb-d3e4f` has `WORKER_ID="worker-5d97f48cb-d3e4f"`

**Advantages**:
- No central naming registry or ID counters required.
- Pod restarts or replacements get clean, distinct consumer names in Redis.
- Traceability: PostgreSQL stores the exact pod name that executed the job in `worker_id`.

### Metric Autoscaling: CPU vs. Queue Backlog

- **Current Implementation**: The included HPA leverages **CPU Utilization (60% target)** via Metrics Server.
- **Production Recommendation**: CPU utilization can lag when jobs are I/O bound (e.g., waiting on external HTTP requests in `HTTPExtractor`). Production autoscaling should leverage an external metric scaler (such as **KEDA** or Prometheus adapter) scaling on Redis Stream consumer lag (`jobs_unprocessed = stream_length - stream_last_delivered_id`).

### Empirical Scaling Benchmarks

Empirical validation performed on a multi-node kind cluster (Kubernetes v1.37):

| Benchmark Scenario | Replicas | Completed Jobs | Elapsed Time | Speedup / Impact |
|---|---|---|---|---|
| **Two-Worker Baseline** | 2 Workers | 32 jobs (2s synthetic) | 33.04s | Baseline (~1.0 job/s) |
| **Eight-Worker Scaled** | 8 Workers | 32 jobs (2s synthetic) | 8.96s | **3.69x throughput scaling** |
| **High Concurrency Reliability** | Autoscaled | 200 jobs | Fully processed | 100% success, 0 failed, 0 stuck |
| **Concurrent Deduplication** | Autoscaled | 100 concurrent requests (1 key) | Completed | Exactly 1 job record created |

---

## Data Consistency

### Consistency Model: Strong Consistency

#### Job State
- PostgreSQL provides ACID guarantees.
- Row-level locks (`SELECT ... FOR UPDATE`) prevent race conditions during worker claim and completion.

#### Distributed Coordination
- etcd uses Raft consensus for linearizable reads and writes.
- Leader election is strongly consistent across scheduler replicas.

### Handling Inconsistencies

#### Scenario: Job in Redis but Not Database
- **Cause**: Database transaction rolled back after Redis message was published.
- **Detection**: Worker receives job ID from Redis, queries PostgreSQL, and receives `sql.ErrNoRows`.
- **Resolution**: Worker logs a warning and acknowledges the Redis message (`XAck`) to purge the orphan from the queue.

#### Scenario: Job Processed Twice
- **Cause**: Worker crashes after processing payload but before acknowledging to Redis.
- **Prevention**:
  1. Distributed lock in etcd prevents another worker from processing concurrently.
  2. Database status validation ensures updates only proceed from `queued` to `running`.
  3. Extractors and handlers should be designed idempotently.

---

## Performance Optimization

### Dispatcher Optimization
- **Batch Dispatching**: Leader queries up to 100 pending jobs in a single batch query:
  ```sql
  SELECT id, payload, priority 
  FROM jobs 
  WHERE status = 'pending'
  ORDER BY priority DESC, created_at ASC 
  LIMIT 100;
  ```
- **Priority Queue Index**:
  ```sql
  CREATE INDEX idx_jobs_dispatch 
  ON jobs(status, priority DESC, created_at ASC) 
  WHERE status = 'pending';
  ```

### Database Connection Pooling
Each pod configures Go `database/sql` pools sized for its Kubernetes resource limits:
```go
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(10)
db.SetConnMaxLifetime(60 * time.Minute)
```

---

## Monitoring & Observability

### Kubernetes Health Endpoints

Each pod exposes `/health`:
```json
{
  "status": "healthy",
  "instance_id": "scheduler-789cf9cb8b-9tz2p",
  "is_leader": true,
  "coordinator_health": true,
  "timestamp": 1705315200
}
```

### Metrics Export
Pods expose `/metrics` returning real-time job processing counters:
- `pending_jobs`, `queued_jobs`, `running_jobs`, `completed_jobs`, `failed_jobs`
- `jobs_dispatched`
- `last_dispatch` timestamp

### Cluster Diagnostic Commands
```bash
# Check workload health
kubectl -n task-scheduler get pods,svc,statefulset,hpa

# Inspect resource consumption
kubectl top pods -n task-scheduler

# Verify leader election
curl http://localhost:8080/leader

# Tail logs of active leader
kubectl -n task-scheduler logs -l app=scheduler --tail=50 -f
```

---

## Extractor Layer

### Purpose
The extractor layer (`pkg/extractor/`) provides a pluggable mechanism for processing job payloads. Workers delegate execution based on payload characteristics rather than hardcoding business logic.

```
Worker receives job
       │
       ▼
┌──────────────────┐
│ Extractor        │
│ Registry         │
└────────┬─────────┘
         │ CanHandle(payload)?
         ▼
┌───────────────────────────────────────┐
│                                       │
▼                    ▼                  ▼
HTTP              Noop               Custom
Extractor         Extractor          Extractor
│                    │                  │
▼                    ▼                  ▼
Make HTTP         Simulate           Domain
Request           Work               Logic
```

### Extractor Interface
```go
type Extractor interface {
    Name() string
    CanHandle(ctx context.Context, payload string) bool
    Extract(ctx context.Context, jobID, payload string) (*Result, error)
}
```

### Built-in Extractors

| Extractor | Purpose | Payload Detection |
|---|---|---|
| `NoopExtractor` | Simulates synthetic latency (default fallback) | Matches any payload |
| `HTTPExtractor` | Dispatches outbound HTTP request | Detects `{"url": "..."}` in payload |

---

## Future Enhancements

1. **KEDA Autoscaling**: Replace CPU-based worker HPA with KEDA Redis Streams lag-based autoscaling.
2. **Kubernetes Gateway / Ingress**: Implement Traefik or Envoy Ingress for TLS termination and rate-limiting on the scheduler REST API.
3. **Job Dependencies (DAGs)**: Add prerequisite job IDs to allow complex pipeline scheduling.
4. **Dead Letter Queue (DLQ)**: Automatically route jobs failing after `max_retries` to a dedicated DLQ stream for manual inspection.
5. **OpenTelemetry Tracing**: Trace job execution context across HTTP submission, Redis Stream dispatch, worker pick-up, and extractor execution.

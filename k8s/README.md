# Kubernetes deployment

This directory deploys the scheduler, workers, PostgreSQL, Redis, and a three-member etcd cluster to a local kind cluster. The repository-root Kustomization packages the database schema rather than requiring a manually-created ConfigMap.

## Deploy to kind

```bash
kind create cluster --name task-scheduler --config kind-config.yaml
docker build -t task-scheduler-scheduler:local -f Dockerfile.scheduler .
docker build -t task-scheduler-worker:local -f Dockerfile.worker .
kind load docker-image task-scheduler-scheduler:local --name task-scheduler
kind load docker-image task-scheduler-worker:local --name task-scheduler
kubectl apply -k .
kubectl -n task-scheduler wait --for=condition=complete job/db-init --timeout=180s
kubectl -n task-scheduler rollout status deployment/scheduler --timeout=180s
kubectl -n task-scheduler rollout status deployment/worker --timeout=180s
```

`POSTGRES_DSN` is intentionally stored in the development Secret, while Redis and etcd locations live in a ConfigMap. The application reaches all dependencies through Kubernetes Service DNS, never `localhost`.

## Verify and demonstrate recovery

```bash
kubectl -n task-scheduler port-forward svc/scheduler 8080:8080
curl http://localhost:8080/health
curl http://localhost:8080/leader
curl -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' -d '{"payload":"test task","priority":5}'
kubectl -n task-scheduler get pods
```

To demonstrate scheduler failover, identify the leader with `/leader`, delete that scheduler Pod, then query `/leader` again after the replacement is ready. To demonstrate etcd recovery, delete `etcd-0`; the StatefulSet preserves its identity and volume.

The CPU-based HPA requires Metrics Server. Install the kind-compatible local add-on with `sh k8s/install-metrics-server-kind.sh`, then confirm it with `kubectl top pods -n task-scheduler`. See [VALIDATION.md](VALIDATION.md) for the reproducible local-test methodology and observed results.

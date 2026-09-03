# Local validation results

Environment: kind cluster with one control-plane and two worker nodes; Kubernetes v1.37; two-second synthetic no-op jobs. These are local validation results, not production SLOs.

| Check | Result |
| --- | --- |
| Database initialization | Completed successfully; schema Job created tables, indexes, triggers, views, and helper functions. |
| End-to-end processing | A submitted job reached `completed` in PostgreSQL with a recorded worker ID. |
| Scheduler failover | Deleting the elected scheduler Pod caused Kubernetes to replace it; a remaining scheduler was elected leader. |
| etcd recovery | Deleting `etcd-0` caused the StatefulSet to recreate it; scheduler health remained true. |
| HPA | After installing Metrics Server, CPU utilization was available. A bounded CPU load scaled workers from 2 to 8 at 66% observed CPU versus a 60% target. |
| Two-worker benchmark | 32 synthetic jobs: 32 completed in 33.04 seconds. |
| Eight-worker benchmark | 32 synthetic jobs: 32 completed in 8.96 seconds, a 3.69x improvement over the two-worker run. |
| Reliability sample | 200 synthetic jobs: 200 completed, 0 failed, 0 outstanding. |
| Dedupe sample | 100 concurrent submissions with one dedupe key created exactly one completed job record. |

## Reproduce the HPA setup

```bash
sh k8s/install-metrics-server-kind.sh
kubectl top pods -n task-scheduler
kubectl -n task-scheduler get hpa
```

The HPA is CPU-based. It demonstrates Kubernetes autoscaling, but production queue-backlog scaling should use an external metric (for example, Redis Streams consumer lag) rather than CPU alone.

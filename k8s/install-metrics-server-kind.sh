#!/usr/bin/env sh
set -eu

# Metrics Server supplies the resource API used by the CPU-based worker HPA.
# kind kubelets use locally generated certificates, so the insecure TLS option
# is required only for this local-development cluster.
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml
kubectl -n kube-system set args deployment/metrics-server --containers=metrics-server -- \
  --cert-dir=/tmp \
  --secure-port=10250 \
  --kubelet-preferred-address-types=InternalIP,ExternalIP,Hostname \
  --kubelet-use-node-status-port \
  --metric-resolution=15s \
  --kubelet-insecure-tls
kubectl -n kube-system rollout status deployment/metrics-server --timeout=180s
kubectl get apiservice v1beta1.metrics.k8s.io

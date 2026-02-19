#!/usr/bin/env bash
set -euo pipefail

cluster_name="${1:-trigger-test}"
namespace="${2:-trigger-k8s-integration}"

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required but was not found in PATH"
  exit 1
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but was not found in PATH"
  exit 1
fi

if ! kind get clusters | grep -qx "${cluster_name}"; then
  ./hosting/k8s/setup-kind.sh "${cluster_name}"
fi

kubectl config use-context "kind-${cluster_name}" >/dev/null
kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "kind cluster ready: ${cluster_name}"
echo "namespace ready: ${namespace}"

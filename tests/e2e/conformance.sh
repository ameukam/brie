#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

mode="${1:-quick}"
case "$mode" in
  quick) wait_minutes=20 ;;
  certified-conformance) wait_minutes=240 ;;
  *) printf 'Unsupported mode: %s\n' "$mode" >&2; exit 2 ;;
esac

for tool in docker kind kubectl sonobuoy jq; do
  command -v "$tool" >/dev/null || { printf 'Missing tool: %s\n' "$tool" >&2; exit 1; }
done

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
artifacts="${ARTIFACTS_DIR:-$root/artifacts/conformance}"
mkdir -p "$artifacts"
artifacts="$(cd -- "$artifacts" && pwd)"
work="$(mktemp -d)"
cluster="brie-$(basename "$work" | tr '[:upper:].' '[:lower:]-')"
export KUBECONFIG="$work/kubeconfig"

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT trap.
cleanup() {
  local status="$1"
  if ! kind export logs "$artifacts/kind" --name "$cluster"; then
    printf 'Failed to collect kind diagnostics.\n' >&2
    status=1
  fi
  if ! kind delete cluster --name "$cluster"; then
    printf 'Failed to delete disposable cluster %s.\n' "$cluster" >&2
    status=1
  fi
  # Remove only the generated credential file, never a user's kubeconfig.
  if ! rm -f -- "$KUBECONFIG" || ! rmdir -- "$work"; then
    printf 'Failed to remove temporary cluster credentials.\n' >&2
    status=1
  fi
  exit "$status"
}
trap 'cleanup "$?"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

{
  printf 'Upstream kind baseline only; not Brie Node Conformance.\nMode: %s\n' "$mode"
  kind version
  sonobuoy version --short
  kubectl version --client -o yaml
} > "$artifacts/versions.txt"

kind create cluster --name "$cluster" --config "$root/tests/e2e/kind.yaml" \
  --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 \
  --wait 180s
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl version -o yaml >> "$artifacts/versions.txt"
kubectl get nodes -o wide >> "$artifacts/versions.txt"

status=0
sonobuoy run --mode "$mode" --plugin e2e \
  --sonobuoy-image docker.io/sonobuoy/sonobuoy:v0.57.5@sha256:7a65a613a53017dfbe674f61890f8f5b16b4a8085286f625eeb42b6ae64c48b8 \
  --kube-conformance-image registry.k8s.io/conformance:v1.37.0@sha256:ac2957464746344fe3e44c4ef24bc03e4161c3abb86213cdf62ae3b5cd48946c \
  --timeout "$((wait_minutes * 60))" --wait="$wait_minutes" --wait-output progress \
  2>&1 | tee "$artifacts/run.log" || status=$?

# Completion is not success: inspect actual test results even if run --wait succeeds.
archive="$(sonobuoy retrieve "$artifacts")"
sonobuoy results "$archive" --plugin e2e > "$artifacts/summary.txt"
cat "$artifacts/summary.txt"
sonobuoy results "$archive" --plugin e2e --mode detailed > "$artifacts/results.jsonl"
jq -s -e --arg mode "$mode" -f "$root/tests/e2e/check-results.jq" \
  "$artifacts/results.jsonl"
exit "$status"

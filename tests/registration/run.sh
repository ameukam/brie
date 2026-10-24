#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

for tool in docker kind kubectl go; do
  command -v "$tool" >/dev/null || { printf 'Missing tool: %s\n' "$tool" >&2; exit 1; }
done
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
artifacts="${ARTIFACTS_DIR:-$root/artifacts/registration}"
mkdir -p "$artifacts"
artifacts="$(cd -- "$artifacts" && pwd)"
work="$(mktemp -d)"
cluster="brie-registration-$(basename "$work" | tr '[:upper:].' '[:lower:]-')"
export KUBECONFIG="$work/kubeconfig"

# shellcheck disable=SC2329 # Invoked by the EXIT trap.
cleanup() {
  local status="$1"
  if ! kind export logs "$artifacts/kind" --name "$cluster"; then
    printf 'Failed to collect kind diagnostics.\n' >&2
    status=1
  fi
  if ! kind delete cluster --name "$cluster"; then
    printf 'Failed to delete registration cluster %s.\n' "$cluster" >&2
    status=1
  fi
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
  kind version
  kubectl version --client -o yaml
  go version
} > "$artifacts/versions.txt"

kind create cluster --name "$cluster" --config "$root/tests/e2e/kind.yaml" \
  --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 \
  --wait 180s
kubectl wait --for=condition=Ready nodes --all --timeout=180s
cd "$root"
BRIE_REGISTRATION_E2E=1 go test -race -tags=registration_e2e \
  -count=1 -timeout=5m -v ./tests/registration 2>&1 | tee "$artifacts/results.txt"

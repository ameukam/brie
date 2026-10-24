#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
[[ $# == 1 && "$1" == /* && ! -e "$1" ]] || {
  echo "Usage: bash $0 /absolute/new-artifact-directory" >&2
  exit 2
}
out="$1"
mkdir -p "$out"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
curl --fail --silent --show-error --location --retry 3 \
  https://github.com/firecracker-microvm/firecracker/releases/download/v1.17.0/firecracker-v1.17.0-aarch64.tgz \
  -o "$out/firecracker.tgz"
curl --fail --silent --show-error --location --retry 3 \
  https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261023-dd7c24b0d165-0/aarch64/vmlinux-6.1.188 \
  -o "$out/vmlinux"
(
  cd "$out"
  printf '%s\n' \
    'e351ebe4f7a16b5873bbd51005d2e6767103cff4d5ebc829df2d3f95a93e2256  firecracker.tgz' \
    '52748cec9d75865246d95906021fea6754bc0abc2cba0cfc792dac5ec7f46f83  vmlinux' \
    | sha256sum --check --strict
  tar -xzf firecracker.tgz release-v1.17.0-aarch64/firecracker-v1.17.0-aarch64
)
mkdir -p "$out/rootfs/sbin" "$out/rootfs/dev"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -tags=brie_guest \
  -o "$out/rootfs/sbin/init" ./examples/gce/guest-init
truncate -s 64M "$out/rootfs.ext4"
mkfs.ext4 -q -F -d "$out/rootfs" "$out/rootfs.ext4"
# Populate guest-only device inodes without creating privileged host devices.
printf 'cd /dev\nmknod console c 5 1\nmknod null c 1 3\n' \
  | debugfs -w -f /dev/stdin "$out/rootfs.ext4"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -tags=integration \
  -o "$out/firecracker.test" ./internal/runtime/internal/owner
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath \
  -o "$out/brie-node" ./cmd/brie-node
(
  cd "$out"
  sha256sum vmlinux rootfs.ext4 firecracker.test brie-node \
    release-v1.17.0-aarch64/firecracker-v1.17.0-aarch64 > SHA256SUMS
)
echo "Artifacts prepared. No VM was launched. Transfer only this synthetic bundle to the approved host."

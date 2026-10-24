#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
[[ $# == 1 && "$1" == /* && -d "$1" ]] || {
  echo "Usage: PROJECT=... ZONE=... NAME=brie-... bash $0 /absolute/prepared-bundle" >&2
  exit 2
}
: "${PROJECT:?Set the approved experiment project}"
: "${ZONE:?Set the approved experiment zone}"
: "${NAME:?Set the experiment name}"
[[ "$NAME" =~ ^brie-[a-z0-9-]{1,30}[a-z0-9]$ ]] || exit 2
[[ "$ZONE" =~ ^[a-z]+-[a-z0-9]+[0-9]-[a-z]$ ]] || exit 2
out="$1"
(
  cd "$out"
  sha256sum --check --strict SHA256SUMS
)
# Only synthetic artifacts are transferred, never a kubeconfig or source tree.
archive="$(mktemp --suffix=.tgz)"
trap 'rm -f -- "$archive"' EXIT
tar -czf "$archive" -C "$out" SHA256SUMS vmlinux rootfs.ext4 firecracker.test \
  brie-node release-v1.17.0-aarch64/firecracker-v1.17.0-aarch64
gcloud compute scp "$archive" "$NAME-worker:/tmp/brie-experiment-boot.tgz" \
  --project="$PROJECT" --zone="$ZONE" --tunnel-through-iap --quiet
gcloud compute ssh "$NAME-worker" --project="$PROJECT" --zone="$ZONE" \
  --tunnel-through-iap --quiet --command='
    set -eu
    sudo test ! -e /var/lib/brie-experiment/boot
    sudo mkdir -m 700 /var/lib/brie-experiment/boot
    sudo tar -xzf /tmp/brie-experiment-boot.tgz -C /var/lib/brie-experiment/boot
    sudo sh -c "
      set -eu
      cd /var/lib/brie-experiment/boot
      sha256sum --check --strict SHA256SUMS
      ./brie-node --version
      BRIE_REAL_VM=1 \
        FIRECRACKER_BINARY=/var/lib/brie-experiment/boot/release-v1.17.0-aarch64/firecracker-v1.17.0-aarch64 \
        GUEST_KERNEL=/var/lib/brie-experiment/boot/vmlinux \
        GUEST_ROOTFS=/var/lib/brie-experiment/boot/rootfs.ext4 \
        ./firecracker.test -test.run=\"^TestRealOwnerBoot$\" -test.v -test.timeout=90s
    "
    rm -f /tmp/brie-experiment-boot.tgz
  '

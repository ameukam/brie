#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
umask 077
trap 'echo "Brie host preflight failed at line $LINENO" >&2' ERR
[[ "$(uname -m)" == aarch64 ]]
[[ -c /dev/kvm && -r /dev/kvm && -w /dev/kvm ]]
mkdir -p /var/lib/brie-experiment
{
  uname -a
  lscpu
  free -h
  ls -l /dev/kvm
  echo 'Host preflight only. Brie is not installed or registered as a Kubernetes node.'
} > /var/lib/brie-experiment/host.txt
touch /var/lib/brie-experiment/host-ready

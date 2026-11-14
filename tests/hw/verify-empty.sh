#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Lists every resource type the lab creates, by run label and by run name
# prefix, and fails unless all of them are empty. A failed list also fails.
set -euo pipefail

HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$HW_DIR/lib.sh"
hw_init verify-empty

remaining=0

check() {
	local kind=$1 output count
	shift
	if ! output=$(hw_gcloud "$@"); then
		remaining=$((remaining + 1))
		printf 'verify-empty: %s: list failed; cannot verify\n' "$kind" >&2
		hw_log verify-empty "$kind list-failed"
		return
	fi
	count=$(grep -c . <<<"$output" || true)
	printf 'verify-empty: %s %d\n' "$kind" "$count"
	hw_log verify-empty "$kind count=$count"
	if ((count > 0)); then
		remaining=$((remaining + count))
		printf 'verify-empty: remaining %s: %s\n' "$kind" "$(tr '\n' ' ' <<<"$output")" >&2
	fi
}

check instances compute instances list --filter="$(hw_label_filter)" --format='value(name,zone.basename())'
check disks compute disks list --filter="$(hw_label_filter)" --format='value(name,zone.basename())'
check buckets storage buckets list --filter="$(hw_label_filter)" --format='value(name)'
check firewall-rules compute firewall-rules list --filter="$(hw_prefix_filter)" --format='value(name)'
check routers compute routers list --filter="$(hw_prefix_filter)" --format='value(name,region.basename())'
check subnets compute networks subnets list --filter="$(hw_prefix_filter)" --format='value(name,region.basename())'
check networks compute networks list --filter="$(hw_prefix_filter)" --format='value(name)'

if ((remaining > 0)); then
	printf 'verify-empty: FAILED: run %s still has resources or could not be listed\n' "$RUN_ID" >&2
	exit 1
fi
printf 'verify-empty: run %s has no remaining resources\n' "$RUN_ID"

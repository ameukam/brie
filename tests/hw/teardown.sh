#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Deletes only resources that carry this run's ownership marker, in dependency
# order, then runs verify-empty.sh. Each candidate is re-described and its
# label (or, for types without labels, its description) is checked immediately
# before deletion. Candidates without the marker are refused, never deleted.
set -euo pipefail

HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$HW_DIR/lib.sh"
hw_init teardown

refused=0
failed=0

refuse() {
	refused=$((refused + 1))
	hw_log refuse "$*"
	printf 'teardown: REFUSED %s\n' "$*" >&2
}

delete() {
	if hw_gcloud "$@"; then
		return 0
	fi
	failed=$((failed + 1))
	printf 'teardown: FAILED %s\n' "$*" >&2
}

# list stores a gcloud list result in listed; a failed list counts as a
# teardown failure because its resources stay unverified.
listed=
list() {
	if ! listed=$(hw_gcloud "$@"); then
		listed=
		failed=$((failed + 1))
		printf 'teardown: FAILED to list: %s\n' "$*" >&2
	fi
}

zoned_by_label() {
	local kind=$1 name zone candidates
	list compute "$kind" list --filter="$(hw_label_filter)" --format='value(name,zone.basename())'
	candidates=$listed
	while read -r name zone; do
		[[ -n $name ]] || continue
		if hw_owned_by_label compute "$kind" describe "$name" --zone="$zone"; then
			delete compute "$kind" delete "$name" --zone="$zone" --quiet
		else
			refuse "$kind $name in $zone: label $HW_MARKER is absent"
		fi
	done <<<"$candidates"
}

buckets() {
	local name candidates
	list storage buckets list --filter="$(hw_label_filter)" --format='value(name)'
	candidates=$listed
	while read -r name; do
		[[ -n $name ]] || continue
		if hw_owned_by_label storage buckets describe "gs://$name"; then
			delete storage rm --recursive "gs://$name"
		else
			refuse "bucket gs://$name: label $HW_MARKER is absent"
		fi
	done <<<"$candidates"
}

firewall_rules() {
	local name candidates
	list compute firewall-rules list --filter="$(hw_prefix_filter)" --format='value(name)'
	candidates=$listed
	while read -r name; do
		[[ -n $name ]] || continue
		if hw_owned_by_description compute firewall-rules describe "$name"; then
			delete compute firewall-rules delete "$name" --quiet
		else
			refuse "firewall rule $name: description is not $HW_MARKER"
		fi
	done <<<"$candidates"
}

routers() {
	local name region nat nats candidates foreign
	list compute routers list --filter="$(hw_prefix_filter)" --format='value(name,region.basename())'
	candidates=$listed
	while read -r name region; do
		[[ -n $name ]] || continue
		if ! hw_owned_by_description compute routers describe "$name" --region="$region"; then
			refuse "router $name in $region and its NAT: description is not $HW_MARKER"
			continue
		fi
		# NATs carry neither labels nor descriptions, so only the NAT name
		# provision.sh records for this run is deleted. Any other NAT under
		# the router is refused, and the router is kept for it.
		list compute routers nats list --router="$name" --region="$region" --format='value(name)'
		nats=$listed
		foreign=0
		while read -r nat; do
			[[ -n $nat ]] || continue
			if [[ $nat != "$HW_PREFIX-nat" ]]; then
				refuse "NAT $nat on router $name in $region: it is not this run's NAT $HW_PREFIX-nat; keeping the router"
				foreign=1
			fi
		done <<<"$nats"
		if ((foreign == 1)); then
			continue
		fi
		while read -r nat; do
			[[ -n $nat ]] || continue
			delete compute routers nats delete "$nat" --router="$name" --region="$region" --quiet
		done <<<"$nats"
		delete compute routers delete "$name" --region="$region" --quiet
	done <<<"$candidates"
}

subnets() {
	local name region candidates
	list compute networks subnets list --filter="$(hw_prefix_filter)" --format='value(name,region.basename())'
	candidates=$listed
	while read -r name region; do
		[[ -n $name ]] || continue
		if hw_owned_by_description compute networks subnets describe "$name" --region="$region"; then
			delete compute networks subnets delete "$name" --region="$region" --quiet
		else
			refuse "subnet $name in $region: description is not $HW_MARKER"
		fi
	done <<<"$candidates"
}

networks() {
	local name candidates
	list compute networks list --filter="$(hw_prefix_filter)" --format='value(name)'
	candidates=$listed
	while read -r name; do
		[[ -n $name ]] || continue
		if hw_owned_by_description compute networks describe "$name"; then
			delete compute networks delete "$name" --quiet
		else
			refuse "network $name: description is not $HW_MARKER"
		fi
	done <<<"$candidates"
}

zoned_by_label instances
zoned_by_label disks
buckets
firewall_rules
routers
subnets
networks

verify=0
"$HW_DIR/verify-empty.sh" || verify=$?
hw_log teardown-result "refused=$refused failed=$failed verify_empty_status=$verify"
if ((refused > 0 || failed > 0 || verify != 0)); then
	printf 'teardown: incomplete: refused=%d failed=%d verify-empty status=%d\n' "$refused" "$failed" "$verify" >&2
	exit 1
fi
printf 'teardown: all resources for run %s deleted and verified empty\n' "$RUN_ID"

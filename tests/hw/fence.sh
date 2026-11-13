#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Lab fence for D5: stops a source host with `gcloud compute instances stop`,
# waits until the same instance incarnation reports TERMINATED, detaches its
# persistent data disk, and prints machine-readable evidence. It acts only on
# an instance labelled for this run and exits nonzero unless every step is
# observed. An accepted stop request alone is not fence evidence.
#
# Usage: fence.sh INSTANCE ZONE
set -euo pipefail

HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$HW_DIR/lib.sh"
hw_init fence

(($# == 2)) || hw_die "usage: fence.sh INSTANCE ZONE"
INSTANCE=$1
FENCE_ZONE=$2
[[ $FENCE_ZONE =~ $HW_ZONE_PATTERN ]] || hw_die "zone $FENCE_ZONE is not a valid zone"
FENCE_TIMEOUT_SECONDS=${FENCE_TIMEOUT_SECONDS:-600}
FENCE_POLL_SECONDS=${FENCE_POLL_SECONDS:-10}
for name in FENCE_TIMEOUT_SECONDS FENCE_POLL_SECONDS; do
	[[ ${!name} =~ ^[1-9][0-9]{0,4}$ ]] || hw_die "$name must be a positive integer"
done
DATA_DISK=$INSTANCE-data

instance_id=
creation_timestamp=
labels=
status=
requested_at=
stop_returned_at=
stop_exit_status=
terminated_observed_at=
disk_detached_at=
disk_users_after=
reason=

json_string() {
	local value=${1//\\/\\\\}
	value=${value//\"/\\\"}
	printf '"%s"' "${value//$'\t'/ }"
}

emit() {
	local verified=$1 evidence
	evidence=$(
		printf '{"schema":"brie.hw.fence/v1","run_id":%s,"project":%s,"zone":%s,"instance":%s,' \
			"$(json_string "$RUN_ID")" "$(json_string "$PROJECT")" "$(json_string "$FENCE_ZONE")" "$(json_string "$INSTANCE")"
		printf '"instance_id":%s,"creation_timestamp":%s,"action":"gcloud compute instances stop",' \
			"$(json_string "$instance_id")" "$(json_string "$creation_timestamp")"
		printf '"requested_at":%s,"stop_returned_at":%s,"stop_exit_status":%s,' \
			"$(json_string "$requested_at")" "$(json_string "$stop_returned_at")" "$(json_string "$stop_exit_status")"
		printf '"terminated_observed_at":%s,"final_status":%s,"data_disk":%s,' \
			"$(json_string "$terminated_observed_at")" "$(json_string "$status")" "$(json_string "$DATA_DISK")"
		printf '"disk_detached_at":%s,"disk_users_after":%s,"verified":%s,"reason":%s,"recorded_at":%s}' \
			"$(json_string "$disk_detached_at")" "$(json_string "$disk_users_after")" "$verified" \
			"$(json_string "$reason")" "$(json_string "$(hw_now_iso)")"
	)
	printf '%s\n' "$evidence" >>"$HW_LOG_DIR/fence-evidence.jsonl"
	printf '%s\n' "$evidence"
	hw_log fence "instance=$INSTANCE verified=$verified reason=$reason"
}

fail() {
	reason=$1
	emit false
	printf 'fence: NOT VERIFIED: %s\n' "$reason" >&2
	exit 1
}

# describe_instance sets instance fields; it fails if the incarnation changed.
describe_instance() {
	local output id created
	output=$(hw_gcloud compute instances describe "$INSTANCE" --zone="$FENCE_ZONE" \
		--format='value(id,creationTimestamp,status,labels)') || return 1
	IFS=$'\t' read -r id created status labels <<<"$output"
	if [[ -n $instance_id && ($id != "$instance_id" || $created != "$creation_timestamp") ]]; then
		fail "instance incarnation changed from $instance_id/$creation_timestamp to $id/$created"
	fi
	instance_id=$id
	creation_timestamp=$created
}

describe_instance || fail "cannot describe instance $INSTANCE in $FENCE_ZONE"
if ! hw_labels_match "$labels"; then
	fail "instance $INSTANCE does not carry label $HW_MARKER; refusing to stop it"
fi
[[ -n $instance_id && -n $creation_timestamp ]] || fail "instance id or creation timestamp is missing"

requested_at=$(hw_now_iso)
stop_exit_status=0
hw_gcloud compute instances stop "$INSTANCE" --zone="$FENCE_ZONE" --quiet || stop_exit_status=$?
stop_returned_at=$(hw_now_iso)

deadline=$(($(hw_now_epoch) + FENCE_TIMEOUT_SECONDS))
while true; do
	describe_instance || fail "cannot describe instance $INSTANCE after stop"
	if [[ $status == TERMINATED ]]; then
		terminated_observed_at=$(hw_now_iso)
		break
	fi
	if (($(hw_now_epoch) >= deadline)); then
		fail "status $status did not reach TERMINATED within $FENCE_TIMEOUT_SECONDS s (stop exit status $stop_exit_status)"
	fi
	sleep "$FENCE_POLL_SECONDS"
done

disk_labels=$(hw_gcloud compute disks describe "$DATA_DISK" --zone="$FENCE_ZONE" --format='value(labels)') ||
	fail "cannot describe data disk $DATA_DISK"
hw_labels_match "$disk_labels" || fail "data disk $DATA_DISK does not carry label $HW_MARKER"
users=$(hw_gcloud compute disks describe "$DATA_DISK" --zone="$FENCE_ZONE" --format='value(users)') ||
	fail "cannot read users of data disk $DATA_DISK"
if [[ $users == *"/instances/$INSTANCE"* ]]; then
	hw_gcloud compute instances detach-disk "$INSTANCE" --zone="$FENCE_ZONE" --disk="$DATA_DISK" ||
		fail "detaching $DATA_DISK failed"
fi
disk_users_after=$(hw_gcloud compute disks describe "$DATA_DISK" --zone="$FENCE_ZONE" --format='value(users)') ||
	fail "cannot verify users of data disk $DATA_DISK"
if [[ $disk_users_after == *"/instances/$INSTANCE"* ]]; then
	fail "data disk $DATA_DISK is still attached to $INSTANCE"
fi
disk_detached_at=$(hw_now_iso)

describe_instance || fail "cannot re-describe instance $INSTANCE after detach"
[[ $status == TERMINATED ]] || fail "instance status changed to $status after detach"

reason="stopped, TERMINATED observed for the same incarnation, data disk detached"
emit true

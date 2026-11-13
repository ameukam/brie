#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Provisions one disposable lab: a dedicated VPC, subnet, firewall rules,
# Cloud Router and NAT, one evidence bucket, up to four c4a-highmem-96-metal
# workload hosts with a persistent data disk each, and up to two
# c4a-standard-8 hosts. Every create is checked against the spend ceiling
# first. Instance creates fall back from ZONE to ZONE2 on stockout; after 45
# minutes of total stockout the run aborts and tears down. A run that already
# has resources is refused before any create. Any failure after this
# invocation created the run's network also runs teardown.sh.
set -euo pipefail

HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$HW_DIR/lib.sh"
hw_init provision
hw_load_prices

METAL_TYPE=c4a-highmem-96-metal
STANDARD_TYPE=c4a-standard-8
MAX_METAL=4
MAX_STANDARD=2
STOCKOUT_CAP_SECONDS=2700
BOOT_DISK_GB=${BOOT_DISK_GB:-100}
DATA_DISK_GB=${DATA_DISK_GB:-200}
SUBNET_RANGE=${SUBNET_RANGE:-10.200.0.0/20}
POD_RANGE=${POD_RANGE:-10.244.0.0/16}
STOCKOUT_RETRY_SECONDS=${STOCKOUT_RETRY_SECONDS:-60}
IMAGE_FAMILY=ubuntu-2404-lts-arm64
IMAGE_PROJECT=ubuntu-os-cloud
IAP_RANGE=35.235.240.0/20

for name in METAL_COUNT STANDARD_COUNT; do
	[[ -n ${!name:-} ]] || hw_die "required input $name is not set; it has no default"
	[[ ${!name} =~ ^[0-9]$ ]] || hw_die "$name must be a small non-negative integer"
done
((METAL_COUNT >= 1 && METAL_COUNT <= MAX_METAL)) || hw_die "METAL_COUNT must be between 1 and $MAX_METAL"
((STANDARD_COUNT <= MAX_STANDARD)) || hw_die "STANDARD_COUNT must be at most $MAX_STANDARD"
for name in BOOT_DISK_GB DATA_DISK_GB STOCKOUT_RETRY_SECONDS; do
	[[ ${!name} =~ ^[1-9][0-9]{0,4}$ ]] || hw_die "$name must be a positive integer"
done
for name in SUBNET_RANGE POD_RANGE; do
	[[ ${!name} =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}/[0-9]{1,2}$ ]] || hw_die "$name must be an IPv4 CIDR"
done

NETWORK=$HW_PREFIX-net
SUBNET=$HW_PREFIX-subnet
ROUTER=$HW_PREFIX-router
NAT=$HW_PREFIX-nat
BUCKET=$HW_PREFIX-$PROJECT
((${#BUCKET} <= 63)) || hw_die "bucket name $BUCKET is longer than 63 characters"
INVENTORY=$HW_LOG_DIR/inventory.tsv
LABELS=$HW_LABEL_KEY=$RUN_ID

# The budget covers PLANNED_HOURS plus the longest provisioning can take
# retrying stockouts, for every resource, from the moment provisioning starts.
# Every instance is created with a GCE termination time at that deadline and
# action DELETE, so the hosts and their data disks are removed by GCE even if
# this script and teardown.sh never run again.
PROVISION_START=$(hw_now_epoch)
HW_BUDGET_HOURS=$(awk -v p="$PLANNED_HOURS" -v s="$((STOCKOUT_CAP_SECONDS + STOCKOUT_RETRY_SECONDS))" \
	'BEGIN { printf "%.17g", p + s / 3600 }')
DEADLINE_EPOCH=$(awk -v t="$PROVISION_START" -v h="$HW_BUDGET_HOURS" \
	'BEGIN { s = h * 3600; i = int(s); if (i < s) i++; printf "%d", t + i }')
DEADLINE=$(date -u -d "@$DEADLINE_EPOCH" +%Y-%m-%dT%H:%M:%SZ)

# Refuse the whole plan before creating anything if it cannot fit.
plan_gib=$((METAL_COUNT * (BOOT_DISK_GB + DATA_DISK_GB) + STANDARD_COUNT * BOOT_DISK_GB))
plan_rate=$(awk -v m="$METAL_COUNT" -v pm="$(hw_price "$METAL_TYPE")" \
	-v s="$STANDARD_COUNT" -v ps="$(hw_price "$STANDARD_TYPE")" \
	-v g="$plan_gib" -v pg="$(hw_price hyperdisk-balanced-gib)" \
	-v n="$(hw_price cloud-nat-gateway)" -v f="$(hw_price fixed-overhead)" \
	'BEGIN { printf "%.17g", m * pm + s * ps + g * pg + n + f }')
plan_usd=$(hw_projected_usd "$plan_rate")
hw_log plan "metal=$METAL_COUNT standard=$STANDARD_COUNT disk_gib=$plan_gib hours=$HW_BUDGET_HOURS deadline=$DEADLINE projected_usd=$plan_usd"
if hw_over_ceiling "$plan_rate"; then
	hw_die "refusing plan: projected spend $plan_usd USD over $HW_BUDGET_HOURS h exceeds ceiling $SPEND_CEILING_USD USD"
fi
printf 'provision: plan projects %s USD over %s h, PLANNED_HOURS plus the stockout allowance (ceiling %s USD); GCE deletes the hosts at %s\n' \
	"$plan_usd" "$HW_BUDGET_HOURS" "$SPEND_CEILING_USD" "$DEADLINE"

# Teardown is armed only after this invocation has created the run's network,
# its exclusive claim on the run: names are fixed per run, so a concurrent or
# repeated provision.sh fails that create and exits without deleting
# anything. Before the claim, a run that already has resources is refused.
claimed=0
finished=0
on_exit() {
	local status=$?
	if ((claimed == 1 && finished == 0)); then
		hw_log abort "provision failed with status $status; running teardown"
		printf 'provision: failed; running teardown for run %s\n' "$RUN_ID" >&2
		"$HW_DIR/teardown.sh" || printf 'provision: TEARDOWN INCOMPLETE; inspect run %s manually\n' "$RUN_ID" >&2
	fi
	exit "$status"
}
trap on_exit EXIT

create() {
	hw_gcloud "$@"
}

stockout_since=
stockout_total=0

is_stockout() {
	grep -Eq 'ZONE_RESOURCE_POOL_EXHAUSTED|does not have enough resources available|stockout' <<<"$1"
}

stockout_elapsed() {
	local open=0
	if [[ -n $stockout_since ]]; then
		open=$(($(hw_now_epoch) - stockout_since))
	fi
	printf '%d' $((stockout_total + open))
}

check_stockout_cap() {
	local elapsed
	elapsed=$(stockout_elapsed)
	if ((elapsed >= STOCKOUT_CAP_SECONDS)); then
		hw_die "stockout cap reached: $elapsed s of stockout in $ZONE and $ZONE2 (cap $STOCKOUT_CAP_SECONDS s); aborting"
	fi
}

# create_instance NAME ROLE MACHINE_TYPE NIC_TYPE sets created_zone.
created_zone=
create_instance() {
	local name=$1 role=$2 machine_type=$3 nic_type=$4 zone output status
	while true; do
		for zone in "$ZONE" "$ZONE2"; do
			if (($(hw_now_epoch) >= DEADLINE_EPOCH)); then
				hw_die "budget deadline $DEADLINE reached before instance $name was created; aborting"
			fi
			status=0
			output=$(create compute instances create "$name" --zone="$zone" \
				--machine-type="$machine_type" \
				--image-family="$IMAGE_FAMILY" --image-project="$IMAGE_PROJECT" \
				--boot-disk-type=hyperdisk-balanced --boot-disk-size="${BOOT_DISK_GB}GB" \
				--network-interface="subnet=$SUBNET,no-address,nic-type=$nic_type" \
				--can-ip-forward --maintenance-policy=TERMINATE \
				--termination-time="$DEADLINE" --instance-termination-action=DELETE \
				--no-service-account --no-scopes \
				--metadata=enable-oslogin=TRUE \
				--labels="$LABELS,brie-hw-role=$role" 2>&1) || status=$?
			if ((status == 0)); then
				if [[ -n $stockout_since ]]; then
					stockout_total=$((stockout_total + $(hw_now_epoch) - stockout_since))
					stockout_since=
				fi
				created_zone=$zone
				return 0
			fi
			if ! is_stockout "$output"; then
				printf '%s\n' "$output" >&2
				hw_die "creating instance $name in $zone failed"
			fi
			hw_log stockout "$name zone=$zone"
			printf 'provision: stockout for %s in %s\n' "$name" "$zone" >&2
			if [[ -z $stockout_since ]]; then
				stockout_since=$(hw_now_epoch)
			fi
			check_stockout_cap
		done
		sleep "$STOCKOUT_RETRY_SECONDS"
		check_stockout_cap
	done
}

add_host() {
	local name=$1 role=$2 machine_type=$3 nic_type=$4
	hw_budget_reserve "$machine_type" 1 "instance $name"
	hw_budget_reserve hyperdisk-balanced-gib "$BOOT_DISK_GB" "boot disk $name"
	create_instance "$name" "$role" "$machine_type" "$nic_type"
	local zone=$created_zone
	create compute disks add-labels "$name" --zone="$zone" --labels="$LABELS,brie-hw-role=boot"
	if [[ $role == metal ]]; then
		hw_budget_reserve hyperdisk-balanced-gib "$DATA_DISK_GB" "data disk $name-data"
		create compute disks create "$name-data" --zone="$zone" --type=hyperdisk-balanced \
			--size="${DATA_DISK_GB}GB" --labels="$LABELS,brie-hw-role=data"
		create compute instances attach-disk "$name" --zone="$zone" --disk="$name-data" --device-name=brie-data
		# Deleted with its instance at the deadline unless fence.sh detached it.
		create compute instances set-disk-auto-delete "$name" --zone="$zone" --disk="$name-data" --auto-delete
	fi
	printf '%s\t%s\t%s\n' "$name" "$role" "$zone" >>"$INVENTORY"
	hw_log inventory "$name role=$role zone=$zone"
}

[[ ! -e $INVENTORY ]] || hw_die "inventory $INVENTORY already exists; use a new RUN_ID or HW_LOG_DIR"
if ! "$HW_DIR/verify-empty.sh" >/dev/null; then
	hw_die "run $RUN_ID already has resources in $PROJECT, or they could not be listed; refusing to provision or clean up"
fi
hw_budget_reserve fixed-overhead 1 "fixed overhead"
create compute networks create "$NETWORK" --subnet-mode=custom --description="$HW_MARKER" ||
	hw_die "could not create network $NETWORK; another provision may own run $RUN_ID, so nothing is cleaned up"
claimed=1
hw_log claim "network $NETWORK"
create compute networks subnets create "$SUBNET" --network="$NETWORK" --region="$HW_REGION" \
	--range="$SUBNET_RANGE" --enable-private-ip-google-access --description="$HW_MARKER"
create compute firewall-rules create "$HW_PREFIX-internal" --network="$NETWORK" --direction=INGRESS \
	--allow=tcp,udp,icmp --source-ranges="$SUBNET_RANGE,$POD_RANGE" --description="$HW_MARKER"
create compute firewall-rules create "$HW_PREFIX-iap-ssh" --network="$NETWORK" --direction=INGRESS \
	--allow=tcp:22 --source-ranges="$IAP_RANGE" --description="$HW_MARKER"
create compute routers create "$ROUTER" --network="$NETWORK" --region="$HW_REGION" --description="$HW_MARKER"
hw_budget_reserve cloud-nat-gateway 1 "Cloud NAT $NAT"
create compute routers nats create "$NAT" --router="$ROUTER" --region="$HW_REGION" \
	--auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges

lifecycle=$HW_LOG_DIR/bucket-lifecycle.json
printf '{"rule":[{"action":{"type":"Delete"},"condition":{"age":7}}]}\n' >"$lifecycle"
create storage buckets create "gs://$BUCKET" --location="$HW_REGION" --uniform-bucket-level-access \
	--public-access-prevention --default-storage-class=STANDARD
create storage buckets update "gs://$BUCKET" --update-labels="$LABELS" --lifecycle-file="$lifecycle"

for ((i = 1; i <= STANDARD_COUNT; i++)); do
	add_host "$HW_PREFIX-std-$i" standard "$STANDARD_TYPE" GVNIC
done
for ((i = 1; i <= METAL_COUNT; i++)); do
	add_host "$HW_PREFIX-metal-$i" metal "$METAL_TYPE" IDPF
done

finished=1
hw_log provisioned "inventory=$INVENTORY committed_usd_per_hour=$HW_COMMITTED_USD_PER_HOUR"
printf 'provision: run %s provisioned; inventory in %s\n' "$RUN_ID" "$INVENTORY"

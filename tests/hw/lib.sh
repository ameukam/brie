# shellcheck shell=bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Shared guards for the Brie hardware lab scripts. Source this file; do not
# execute it. Every cloud call goes through hw_gcloud, which passes the
# operator-named project explicitly and never reads or changes the gcloud configuration.

HW_LABEL_KEY=brie-hw-run
HW_RUN_ID_PATTERN='^[a-z][a-z0-9]{2,15}$'
HW_PROJECT_PATTERN='^[a-z][a-z0-9-]{4,28}[a-z0-9]$'
HW_ZONE_PATTERN='^[a-z]+-[a-z]+[0-9]+-[a-z]$'
HW_USD_PATTERN='^[0-9]+(\.[0-9]+)?$'
# Keys every operator-supplied price table must define, in USD per unit-hour.
HW_PRICE_KEYS=(c4a-highmem-96-metal c4a-standard-8 hyperdisk-balanced-gib cloud-nat-gateway fixed-overhead)

hw_die() {
	printf '%s: %s\n' "${HW_SCRIPT:-brie-hw}" "$*" >&2
	if [[ -n ${HW_ACTION_LOG:-} ]]; then
		hw_log abort "$*"
	fi
	exit 1
}

hw_now_epoch() {
	date -u +%s
}

hw_now_iso() {
	date -u +%Y-%m-%dT%H:%M:%SZ
}

# hw_log appends one tab-separated record; the log is opened append-only and
# is never truncated or rewritten by these scripts.
hw_log() {
	local event=$1
	shift
	printf '%s\t%s\t%s\t%s\n' "$(hw_now_iso)" "$HW_SCRIPT" "$event" "$*" >>"$HW_ACTION_LOG"
}

# hw_init validates the required inputs. PROJECT, ZONE, ZONE2, RUN_ID, and
# SPEND_CEILING_USD have no defaults and are never read from the gcloud configuration.
hw_init() {
	HW_SCRIPT=$1
	if [[ ${APPROVE_COSTS:-} != yes ]]; then
		hw_die "refusing to run: set APPROVE_COSTS=yes to approve spending in the named project"
	fi
	local name
	for name in PROJECT ZONE ZONE2 RUN_ID SPEND_CEILING_USD; do
		if [[ -z ${!name:-} ]]; then
			hw_die "required input $name is not set; it has no default"
		fi
	done
	[[ $PROJECT =~ $HW_PROJECT_PATTERN ]] || hw_die "PROJECT $PROJECT is not a valid project ID"
	[[ $ZONE =~ $HW_ZONE_PATTERN ]] || hw_die "ZONE $ZONE is not a valid zone"
	[[ $ZONE2 =~ $HW_ZONE_PATTERN ]] || hw_die "ZONE2 $ZONE2 is not a valid zone"
	[[ $ZONE != "$ZONE2" ]] || hw_die "ZONE2 must differ from ZONE"
	[[ ${ZONE%-*} == "${ZONE2%-*}" ]] || hw_die "ZONE and ZONE2 must be in the same region"
	[[ $RUN_ID =~ $HW_RUN_ID_PATTERN ]] || hw_die "RUN_ID $RUN_ID must match $HW_RUN_ID_PATTERN"
	[[ $SPEND_CEILING_USD =~ $HW_USD_PATTERN ]] || hw_die "SPEND_CEILING_USD must be a positive USD amount"
	hw_greater "$SPEND_CEILING_USD" 0 || hw_die "SPEND_CEILING_USD must be greater than zero"

	HW_PREFIX=brie-hw-$RUN_ID
	HW_MARKER=$HW_LABEL_KEY=$RUN_ID
	HW_REGION=${ZONE%-*}
	HW_LOG_DIR=${HW_LOG_DIR:-artifacts/hw/$RUN_ID}
	mkdir -p "$HW_LOG_DIR"
	HW_ACTION_LOG=$HW_LOG_DIR/actions.log
	[[ ! -L $HW_ACTION_LOG ]] || hw_die "action log $HW_ACTION_LOG must not be a symlink"
	# Any gcloud call that somehow lacked --project would still use this
	# project rather than the active configuration's default.
	export CLOUDSDK_CORE_PROJECT=$PROJECT
	hw_log start "project=$PROJECT region=$HW_REGION zone=$ZONE zone2=$ZONE2 run=$RUN_ID ceiling_usd=$SPEND_CEILING_USD"
}

# hw_gcloud runs gcloud with the project passed explicitly as the first flag,
# so it also precedes any "--" passthrough arguments.
hw_gcloud() {
	local arg
	for arg in "$@"; do
		case $arg in
		--project | --project=*) hw_die "internal error: the project is passed only by hw_gcloud" ;;
		--) break ;;
		esac
	done
	if [[ ${1:-} == config ]]; then
		hw_die "internal error: the config command group is never used"
	fi
	hw_log gcloud "--project=$PROJECT $*"
	local status=0
	gcloud --project="$PROJECT" "$@" || status=$?
	hw_log gcloud-exit "$status"
	return "$status"
}

# hw_labels_match reports whether a gcloud value(labels) rendering, such as
# "brie-hw-run=r1;role=metal", carries this run's label.
hw_labels_match() {
	local items item
	IFS=';' read -r -a items <<<"$1"
	for item in "${items[@]}"; do
		if [[ $item == "$HW_MARKER" ]]; then
			return 0
		fi
	done
	return 1
}

# hw_owned_by_label re-describes a labelled resource and succeeds only when it
# carries this run's label. Arguments are the gcloud describe command.
hw_owned_by_label() {
	local labels
	labels=$(hw_gcloud "$@" --format='value(labels)') || return 1
	hw_labels_match "$labels"
}

# hw_owned_by_description is the ownership check for resource types without
# labels (networks, subnets, firewall rules, and routers): their description
# must be exactly the run marker.
hw_owned_by_description() {
	local description
	description=$(hw_gcloud "$@" --format='value(description)') || return 1
	[[ $description == "$HW_MARKER" ]]
}

# hw_label_filter matches resources by run label or by run name prefix.
hw_label_filter() {
	printf 'labels.%s=%s OR name ~ "^%s-"' "$HW_LABEL_KEY" "$RUN_ID" "$HW_PREFIX"
}

hw_prefix_filter() {
	printf 'name ~ "^%s-"' "$HW_PREFIX"
}

hw_greater() {
	awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 > b + 0) }'
}

# hw_load_prices validates the operator-supplied PRICE_TABLE. Each non-comment
# line is "<key> <usd-per-unit-hour>". There is no built-in price list.
hw_load_prices() {
	[[ -n ${PRICE_TABLE:-} ]] || hw_die "required input PRICE_TABLE is not set; supply an hourly price table"
	[[ -f $PRICE_TABLE && -r $PRICE_TABLE ]] || hw_die "price table $PRICE_TABLE is missing or unreadable"
	[[ -n ${PLANNED_HOURS:-} ]] || hw_die "required input PLANNED_HOURS is not set"
	if [[ ! $PLANNED_HOURS =~ $HW_USD_PATTERN ]] || ! hw_greater "$PLANNED_HOURS" 0; then
		hw_die "PLANNED_HOURS must be a positive number"
	fi
	local key value count
	for key in "${HW_PRICE_KEYS[@]}"; do
		count=$(awk -v k="$key" '$1 == k { n++ } END { print n + 0 }' "$PRICE_TABLE")
		[[ $count == 1 ]] || hw_die "price table must define $key exactly once"
		value=$(awk -v k="$key" '$1 == k { print $2 }' "$PRICE_TABLE")
		[[ $value =~ $HW_USD_PATTERN ]] || hw_die "price for $key is not a USD amount: $value"
	done
	HW_COMMITTED_USD_PER_HOUR=0
	HW_BUDGET_HOURS=$PLANNED_HOURS
}

hw_price() {
	awk -v k="$1" '$1 == k { print $2 }' "$PRICE_TABLE"
}

# hw_projected_usd prints, rounded up to the cent, the spend projected over
# HW_BUDGET_HOURS for an hourly rate. It is for display; decisions use
# hw_over_ceiling on unrounded values.
hw_projected_usd() {
	awk -v r="$1" -v h="$HW_BUDGET_HOURS" 'BEGIN { c = r * h * 100; i = int(c); if (i < c) i++; printf "%.2f", i / 100 }'
}

# hw_over_ceiling succeeds when an hourly rate held for HW_BUDGET_HOURS
# exceeds SPEND_CEILING_USD, comparing unrounded amounts.
hw_over_ceiling() {
	awk -v r="$1" -v h="$HW_BUDGET_HOURS" -v c="$SPEND_CEILING_USD" 'BEGIN { exit !(r * h > c + 0) }'
}

# hw_budget_reserve refuses a create when the committed hourly rate plus this
# resource, held for HW_BUDGET_HOURS, would exceed SPEND_CEILING_USD.
hw_budget_reserve() {
	local key=$1 units=$2 what=$3 rate projected
	rate=$(awk -v c="$HW_COMMITTED_USD_PER_HOUR" -v p="$(hw_price "$key")" -v u="$units" \
		'BEGIN { printf "%.17g", c + p * u }')
	projected=$(hw_projected_usd "$rate")
	if hw_over_ceiling "$rate"; then
		hw_die "refusing to create $what: projected spend $projected USD over $HW_BUDGET_HOURS h exceeds ceiling $SPEND_CEILING_USD USD"
	fi
	HW_COMMITTED_USD_PER_HOUR=$rate
	hw_log budget "$what key=$key units=$units projected_usd=$projected"
}

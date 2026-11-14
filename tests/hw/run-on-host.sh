#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Copies a host-side lab script, its shared helpers, and input files to one
# instance labelled for this run, then runs the script there as root through
# IAP SSH. It refuses instances without the run label.
#
# Usage: run-on-host.sh INSTANCE ZONE SCRIPT [INPUT_FILE...]
set -euo pipefail

HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$HW_DIR/lib.sh"
hw_init run-on-host

(($# >= 3)) || hw_die "usage: run-on-host.sh INSTANCE ZONE SCRIPT [INPUT_FILE...]"
INSTANCE=$1
HOST_ZONE=$2
SCRIPT=$3
shift 3
[[ $HOST_ZONE =~ $HW_ZONE_PATTERN ]] || hw_die "zone $HOST_ZONE is not a valid zone"
case $SCRIPT in
install-host.sh | install-control-plane.sh | node-conformance.sh) ;;
*) hw_die "$SCRIPT is not a host-side lab script" ;;
esac
for file in "$@"; do
	[[ -f $file ]] || hw_die "input file $file does not exist"
	[[ $(basename "$file") =~ ^[A-Za-z0-9._-]+$ ]] || hw_die "input file name $file must be a plain name"
done

if ! hw_owned_by_label compute instances describe "$INSTANCE" --zone="$HOST_ZONE"; then
	hw_die "instance $INSTANCE in $HOST_ZONE does not carry label $HW_MARKER; refusing to run on it"
fi

remote_dir=/tmp/brie-hw-$RUN_ID
remote_args=()
for file in "$@"; do
	remote_args+=("$remote_dir/$(basename "$file")")
done
hw_gcloud compute ssh "$INSTANCE" --zone="$HOST_ZONE" --tunnel-through-iap \
	--command="mkdir -p -m 0700 $remote_dir"
hw_gcloud compute scp --zone="$HOST_ZONE" --tunnel-through-iap \
	"$HW_DIR/$SCRIPT" "$HW_DIR/checksums.sh" "$@" "$INSTANCE:$remote_dir/"
hw_gcloud compute ssh "$INSTANCE" --zone="$HOST_ZONE" --tunnel-through-iap \
	--command="sudo bash $remote_dir/$SCRIPT ${remote_args[*]}"

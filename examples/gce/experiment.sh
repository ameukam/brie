#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

usage() {
  echo "Usage: PROJECT=... ZONE=us-east5-b NAME=brie-... bash $0 {create|status|delete}" >&2
  exit 2
}
[[ $# == 1 ]] || usage
: "${PROJECT:?Set the dedicated experiment project}"
: "${ZONE:?Set the approved zone}"
: "${NAME:?Set a unique brie-prefixed experiment name}"
[[ "$NAME" =~ ^brie-[a-z0-9-]{1,30}[a-z0-9]$ ]] || usage
[[ "$ZONE" =~ ^[a-z]+-[a-z0-9]+[0-9]-[a-z]$ ]] || usage
region="${ZONE%-*}"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
gce=(gcloud --project="$PROJECT" --quiet)

case "$1" in
  create)
    : "${APPROVE_COSTS:?Set APPROVE_COSTS=yes after reviewing the four-hour billable experiment}"
    [[ "$APPROVE_COSTS" == yes ]] || usage
    # An absolute expiration cannot be extended by restarting the instance.
    expires="$(date -u -d '+4 hours' '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'Creating %s in %s; VM deletion deadline %s. Network/NAT require explicit teardown.\n' "$NAME" "$ZONE" "$expires"
    # Unique resource names deliberately fail on collisions; never adopt existing resources.
    "${gce[@]}" compute networks create "$NAME" --subnet-mode=custom
    "${gce[@]}" compute networks subnets create "$NAME" --network="$NAME" \
      --region="$region" --range=10.203.0.0/24
    "${gce[@]}" compute firewall-rules create "$NAME-iap" --network="$NAME" \
      --allow=tcp:22 --source-ranges=35.235.240.0/20 --target-tags="$NAME"
    "${gce[@]}" compute firewall-rules create "$NAME-api" --network="$NAME" \
      --allow=tcp:6443 --source-tags="$NAME-worker" --target-tags="$NAME-control"
    "${gce[@]}" compute routers create "$NAME" --network="$NAME" --region="$region"
    "${gce[@]}" compute routers nats create "$NAME" --router="$NAME" \
      --region="$region" --nat-custom-subnet-ip-ranges="$NAME" --auto-allocate-nat-external-ips
    common=(--zone="$ZONE" --subnet="$NAME" --no-address --no-service-account --no-scopes
      "--metadata=enable-oslogin=true,block-project-ssh-keys=true"
      --labels="brie-experiment=$NAME" --termination-time="$expires"
      --instance-termination-action=DELETE --no-restart-on-failure
      --boot-disk-auto-delete)
    # Test scarce bare-metal allocation before starting the control plane.
    "${gce[@]}" compute instances create "$NAME-worker" "${common[@]}" \
      --machine-type=c4a-highmem-96-metal --tags="$NAME,$NAME-worker" \
      --maintenance-policy=TERMINATE \
      --image=ubuntu-2404-noble-arm64-v20261021 --image-project=ubuntu-os-cloud \
      --boot-disk-type=hyperdisk-balanced --boot-disk-size=100GB \
      --metadata-from-file="startup-script=$root/worker.sh"
    "${gce[@]}" compute instances create "$NAME-control" "${common[@]}" \
      --machine-type=e2-standard-4 --tags="$NAME,$NAME-control" \
      --maintenance-policy=MIGRATE \
      --image=ubuntu-2404-noble-amd64-v20261021 --image-project=ubuntu-os-cloud \
      --boot-disk-type=pd-balanced --boot-disk-size=40GB \
      --metadata-from-file="startup-script=$root/control-plane.sh,brie-kubeadm=$root/kubeadm.yaml,brie-addons=$root/addons.sh,brie-features=$root/features.sh"
    ;;
  status)
    "${gce[@]}" compute instances list --filter="labels.brie-experiment=$NAME" \
      --format='table(name,zone.basename(),machineType.basename(),status,scheduling.terminationTime)'
    ;;
  delete)
    : "${APPROVE_DELETE:?Set APPROVE_DELETE=yes to delete this named experiment only}"
    [[ "$APPROVE_DELETE" == yes ]] || usage
    # Not-found is explicitly distinguished from authorization/API failures.
    instances="$("${gce[@]}" compute instances list --zones="$ZONE" \
      --filter="labels.brie-experiment=$NAME" --format='value(name)')"
    for role in worker control; do
      if grep -Fxq "$NAME-$role" <<< "$instances"; then
        "${gce[@]}" compute instances delete "$NAME-$role" --zone="$ZONE"
      fi
    done
    routers="$("${gce[@]}" compute routers list --filter="name=$NAME AND region:$region" --format='value(name)')"
    if grep -Fxq "$NAME" <<< "$routers"; then
      nats="$("${gce[@]}" compute routers nats list --router="$NAME" --region="$region" --format='value(name)')"
      if grep -Fxq "$NAME" <<< "$nats"; then
        "${gce[@]}" compute routers nats delete "$NAME" --router="$NAME" --region="$region"
      fi
      "${gce[@]}" compute routers delete "$NAME" --region="$region"
    fi
    rules="$("${gce[@]}" compute firewall-rules list --filter="network:$NAME" --format='value(name)')"
    for rule in "$NAME-api" "$NAME-iap"; do
      if grep -Fxq "$rule" <<< "$rules"; then
        "${gce[@]}" compute firewall-rules delete "$rule"
      fi
    done
    subnets="$("${gce[@]}" compute networks subnets list --filter="name=$NAME AND region:$region" --format='value(name)')"
    if grep -Fxq "$NAME" <<< "$subnets"; then
      "${gce[@]}" compute networks subnets delete "$NAME" --region="$region"
    fi
    networks="$("${gce[@]}" compute networks list --filter="name=$NAME" --format='value(name)')"
    if grep -Fxq "$NAME" <<< "$networks"; then
      "${gce[@]}" compute networks delete "$NAME"
    fi
    ;;
  *) usage ;;
esac

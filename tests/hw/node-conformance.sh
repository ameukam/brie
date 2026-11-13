#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Runs the upstream Kubernetes [NodeConformance] tests from the pinned
# kubernetes-test release against the lab cluster and publishes every result,
# including every failure. There are no skip filters. Run it as root on the
# control-plane host (for example through run-on-host.sh) with NODE_NAME set
# to the Brie node. The kubeadm control-plane taint must still be present so
# that test Pods cannot silently run on the kubelet-managed control plane.
#
# Usage:
#   node-conformance.sh CHECKSUMS
#   node-conformance.sh --check-only CHECKSUMS
#   node-conformance.sh --summarize REPORT_DIR
set -euo pipefail

HW_SCRIPT='node-conformance'
HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=checksums.sh
source "$HW_DIR/checksums.sh"

KUBECONFIG_PATH=/etc/kubernetes/admin.conf
EVIDENCE_ROOT=${EVIDENCE_DIR:-/var/lib/brie-hw/evidence}
FOCUS='\[NodeConformance\]'
BRIE_TAINT_KEY=brie.dev/runtime

usage() {
	hw_checksums_die "usage: node-conformance.sh CHECKSUMS | --check-only CHECKSUMS | --summarize REPORT_DIR"
}

load_checksums() {
	hw_checksums_load "$1"
	hw_checksums_require kubernetes-test
	K8S_VERSION=${HW_ARTIFACT_VERSION[kubernetes-test]}
	[[ $K8S_VERSION =~ ^1\.37\.[0-9]+$ ]] || hw_checksums_die "kubernetes-test must be a 1.37 release, not $K8S_VERSION"
}

# summarize writes summary.txt and failures.txt from every JUnit file in a
# report directory. It lists all failures, never a sample, and fails if any
# test failed or errored or if no test ran.
summarize() {
	local dir=$1
	[[ -d $dir ]] || hw_checksums_die "report directory $dir does not exist"
	python3 - "$dir" <<'PY'
import glob
import os
import sys
import xml.etree.ElementTree as ET

directory = sys.argv[1]
files = sorted(glob.glob(os.path.join(directory, "junit*.xml")))
if not files:
    sys.exit(f"node-conformance: no JUnit report in {directory}")
total = passed = skipped = 0
failures = []
for path in files:
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as err:
        sys.exit(f"node-conformance: cannot parse {path}: {err}")
    for case in root.iter("testcase"):
        total += 1
        name = " ".join(case.get("name", "").split())
        bad = case.find("failure")
        if bad is None:
            bad = case.find("error")
        if bad is not None:
            message = (bad.get("message") or bad.text or "").strip().splitlines()
            failures.append((name, message[0] if message else ""))
        elif case.find("skipped") is not None:
            skipped += 1
        else:
            passed += 1
with open(os.path.join(directory, "failures.txt"), "w") as out:
    for name, message in failures:
        out.write(f"FAIL\t{name}\t{message}\n")
summary = f"total {total}\npassed {passed}\nfailed {len(failures)}\nskipped {skipped}\n"
with open(os.path.join(directory, "summary.txt"), "w") as out:
    out.write(summary)
sys.stdout.write(summary)
for name, message in failures:
    sys.stdout.write(f"FAIL\t{name}\t{message}\n")
if total == 0:
    sys.exit("node-conformance: the report contains no tests")
sys.exit(1 if failures else 0)
PY
}

case ${1:-} in
--check-only)
	(($# == 2)) || usage
	load_checksums "$2"
	printf 'node-conformance: %s pins kubernetes-test %s\n' "$2" "$K8S_VERSION"
	exit 0
	;;
--summarize)
	(($# == 2)) || usage
	summarize "$2"
	exit 0
	;;
'' | -*) usage ;;
esac
(($# == 1)) || usage
load_checksums "$1"
[[ -n ${NODE_NAME:-} ]] || hw_checksums_die "required input NODE_NAME is not set"
[[ $EUID -eq 0 ]] || hw_checksums_die "must run as root on the control-plane host"
[[ -r $KUBECONFIG_PATH ]] || hw_checksums_die "$KUBECONFIG_PATH is not readable"

k() {
	kubectl --kubeconfig="$KUBECONFIG_PATH" --request-timeout=60s "$@"
}

server=$(k version -o json | python3 -c 'import json, sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')
[[ $server == "v$K8S_VERSION" ]] || hw_checksums_die "server $server does not match kubernetes-test v$K8S_VERSION"
taints=$(k get node "$NODE_NAME" -o 'jsonpath={.spec.taints[*].key}')
[[ " $taints " == *" $BRIE_TAINT_KEY "* ]] || hw_checksums_die "$NODE_NAME lacks the $BRIE_TAINT_KEY taint; is it the Brie node?"
for node in $(k get nodes -o 'jsonpath={.items[*].metadata.name}'); do
	[[ $node != "$NODE_NAME" ]] || continue
	node_taints=$(k get node "$node" -o 'jsonpath={.spec.taints[*].key}')
	[[ " $node_taints " == *" node-role.kubernetes.io/control-plane "* ]] ||
		hw_checksums_die "node $node is schedulable for test Pods; results would not describe Brie"
done

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
hw_fetch kubernetes-test "$work/kubernetes-test.tar.gz"
tar -C "$work" -xzf "$work/kubernetes-test.tar.gz" kubernetes/test/bin/e2e.test

out=$EVIDENCE_ROOT/node-conformance-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$out"
{
	printf 'kubernetes-test %s %s %s\n' "$K8S_VERSION" "${HW_ARTIFACT_URL[kubernetes-test]}" "${HW_ARTIFACT_SHA[kubernetes-test]}"
	printf 'server %s\nbrie-node %s\nfocus %s\nskip none\n' "$server" "$NODE_NAME" "$FOCUS"
	k get nodes -o wide
} >"$out/inputs.txt"

status=0
# The Brie runtime taint must not block suite startup; it is not a skip filter.
"$work/kubernetes/test/bin/e2e.test" \
	--kubeconfig="$KUBECONFIG_PATH" --provider=skeleton \
	--report-dir="$out" \
	--non-blocking-taints="node-role.kubernetes.io/control-plane,$BRIE_TAINT_KEY" \
	--ginkgo.focus="$FOCUS" --ginkgo.timeout=6h --ginkgo.no-color --ginkgo.v \
	>"$out/e2e.log" 2>&1 || status=$?
printf 'e2e.test exit status %d\n' "$status" >>"$out/inputs.txt"

summary_status=0
summarize "$out" || summary_status=$?
sums=$(cd "$out" && find . -type f -print0 | sort -z | xargs -0 sha256sum)
printf '%s\n' "$sums" >"$out/SHA256SUMS"
printf 'node-conformance: results in %s; publish every line of failures.txt\n' "$out"
if ((status != 0 || summary_status != 0)); then
	exit 1
fi

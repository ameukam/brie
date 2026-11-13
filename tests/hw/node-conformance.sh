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
#   NODE_NAME=NODE node-conformance.sh --check-nodes NODES_JSON
#   NODE_NAME=NODE node-conformance.sh --check-placement PLACEMENT_FILE
set -euo pipefail

HW_SCRIPT='node-conformance'
HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=checksums.sh
source "$HW_DIR/checksums.sh"

KUBECONFIG_PATH=/etc/kubernetes/admin.conf
EVIDENCE_ROOT=${EVIDENCE_DIR:-/var/lib/brie-hw/evidence}
FOCUS='\[NodeConformance\]'
FOCUS_TAG='[NodeConformance]'
CONTROL_PLANE_TAINT_KEY=node-role.kubernetes.io/control-plane
BRIE_TAINT_KEY=brie.dev/runtime

usage() {
	hw_checksums_die "usage: node-conformance.sh CHECKSUMS | --check-only CHECKSUMS | --summarize REPORT_DIR | --check-nodes NODES_JSON | --check-placement PLACEMENT_FILE"
}

load_checksums() {
	hw_checksums_load "$1"
	hw_checksums_require kubernetes-test
	K8S_VERSION=${HW_ARTIFACT_VERSION[kubernetes-test]}
	[[ $K8S_VERSION =~ ^1\.37\.[0-9]+$ ]] || hw_checksums_die "kubernetes-test must be a 1.37 release, not $K8S_VERSION"
}

# summarize writes summary.txt, failures.txt, and skipped.txt from every JUnit
# file in a report directory. Selected cases carry the focus tag; the other
# cases are suite nodes and specs the focus filtered out. It lists all
# failures, never a sample. It exits 1 if any case failed or errored or no
# selected case ran, and 2 (incomplete) if a selected case was skipped.
summarize() {
	local dir=$1
	[[ -d $dir ]] || hw_checksums_die "report directory $dir does not exist"
	python3 - "$dir" "$FOCUS_TAG" <<'PY'
import glob
import os
import sys
import xml.etree.ElementTree as ET

directory, focus = sys.argv[1:]
files = sorted(glob.glob(os.path.join(directory, "junit*.xml")))
if not files:
    sys.exit(f"node-conformance: no JUnit report in {directory}")
selected = passed = unselected = 0
failures = []
skipped = []
for path in files:
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as err:
        sys.exit(f"node-conformance: cannot parse {path}: {err}")
    for case in root.iter("testcase"):
        name = " ".join(case.get("name", "").split())
        is_selected = focus in name
        selected += is_selected
        bad = case.find("failure")
        if bad is None:
            bad = case.find("error")
        if bad is not None:
            message = (bad.get("message") or bad.text or "").strip().splitlines()
            failures.append((name, message[0] if message else ""))
        elif not is_selected:
            unselected += 1
        elif case.find("skipped") is not None:
            skipped.append(name)
        else:
            passed += 1
with open(os.path.join(directory, "failures.txt"), "w") as out:
    for name, message in failures:
        out.write(f"FAIL\t{name}\t{message}\n")
with open(os.path.join(directory, "skipped.txt"), "w") as out:
    for name in skipped:
        out.write(f"SKIPPED\t{name}\n")
summary = (
    f"selected {selected}\npassed {passed}\nfailed {len(failures)}\n"
    f"skipped {len(skipped)}\nunselected {unselected}\n"
)
with open(os.path.join(directory, "summary.txt"), "w") as out:
    out.write(summary)
sys.stdout.write(summary)
for name, message in failures:
    sys.stdout.write(f"FAIL\t{name}\t{message}\n")
for name in skipped:
    sys.stdout.write(f"SKIPPED\t{name}\n")
if selected == 0:
    sys.exit(f"node-conformance: the report contains no selected {focus} tests")
if failures:
    sys.exit(1)
if skipped:
    sys.stderr.write(f"node-conformance: INCOMPLETE: {len(skipped)} selected {focus} tests were skipped\n")
    sys.exit(2)
PY
}

# check_nodes validates a "kubectl get nodes -o json" inventory: it must name
# NODE_NAME with the Brie runtime taint, and every other node must carry the
# control-plane taint with a blocking effect, so test Pods can land only on
# the Brie node.
check_nodes() {
	python3 - "$1" "$NODE_NAME" "$BRIE_TAINT_KEY" "$CONTROL_PLANE_TAINT_KEY" <<'PY'
import json
import sys

path, brie, brie_key, control_plane_key = sys.argv[1:]
try:
    with open(path) as f:
        inventory = json.load(f)
    items = inventory["items"]
    names = [item["metadata"]["name"] for item in items]
except (OSError, ValueError, KeyError, TypeError) as err:
    sys.exit(f"node-conformance: cannot read the node inventory {path}: {err!r}")
if not isinstance(items, list) or not items:
    sys.exit("node-conformance: the node inventory is empty")
if (inventory.get("metadata") or {}).get("continue"):
    sys.exit("node-conformance: the node inventory is incomplete: it has a continue token")
if any(not isinstance(name, str) or not name for name in names) or len(set(names)) != len(names):
    sys.exit("node-conformance: the node inventory has unnamed or duplicate nodes")
if brie not in names:
    sys.exit(f"node-conformance: {brie} is not in the node inventory")
for item, name in zip(items, names):
    taints = (item.get("spec") or {}).get("taints") or []
    if name == brie:
        if not any(t.get("key") == brie_key for t in taints):
            sys.exit(f"node-conformance: {brie} lacks the {brie_key} taint; is it the Brie node?")
        continue
    if not any(t.get("key") == control_plane_key and t.get("effect") in ("NoSchedule", "NoExecute") for t in taints):
        sys.exit(
            f"node-conformance: node {name} has no NoSchedule or NoExecute {control_plane_key} taint, "
            "so it is schedulable for test Pods; results would not describe Brie"
        )
print(f"node-conformance: only {brie} can accept test Pods among {len(names)} nodes")
PY
}

# check_placement reads the Pod placement watch recorded during the run
# ("NAMESPACE NAME NODE" lines) and fails unless every Pod outside the kube-*
# namespaces that was bound to a node was bound to NODE_NAME, and at least one
# was.
check_placement() {
	python3 - "$1" "$NODE_NAME" <<'PY'
import sys

path, brie = sys.argv[1:]
try:
    with open(path) as f:
        lines = f.read().splitlines()
except OSError as err:
    sys.exit(f"node-conformance: cannot read the Pod placement record {path}: {err}")
on_brie = set()
elsewhere = set()
for number, line in enumerate(lines, 1):
    fields = line.split()
    if len(fields) != 3:
        sys.exit(f"node-conformance: {path}:{number}: malformed Pod placement line")
    namespace, name, node = fields
    if namespace.startswith("kube-") or node == "<none>":
        continue
    (on_brie if node == brie else elsewhere).add((namespace, name, node))
for namespace, name, node in sorted(elsewhere):
    print(f"MISPLACED\t{namespace}/{name}\t{node}")
if elsewhere:
    sys.exit(f"node-conformance: {len(elsewhere)} test Pods were bound to nodes other than {brie}")
if not on_brie:
    sys.exit(f"node-conformance: no test Pod was observed on {brie}")
print(f"node-conformance: all {len(on_brie)} observed test Pods were bound to {brie}")
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
--check-nodes)
	(($# == 2)) || usage
	[[ -n ${NODE_NAME:-} ]] || hw_checksums_die "required input NODE_NAME is not set"
	check_nodes "$2"
	exit 0
	;;
--check-placement)
	(($# == 2)) || usage
	[[ -n ${NODE_NAME:-} ]] || hw_checksums_die "required input NODE_NAME is not set"
	check_placement "$2"
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
work=$(mktemp -d)
watcher=
trap 'if [[ -n $watcher ]]; then kill "$watcher" 2>/dev/null || true; fi; rm -rf "$work"' EXIT
# The inventory is captured and checked as a whole; a failed or empty list
# stops the run instead of skipping the isolation check.
k get nodes -o json >"$work/nodes.json" || hw_checksums_die "cannot list the cluster nodes"
check_nodes "$work/nodes.json"
hw_fetch kubernetes-test "$work/kubernetes-test.tar.gz"
tar -C "$work" -xzf "$work/kubernetes-test.tar.gz" kubernetes/test/bin/e2e.test

out=$EVIDENCE_ROOT/node-conformance-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$out"
cp "$work/nodes.json" "$out/nodes.json"
{
	printf 'kubernetes-test %s %s %s\n' "$K8S_VERSION" "${HW_ARTIFACT_URL[kubernetes-test]}" "${HW_ARTIFACT_SHA[kubernetes-test]}"
	printf 'server %s\nbrie-node %s\nfocus %s\nskip none\n' "$server" "$NODE_NAME" "$FOCUS"
	k get nodes -o wide
	printf 'node-inventory nodes.json\n'
} >"$out/inputs.txt"

status=0
# Record where every Pod is bound for the whole run. The watch outlives the
# suite timeout; if it ends early, placement is unknown and the run fails.
kubectl --kubeconfig="$KUBECONFIG_PATH" --request-timeout=7h get pods --all-namespaces --watch \
	--no-headers -o 'custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,NODE:.spec.nodeName' \
	>"$out/pod-placement.txt" 2>"$out/pod-placement.err" &
watcher=$!
# The Brie runtime taint must not block suite startup; it is not a skip filter.
"$work/kubernetes/test/bin/e2e.test" \
	--kubeconfig="$KUBECONFIG_PATH" --provider=skeleton \
	--report-dir="$out" \
	--non-blocking-taints="$CONTROL_PLANE_TAINT_KEY,$BRIE_TAINT_KEY" \
	--ginkgo.focus="$FOCUS" --ginkgo.timeout=6h --ginkgo.no-color --ginkgo.v \
	>"$out/e2e.log" 2>&1 || status=$?
printf 'e2e.test exit status %d\n' "$status" >>"$out/inputs.txt"

placement_status=0
if kill -0 "$watcher" 2>/dev/null; then
	kill "$watcher" 2>/dev/null || true
	wait "$watcher" 2>/dev/null || true
	watcher=
	check_placement "$out/pod-placement.txt" >"$out/placement.txt" 2>&1 || placement_status=1
else
	watcher=
	printf 'node-conformance: the Pod placement watch ended before the suite; placement is unknown\n' >"$out/placement.txt"
	placement_status=1
fi
cat "$out/placement.txt"

summary_status=0
summarize "$out" || summary_status=$?
sums=$(cd "$out" && find . -type f -print0 | sort -z | xargs -0 sha256sum)
printf '%s\n' "$sums" >"$out/SHA256SUMS"
printf 'node-conformance: results in %s; publish every line of failures.txt\n' "$out"
if ((status != 0 || summary_status == 1 || placement_status != 0)); then
	exit 1
fi
if ((summary_status != 0)); then
	exit "$summary_status"
fi

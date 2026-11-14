#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Reproducible M1 sequence (DESIGN.md section 15) for one Brie workload host:
# networked Pod startup, probes, exec, logs, agent restart with surviving VMs,
# and cleanup. Each step records PASS, FAIL, SKIPPED-UNSUPPORTED (the step
# needs a feature the tested Brie build does not declare), or NOT-RUN (the
# harness has no step for it yet). Only an observed check can PASS. Evidence is
# credential-free and covered by SHA256SUMS. Exit status: 0 only if every
# step passed, 1 on any failure, 2 if steps were skipped or not run.
#
# Required inputs besides lib.sh's: M1_KUBECONFIG (absolute path to a lab
# kubeconfig), WORKER and WORKER_ZONE (the labelled workload instance),
# NODE_NAME (its Kubernetes Node), and M1_IMAGE (a digest-pinned BusyBox
# image). M1_FEATURES lists features the Brie build supports, from: probes
# exec logs. It defaults to none.
set -euo pipefail

HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$HW_DIR/lib.sh"
hw_init m1-harness

for name in M1_KUBECONFIG WORKER WORKER_ZONE NODE_NAME M1_IMAGE; do
	[[ -n ${!name:-} ]] || hw_die "required input $name is not set; it has no default"
done
[[ $M1_KUBECONFIG == /* && -f $M1_KUBECONFIG ]] || hw_die "M1_KUBECONFIG must be an absolute path to an existing file"
[[ $WORKER_ZONE =~ $HW_ZONE_PATTERN ]] || hw_die "WORKER_ZONE $WORKER_ZONE is not a valid zone"
[[ $M1_IMAGE =~ ^[a-z0-9.-]+(:[0-9]+)?/[a-z0-9._/-]+@sha256:[0-9a-f]{64}$ ]] || hw_die "M1_IMAGE must be pinned by digest"
RUNTIME_CLASS=${RUNTIME_CLASS:-kata-fc}
AGENT_UNIT=${AGENT_UNIT:-brie-node.service}
M1_FEATURES=${M1_FEATURES:-}
for feature in $M1_FEATURES; do
	case $feature in
	probes | exec | logs) ;;
	*) hw_die "unknown M1_FEATURES entry $feature" ;;
	esac
done
if ! hw_owned_by_label compute instances describe "$WORKER" --zone="$WORKER_ZONE"; then
	hw_die "worker $WORKER does not carry label $HW_MARKER; refusing to use it"
fi

NAMESPACE=brie-m1-$RUN_ID
EVIDENCE=$HW_LOG_DIR/m1-$(date -u +%Y%m%dT%H%M%SZ)
[[ ! -e $EVIDENCE ]] || hw_die "evidence directory $EVIDENCE already exists"
mkdir -p "$EVIDENCE"
RESULTS=$EVIDENCE/results.tsv
printf 'step\tstatus\tdetail\n' >"$RESULTS"

k() {
	kubectl --kubeconfig="$M1_KUBECONFIG" --request-timeout=60s "$@"
}

worker() {
	hw_gcloud compute ssh "$WORKER" --zone="$WORKER_ZONE" --tunnel-through-iap --command="$1"
}

has_feature() {
	[[ " $M1_FEATURES " == *" $1 "* ]]
}

failures=0
record() {
	local step=$1 status=$2 detail=$3
	printf '%s\t%s\t%s\n' "$step" "$status" "${detail//$'\t'/ }" >>"$RESULTS"
	hw_log m1 "$step $status"
	printf 'm1: %-22s %s %s\n' "$step" "$status" "$detail"
	if [[ $status == FAIL ]]; then
		failures=$((failures + 1))
	fi
}

pod_manifest() {
	local name=$1 probe=${2:-}
	cat <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: $NAMESPACE
spec:
  os:
    name: linux
  runtimeClassName: $RUNTIME_CLASS
  restartPolicy: Never
  automountServiceAccountToken: false
  enableServiceLinks: false
  dnsPolicy: ClusterFirst
  terminationGracePeriodSeconds: 5
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    runAsGroup: 65534
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: web
      image: $M1_IMAGE
      imagePullPolicy: IfNotPresent
      command:
        - sh
        - -c
        - mkdir -p /tmp/www && cat /proc/sys/kernel/random/boot_id >/tmp/www/boot_id && echo brie-m1-started && exec httpd -f -p 8080 -h /tmp/www
      resources:
        requests:
          cpu: "1"
          memory: 128Mi
        limits:
          cpu: "1"
          memory: 128Mi
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop:
            - ALL
$probe
YAML
}

# pod_fields prints uid, node, IP, and phase separated by spaces.
pod_fields() {
	k get pod "$1" -n "$NAMESPACE" -o 'jsonpath={.metadata.uid} {.spec.nodeName} {.status.podIP} {.status.phase}'
}

# host_state prints VM PIDs, container IDs, and the guest boot ID for a Pod.
host_state() {
	local uid=$1 ip=$2 pids containers boot
	pids=$(worker "pgrep -x firecracker | sort -n | tr '\\n' ' '") || return 1
	containers=$(worker "sudo crictl ps -q --label io.kubernetes.pod.uid=$uid | sort | tr '\\n' ' '") || return 1
	boot=$(worker "curl -fsS --max-time 5 http://$ip:8080/boot_id") || return 1
	printf 'vm_pids=%s\ncontainer_ids=%s\nguest_boot_id=%s\n' "$pids" "$containers" "$boot"
}

# The namespace is created exclusively with the run label, and cleanup deletes
# only that incarnation: it re-reads the UID and label and deletes with a UID
# precondition. Nothing is deleted before the create succeeds, so a failed
# preflight or a preexisting namespace of the same name is never touched.
namespace_uid=
cleanup_namespace() {
	[[ -n $namespace_uid ]] || return 0
	local current
	if ! current=$(k get namespace "$NAMESPACE" -o "jsonpath={.metadata.uid} {.metadata.labels.$HW_LABEL_KEY}" 2>/dev/null); then
		printf 'm1: could not read namespace %s for cleanup; delete it by hand after checking UID %s\n' "$NAMESPACE" "$namespace_uid" >&2
		return 0
	fi
	if [[ $current != "$namespace_uid $RUN_ID" ]]; then
		hw_log refuse "namespace $NAMESPACE is not the incarnation this run created"
		printf 'm1: REFUSED to delete namespace %s: it is not the incarnation this run created\n' "$NAMESPACE" >&2
		return 0
	fi
	printf '{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":"%s"}}\n' "$namespace_uid" |
		k delete --raw "/api/v1/namespaces/$NAMESPACE" -f - >/dev/null 2>&1 ||
		printf 'm1: deleting namespace %s failed; delete it by hand after checking UID %s\n' "$NAMESPACE" "$namespace_uid" >&2
}
trap cleanup_namespace EXIT

# Preflight.
k version >"$EVIDENCE/kubectl-version.txt" 2>&1 || hw_die "cannot reach the API server"
ready=$(k get node "$NODE_NAME" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}') ||
	hw_die "cannot read node $NODE_NAME"
[[ $ready == True ]] || hw_die "node $NODE_NAME is not Ready"
handler=$(k get runtimeclass "$RUNTIME_CLASS" -o 'jsonpath={.handler}') || hw_die "RuntimeClass $RUNTIME_CLASS is missing"
printf 'node %s\nruntime_class %s\nhandler %s\nimage %s\nfeatures %s\n' \
	"$NODE_NAME" "$RUNTIME_CLASS" "$handler" "$M1_IMAGE" "${M1_FEATURES:-none}" >"$EVIDENCE/inputs.txt"
namespace_manifest=$(printf 'apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n  labels:\n    %s: "%s"\n' \
	"$NAMESPACE" "$HW_LABEL_KEY" "$RUN_ID")
created_uid=$(k create -f - -o 'jsonpath={.metadata.uid}' <<<"$namespace_manifest") ||
	hw_die "could not create namespace $NAMESPACE; refusing to reuse an existing namespace"
[[ $created_uid =~ ^[0-9a-f-]{36}$ ]] || hw_die "namespace $NAMESPACE was created without a recognizable UID; delete it by hand after checking it"
namespace_uid=$created_uid
printf 'namespace %s\nnamespace_uid %s\n' "$NAMESPACE" "$namespace_uid" >>"$EVIDENCE/inputs.txt"

# Networked Pod startup.
pod_manifest m1-web >"$EVIDENCE/m1-web.yaml"
started=$(hw_now_epoch)
uid=''
node=''
ip=''
phase=''
if k apply -f "$EVIDENCE/m1-web.yaml" >/dev/null &&
	k wait --for=condition=Ready pod/m1-web -n "$NAMESPACE" --timeout=300s >/dev/null; then
	read -r uid node ip phase <<<"$(pod_fields m1-web)"
	if [[ $node == "$NODE_NAME" && $phase == Running && -n $ip ]]; then
		record startup PASS "Ready on $node in $(($(hw_now_epoch) - started)) s (harness wall clock)"
	else
		record startup FAIL "node=$node phase=$phase ip=$ip"
	fi
else
	record startup FAIL "Pod did not become Ready within 300 s"
fi
k get pod m1-web -n "$NAMESPACE" -o yaml >"$EVIDENCE/m1-web.status.yaml" 2>&1 || true

boot_before=
if [[ -n $ip ]] && boot_before=$(worker "curl -fsS --max-time 5 http://$ip:8080/boot_id") && [[ -n $boot_before ]]; then
	record pod-network PASS "HTTP from the worker host to $ip:8080 returned the guest boot ID"
else
	record pod-network FAIL "no HTTP response from Pod IP ${ip:-unknown}"
fi
record service-and-dns NOT-RUN "the harness has no Service or in-guest DNS step"

if has_feature probes; then
	# Brie refuses a probe on an undeclared port, and its base profile refuses
	# declared ports, so only the probe Pod declares one.
	probe='      ports:
        - name: http
          containerPort: 8080
      readinessProbe:
        tcpSocket:
          port: 8080
        periodSeconds: 2'
	pod_manifest m1-probe "$probe" >"$EVIDENCE/m1-probe.yaml"
	if k apply -f "$EVIDENCE/m1-probe.yaml" >/dev/null &&
		k wait --for=condition=Ready pod/m1-probe -n "$NAMESPACE" --timeout=300s >/dev/null; then
		record probes PASS "TCP readiness probe reported Ready"
	else
		record probes FAIL "Pod with a readiness probe did not become Ready"
	fi
	k delete pod m1-probe -n "$NAMESPACE" --wait=true --timeout=120s >/dev/null 2>&1 || true
else
	record probes SKIPPED-UNSUPPORTED "probes are not in M1_FEATURES; the restricted profile rejects them"
fi

if has_feature exec; then
	exec_boot=$(k exec m1-web -n "$NAMESPACE" -- cat /proc/sys/kernel/random/boot_id 2>/dev/null || true)
	if [[ -n $exec_boot && $exec_boot == "$boot_before" ]]; then
		record exec PASS "kubectl exec returned the guest boot ID"
	else
		record exec FAIL "kubectl exec output did not match the guest boot ID"
	fi
else
	record exec SKIPPED-UNSUPPORTED "exec is not in M1_FEATURES (D12: no exec through Kubernetes)"
fi

if has_feature logs; then
	if k logs m1-web -n "$NAMESPACE" 2>/dev/null | grep -qx brie-m1-started; then
		record logs PASS "kubectl logs returned the container output"
	else
		record logs FAIL "kubectl logs did not return the container output"
	fi
else
	record logs SKIPPED-UNSUPPORTED "logs are not in M1_FEATURES (D12: no logs through Kubernetes)"
fi
record volumes SKIPPED-UNSUPPORTED "the restricted profile rejects volumes"

# Agent restart with surviving VMs.
if [[ -n $uid && -n $ip ]] && host_state "$uid" "$ip" >"$EVIDENCE/restart-before.txt"; then
	restarted=0
	worker "sudo systemctl restart $AGENT_UNIT" && restarted=1
	ready=
	for _ in $(seq 1 60); do
		ready=$(k get node "$NODE_NAME" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' || true)
		[[ $ready != True ]] || break
		sleep 5
	done
	if ((restarted == 1)) && [[ $ready == True ]] && host_state "$uid" "$ip" >"$EVIDENCE/restart-after.txt"; then
		if ! grep -q '=$' "$EVIDENCE/restart-before.txt" &&
			cmp -s "$EVIDENCE/restart-before.txt" "$EVIDENCE/restart-after.txt"; then
			record agent-restart PASS "VM PIDs, container IDs, and guest boot ID unchanged across $AGENT_UNIT restart"
		else
			record agent-restart FAIL "VM PIDs, container IDs, or guest boot ID changed or were empty"
		fi
	else
		record agent-restart FAIL "agent restart or post-restart observation failed (node Ready=$ready)"
	fi
else
	record agent-restart FAIL "could not observe the running Pod before restart"
fi

record warm-pause-resume SKIPPED-UNSUPPORTED "Brie has no pause API; Kata 4.3.1 Firecracker PauseVM is a no-op"
record deletion-races NOT-RUN "the harness has no deletion/retry race step"
record memory-footprint NOT-RUN "the harness does not measure host memory"

# Cleanup.
if [[ -n $uid ]] && k delete pod m1-web -n "$NAMESPACE" --wait=true --timeout=120s >/dev/null; then
	remaining=$(k get pod m1-web -n "$NAMESPACE" --ignore-not-found -o name || printf 'unknown')
	sandboxes=$(worker "sudo crictl pods -q --label io.kubernetes.pod.uid=$uid" || printf 'unknown')
	if [[ -z $remaining && -z $sandboxes ]]; then
		record cleanup PASS "Pod and its CRI sandbox are gone"
	else
		record cleanup FAIL "remaining Pod=${remaining:-none} sandboxes=${sandboxes:-none}"
	fi
else
	record cleanup FAIL "Pod deletion did not complete within 120 s"
fi

sums=$(cd "$EVIDENCE" && find . -type f -print0 | sort -z | xargs -0 sha256sum)
printf '%s\n' "$sums" >"$EVIDENCE/SHA256SUMS"
skipped=$(grep -cE $'\t(SKIPPED-UNSUPPORTED|NOT-RUN)\t' "$RESULTS" || true)
hw_log m1-result "failures=$failures skipped_or_not_run=$skipped evidence=$EVIDENCE"
printf 'm1: %d failed, %d skipped or not run; evidence in %s\n' "$failures" "$skipped" "$EVIDENCE"
if ((failures > 0)); then
	exit 1
fi
if ((skipped > 0)); then
	# A run with skipped steps is not an M1 result, so it does not exit zero.
	printf 'm1: INCOMPLETE: M1 requires every step to pass\n' >&2
	exit 2
fi
printf 'm1: all steps passed\n'

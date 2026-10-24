#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
umask 077
export KUBECONFIG=/etc/kubernetes/admin.conf
out=/var/lib/brie-experiment/features
mkdir -p "$out"
node="$(kubectl get nodes -o json | jq -er '.items[0].metadata.name')"
kubectl get --raw=/metrics | grep '^kubernetes_feature_enabled' > "$out/apiserver.prom"
kubectl get --raw="/api/v1/nodes/$node/proxy/metrics" \
  | grep '^kubernetes_feature_enabled' > "$out/kubelet.prom"
curl -fsS http://127.0.0.1:10249/metrics \
  | grep '^kubernetes_feature_enabled' > "$out/kube-proxy.prom"
kubelet --help > "$out/kubelet-help.txt" 2>&1
for component in kube-apiserver kube-controller-manager kube-scheduler kube-proxy; do
  if [[ "$component" == kube-proxy ]]; then
    pod="$(kubectl -n kube-system get pods -l k8s-app=kube-proxy -o jsonpath='{.items[0].metadata.name}')"
  else
    pod="$(kubectl -n kube-system get pods -l "component=$component" -o jsonpath='{.items[0].metadata.name}')"
  fi
  kubectl -n kube-system exec "$pod" -- "$component" --help > "$out/$component-help.txt" 2>&1
done
printf 'component\tfeature\tstage\tdefault\n' > "$out/defaults.tsv"
for component in kube-apiserver kube-controller-manager kube-scheduler kube-proxy kubelet; do
  awk -v component="$component" '
    /=true\|false.*default=(true|false)/ {
      feature=$1; sub(/=true\|false/, "", feature)
      stage=$2; gsub(/[()]/, "", stage)
      value=$0; sub(/.*default=/, "", value); sub(/\).*/, "", value)
      print component "\t" feature "\t" stage "\t" value
    }
  ' "$out/$component-help.txt" >> "$out/defaults.tsv"
done
[[ "$(wc -l < "$out/defaults.tsv")" -gt 1 ]] || { echo 'No component feature defaults found' >&2; exit 1; }
kubectl -n kube-system get configmap kube-proxy -o jsonpath='{.data.config\.conf}' > "$out/kube-proxy.yaml"
grep -Eq '^mode: nftables$' "$out/kube-proxy.yaml"
kubectl get --raw="/api/v1/nodes/$node/proxy/configz" | jq '.kubeletconfig.featureGates' > "$out/kubelet-overrides.json"
kubectl -n kube-system get configmap cilium-config -o json \
  | jq '.data | {kubeProxyReplacement: .["kube-proxy-replacement"], ipam: .ipam}' > "$out/cilium.json"
jq -e '.kubeProxyReplacement == "false" and .ipam == "kubernetes"' "$out/cilium.json" >/dev/null
nft list table ip kube-proxy > "$out/nftables.txt"
echo "Feature defaults from installed --help and effective feature metrics saved in $out; no gates overridden."

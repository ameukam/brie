#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
umask 077
trap 'echo "Control-plane bootstrap failed at line $LINENO; inspect root-only bootstrap logs" >&2' ERR
[[ "$(uname -m)" == x86_64 ]]
[[ ! -e /etc/kubernetes/admin.conf ]] || {
  echo 'Existing control-plane state found; refusing to reinitialize.' >&2
  exit 1
}
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq ca-certificates curl gpg jq openssl nftables containerd=2.2.1-0ubuntu1~24.04.4 conntrack socat
swapoff -a
modprobe overlay
modprobe br_netfilter
printf 'net.ipv4.ip_forward=1\nnet.bridge.bridge-nf-call-iptables=1\n' > /etc/sysctl.d/90-brie-experiment.conf
sysctl --system >/dev/null
mkdir -p /etc/containerd
containerd config default | sed 's/SystemdCgroup = false/SystemdCgroup = true/' > /etc/containerd/config.toml
systemctl restart containerd
mkdir -p /etc/apt/keyrings
curl --fail --silent --show-error --location --retry 3 \
  https://pkgs.k8s.io/core:/stable:/v1.37/deb/Release.key \
  | gpg --dearmor --batch --output /etc/apt/keyrings/kubernetes-apt-keyring.gpg
chmod 644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.37/deb/ /' \
  > /etc/apt/sources.list.d/kubernetes.list
chmod 644 /etc/apt/sources.list.d/kubernetes.list
apt-get update -qq
apt-get install -y -qq kubelet=1.37.0-1.1 kubeadm=1.37.0-1.1 kubectl=1.37.0-1.1
apt-mark hold kubelet kubeadm kubectl
mkdir -p /var/lib/brie-experiment
for file in kubeadm.yaml addons.sh features.sh; do
  curl --fail --silent --show-error --retry 3 -H Metadata-Flavor:Google \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/brie-${file%.*}" \
    -o "/var/lib/brie-experiment/$file"
done
kubeadm config validate --config=/var/lib/brie-experiment/kubeadm.yaml
# Keep administrative credentials and bootstrap output on the host only.
kubeadm init --config=/var/lib/brie-experiment/kubeadm.yaml --skip-token-print \
  > /var/lib/brie-experiment/kubeadm.log 2>&1
bash /var/lib/brie-experiment/addons.sh > /var/lib/brie-experiment/addons.log 2>&1
bash /var/lib/brie-experiment/features.sh > /var/lib/brie-experiment/features.log 2>&1
kubectl --kubeconfig=/etc/kubernetes/admin.conf wait --for=condition=Ready nodes --all --timeout=180s
kubectl --kubeconfig=/etc/kubernetes/admin.conf get --raw=/readyz \
  > /var/lib/brie-experiment/apiserver-ready
dpkg-query -W containerd kubelet kubeadm kubectl > /var/lib/brie-experiment/packages.txt
echo 'Upstream control plane bootstrapped. No Brie worker is joined; this is not Brie conformance.'

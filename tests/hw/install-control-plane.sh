#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Installs a single-node kubeadm 1.37 control plane for the lab on an Ubuntu
# 24.04 Arm64 host: containerd 2.4.2 with runc, the Node authorizer and
# NodeRestriction, kube-proxy in iptables mode, and bridge CNI (D11). Every
# download is pinned by URL and SHA-256 in the operator-filled checksums file.
# Run it as root on the control-plane host. It never calls gcloud and records
# no kubeconfig or token in its evidence.
#
# Usage:
#   install-control-plane.sh CHECKSUMS
#   install-control-plane.sh --check-only CHECKSUMS
set -euo pipefail

HW_SCRIPT=install-control-plane
HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=checksums.sh
source "$HW_DIR/checksums.sh"

EVIDENCE_DIR=${EVIDENCE_DIR:-/var/lib/brie-hw/evidence}
POD_CIDR=${POD_CIDR:-10.244.0.0/16}
SERVICE_CIDR=${SERVICE_CIDR:-10.96.0.0/12}
CONTROL_PLANE_ARTIFACTS=(containerd containerd-service runc cni-plugins crictl kubeadm kubelet kubectl)
KUBERNETES_PATTERN='^1\.37\.[0-9]+$'

usage() {
	hw_checksums_die "usage: install-control-plane.sh CHECKSUMS | --check-only CHECKSUMS"
}

load_checksums() {
	local name
	hw_checksums_load "$1"
	hw_checksums_require "${CONTROL_PLANE_ARTIFACTS[@]}"
	K8S_VERSION=${HW_ARTIFACT_VERSION[kubeadm]}
	[[ $K8S_VERSION =~ $KUBERNETES_PATTERN ]] || hw_checksums_die "kubeadm must be a Kubernetes 1.37 release, not $K8S_VERSION"
	for name in kubelet kubectl; do
		[[ ${HW_ARTIFACT_VERSION[$name]} == "$K8S_VERSION" ]] ||
			hw_checksums_die "$name ${HW_ARTIFACT_VERSION[$name]} differs from kubeadm $K8S_VERSION"
	done
}

case ${1:-} in
--check-only)
	(($# == 2)) || usage
	load_checksums "$2"
	printf 'install-control-plane: %s is complete for Kubernetes %s\n' "$2" "$K8S_VERSION"
	exit 0
	;;
'' | -*) usage ;;
esac
(($# == 1)) || usage
load_checksums "$1"
for name in POD_CIDR SERVICE_CIDR; do
	[[ ${!name} =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}/[0-9]{1,2}$ ]] || hw_checksums_die "$name must be an IPv4 CIDR"
done

[[ $EUID -eq 0 ]] || hw_checksums_die "must run as root on the control-plane host"
[[ $(uname -m) == aarch64 ]] || hw_checksums_die "host architecture must be aarch64, not $(uname -m)"
if ! grep -qx 'VERSION_ID="24.04"' /etc/os-release || ! grep -qx 'ID=ubuntu' /etc/os-release; then
	hw_checksums_die "host must run Ubuntu 24.04"
fi
ADVERTISE_ADDRESS=${ADVERTISE_ADDRESS:-$(hostname -I | awk '{ print $1 }')}
[[ $ADVERTISE_ADDRESS =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || hw_checksums_die "cannot determine ADVERTISE_ADDRESS"
[[ ! -e /etc/kubernetes/admin.conf ]] || hw_checksums_die "a control plane is already initialized on this host"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates conntrack curl ethtool iptables socat

for name in "${CONTROL_PLANE_ARTIFACTS[@]}"; do
	hw_fetch "$name" "$work/$name"
done
tar -C /usr/local -xzf "$work/containerd"
install -m 0644 "$work/containerd-service" /etc/systemd/system/containerd.service
install -m 0755 "$work/runc" /usr/local/sbin/runc
mkdir -p /opt/cni/bin
tar -C /opt/cni/bin -xzf "$work/cni-plugins"
tar -C /usr/local/bin -xzf "$work/crictl" crictl
for name in kubeadm kubelet kubectl; do
	install -m 0755 "$work/$name" "/usr/local/bin/$name"
done

printf 'br_netfilter\n' >/etc/modules-load.d/brie-hw.conf
modprobe br_netfilter
printf 'net.ipv4.ip_forward = 1\nnet.bridge.bridge-nf-call-iptables = 1\n' >/etc/sysctl.d/99-brie-hw.conf
sysctl --system >/dev/null
swapoff -a

mkdir -p /etc/containerd
cat >/etc/containerd/config.toml <<'TOML'
version = 3

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc]
  runtime_type = 'io.containerd.runc.v2'

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc.options]
  SystemdCgroup = true

[plugins.'io.containerd.cri.v1.runtime'.cni]
  bin_dirs = ['/opt/cni/bin']
  conf_dir = '/etc/cni/net.d'
TOML
printf 'runtime-endpoint: unix:///run/containerd/containerd.sock\nimage-endpoint: unix:///run/containerd/containerd.sock\ntimeout: 10\n' >/etc/crictl.yaml

cat >/etc/systemd/system/kubelet.service <<'UNIT'
[Unit]
Description=kubelet: The Kubernetes Node Agent
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/local/bin/kubelet
Restart=always
StartLimitInterval=0
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT
mkdir -p /etc/systemd/system/kubelet.service.d
cat >/etc/systemd/system/kubelet.service.d/10-kubeadm.conf <<'UNIT'
[Service]
Environment="KUBELET_KUBECONFIG_ARGS=--bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubelet.conf --kubeconfig=/etc/kubernetes/kubelet.conf"
Environment="KUBELET_CONFIG_ARGS=--config=/var/lib/kubelet/config.yaml"
EnvironmentFile=-/var/lib/kubelet/kubeadm-flags.env
ExecStart=
ExecStart=/usr/local/bin/kubelet $KUBELET_KUBECONFIG_ARGS $KUBELET_CONFIG_ARGS $KUBELET_KUBEADM_ARGS $KUBELET_EXTRA_ARGS
UNIT
systemctl daemon-reload
systemctl enable --now containerd.service
systemctl enable kubelet.service

kubeadm_config=$work/kubeadm.yaml
cat >"$kubeadm_config" <<YAML
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: $ADVERTISE_ADDRESS
nodeRegistration:
  criSocket: unix:///run/containerd/containerd.sock
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: v$K8S_VERSION
networking:
  podSubnet: $POD_CIDR
  serviceSubnet: $SERVICE_CIDR
apiServer:
  extraArgs:
    - name: authorization-mode
      value: Node,RBAC
    - name: enable-admission-plugins
      value: NodeRestriction
---
apiVersion: kubeproxy.config.k8s.io/v1alpha1
kind: KubeProxyConfiguration
mode: iptables
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
YAML
# The default init output prints the bootstrap join token, and run-on-host.sh
# forwards this script's output to the operator's terminal and logs.
kubeadm init --config "$kubeadm_config" --skip-token-print

export KUBECONFIG=/etc/kubernetes/admin.conf
apiserver=/etc/kubernetes/manifests/kube-apiserver.yaml
grep -q -- '--authorization-mode=Node,RBAC' "$apiserver" || hw_checksums_die "API server lacks the Node authorizer"
grep -q -- '--enable-admission-plugins=NodeRestriction' "$apiserver" || hw_checksums_die "API server lacks NodeRestriction"
kubectl -n kube-system get configmap kube-proxy -o jsonpath='{.data.config\.conf}' | grep -q '^mode: iptables$' ||
	hw_checksums_die "kube-proxy is not in iptables mode"

node=$(hostname)
node_cidr=
for _ in $(seq 1 60); do
	node_cidr=$(kubectl get node "$node" -o jsonpath='{.spec.podCIDR}' || true)
	[[ -z $node_cidr ]] || break
	sleep 5
done
[[ -n $node_cidr ]] || hw_checksums_die "the controller manager assigned no Pod CIDR to $node"
mkdir -p /etc/cni/net.d
cat >/etc/cni/net.d/10-brie-bridge.conflist <<CNI
{
  "cniVersion": "1.0.0",
  "name": "brie-bridge",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "hairpinMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [[{"subnet": "$node_cidr"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      }
    },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
CNI
kubectl wait --for=condition=Ready "node/$node" --timeout=300s

mkdir -p "$EVIDENCE_DIR"
evidence=$EVIDENCE_DIR/install-control-plane.txt
{
	printf 'recorded_at %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf 'kernel %s\n' "$(uname -r)"
	hw_artifact_summary
	printf 'kubeadm %s\n' "$(kubeadm version -o short)"
	printf 'containerd %s\n' "$(/usr/local/bin/containerd --version)"
	printf 'authorization-mode Node,RBAC\nadmission-plugins NodeRestriction\nkube-proxy-mode iptables\n'
	printf 'pod-cidr %s\nservice-cidr %s\ncontrol-plane-node %s\ncontrol-plane-pod-cidr %s\n' \
		"$POD_CIDR" "$SERVICE_CIDR" "$node" "$node_cidr"
} >"$evidence"
(cd "$EVIDENCE_DIR" && sha256sum install-control-plane.txt >install-control-plane.txt.sha256)
printf 'install-control-plane: Kubernetes %s initialized; evidence in %s\n' "$K8S_VERSION" "$evidence"

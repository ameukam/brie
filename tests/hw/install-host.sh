#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Installs the provisional D4 tuple on one Ubuntu 24.04 Arm64 workload host:
# containerd 2.4.2, Kata 4.3.1 with its Firecracker 1.17.0, CNI plugins 1.9.1,
# crictl, and a loopback devmapper thin pool. Every download is pinned by URL
# and SHA-256 in the operator-filled checksums file. It writes the D9
# node-local digest manifest and verifies the Kata, Firecracker, and jailer
# binaries against it before configuring containerd. Run it as root on the
# host, for example through run-on-host.sh. It never calls gcloud.
#
# Usage:
#   install-host.sh CHECKSUMS             install and verify (root, on the host)
#   install-host.sh --check-only CHECKSUMS
#   install-host.sh --verify-manifest [MANIFEST]
set -euo pipefail

HW_SCRIPT=install-host
HW_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=checksums.sh
source "$HW_DIR/checksums.sh"

MANIFEST=/etc/brie/hw-digests.sha256
EVIDENCE_DIR=${EVIDENCE_DIR:-/var/lib/brie-hw/evidence}
HOST_ARTIFACTS=(containerd containerd-service kata-static cni-plugins crictl)
FIRECRACKER_VERSION=1.17.0
KATA_FC_CONFIG=/opt/kata/share/defaults/kata-containers/configuration-fc.toml

usage() {
	hw_checksums_die "usage: install-host.sh CHECKSUMS | --check-only CHECKSUMS | --verify-manifest [MANIFEST]"
}

load_checksums() {
	hw_checksums_load "$1"
	hw_checksums_require "${HOST_ARTIFACTS[@]}"
	hw_checksums_require_manifest
}

case ${1:-} in
--check-only)
	(($# == 2)) || usage
	load_checksums "$2"
	printf 'install-host: %s is complete for the host install\n' "$2"
	exit 0
	;;
--verify-manifest)
	(($# <= 2)) || usage
	hw_verify_manifest "${2:-$MANIFEST}"
	printf 'install-host: binaries match %s\n' "${2:-$MANIFEST}"
	exit 0
	;;
'' | -*) usage ;;
esac
(($# == 1)) || usage
load_checksums "$1"

[[ $EUID -eq 0 ]] || hw_checksums_die "must run as root on the workload host"
[[ $(uname -m) == aarch64 ]] || hw_checksums_die "host architecture must be aarch64, not $(uname -m)"
os_release() {
	sed -n "s/^$1=//p" /etc/os-release | tr -d '"'
}
OS_NAME=$(os_release PRETTY_NAME)
[[ $(os_release ID) == ubuntu && $(os_release VERSION_ID) == 24.04 ]] ||
	hw_checksums_die "host must run Ubuntu 24.04, not ${OS_NAME:-unknown}"
[[ -c /dev/kvm ]] || hw_checksums_die "/dev/kvm is not available on this host"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl dmsetup iptables zstd

for name in "${HOST_ARTIFACTS[@]}"; do
	hw_fetch "$name" "$work/$name"
done

tar -C /usr/local -xzf "$work/containerd"
install -m 0644 "$work/containerd-service" /etc/systemd/system/containerd.service
mkdir -p /opt/cni/bin
tar -C /opt/cni/bin -xzf "$work/cni-plugins"
tar -C /usr/local/bin -xzf "$work/crictl" crictl
case ${HW_ARTIFACT_URL[kata-static]} in
*.tar.zst) tar --zstd -C / -xf "$work/kata-static" ;;
*.tar.xz) tar -C / -xJf "$work/kata-static" ;;
*) hw_checksums_die "unsupported Kata archive format: ${HW_ARTIFACT_URL[kata-static]}" ;;
esac

# D9: the manifest comes from operator-verified digests, not from this host.
hw_write_manifest "$MANIFEST"
hw_verify_manifest "$MANIFEST"
/opt/kata/bin/firecracker --version | grep -q "v$FIRECRACKER_VERSION" ||
	hw_checksums_die "Kata's Firecracker is not version $FIRECRACKER_VERSION"
ln -sf /opt/kata/bin/containerd-shim-kata-v2 /usr/local/bin/containerd-shim-kata-v2

printf 'br_netfilter\n' >/etc/modules-load.d/brie-hw.conf
modprobe br_netfilter
printf 'net.ipv4.ip_forward = 1\nnet.bridge.bridge-nf-call-iptables = 1\n' >/etc/sysctl.d/99-brie-hw.conf
sysctl --system >/dev/null

# Disposable lab storage: a loopback-backed thin pool, recreated at boot.
cat >/usr/local/sbin/brie-hw-devmapper-pool <<'POOL'
#!/usr/bin/env bash
set -euo pipefail
dir=/var/lib/containerd/devmapper
pool=devpool
mkdir -p "$dir"
[[ -f $dir/data ]] || truncate -s 16G "$dir/data"
[[ -f $dir/meta ]] || truncate -s 256M "$dir/meta"
if dmsetup info "$pool" >/dev/null 2>&1; then
	exit 0
fi
data_dev=$(losetup --find --show "$dir/data")
meta_dev=$(losetup --find --show "$dir/meta")
sectors=$(($(blockdev --getsize64 "$data_dev") / 512))
dmsetup create "$pool" --table "0 $sectors thin-pool $meta_dev $data_dev 128 32768 1 skip_block_zeroing"
POOL
chmod 0755 /usr/local/sbin/brie-hw-devmapper-pool
cat >/etc/systemd/system/brie-hw-devmapper-pool.service <<'UNIT'
[Unit]
Description=Brie lab loopback devmapper thin pool
Before=containerd.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/sbin/brie-hw-devmapper-pool

[Install]
WantedBy=multi-user.target
UNIT

mkdir -p /etc/containerd
# containerd renders this bridge configuration (D11) from the Pod CIDR that
# Brie sends through CRI UpdateRuntimeConfig.
cat >/etc/containerd/cni-bridge.template <<'CNI'
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
        "ranges": [[{"subnet": "{{.PodCIDR}}"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      }
    },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
CNI
cat >/etc/containerd/config.toml <<TOML
version = 3

[plugins.'io.containerd.snapshotter.v1.devmapper']
  pool_name = 'devpool'
  root_path = '/var/lib/containerd/devmapper'
  base_image_size = '512MB'
  discard_blocks = true

[plugins.'io.containerd.cri.v1.runtime'.cni]
  bin_dirs = ['/opt/cni/bin']
  conf_dir = '/etc/cni/net.d'
  conf_template = '/etc/containerd/cni-bridge.template'

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.kata-fc]
  runtime_type = 'io.containerd.kata.v2'
  snapshotter = 'devmapper'
  pod_annotations = []

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.kata-fc.options]
  ConfigPath = '$KATA_FC_CONFIG'
TOML
printf 'runtime-endpoint: unix:///run/containerd/containerd.sock\nimage-endpoint: unix:///run/containerd/containerd.sock\ntimeout: 10\n' >/etc/crictl.yaml

systemctl daemon-reload
systemctl enable --now brie-hw-devmapper-pool.service containerd.service
crictl version >/dev/null

mkdir -p "$EVIDENCE_DIR"
evidence=$EVIDENCE_DIR/install-host.txt
{
	printf 'recorded_at %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf 'kernel %s\n' "$(uname -r)"
	printf 'os %s\n' "$OS_NAME"
	hw_artifact_summary
	printf 'containerd %s\n' "$(/usr/local/bin/containerd --version)"
	printf 'firecracker %s\n' "$(/opt/kata/bin/firecracker --version | head -n 1)"
	printf 'jailer %s\n' "$(/opt/kata/bin/jailer --version | head -n 1)"
	printf 'kata-runtime %s\n' "$(/opt/kata/bin/kata-runtime --version | head -n 1)"
	printf 'digest-manifest %s\n' "$MANIFEST"
	cat "$MANIFEST"
	# D9: host VMM privilege settings are recorded, not changed.
	grep -E '^[[:space:]]*(jailer_path|disable_guest_seccomp|default_vcpus|default_memory|kernel|image)[[:space:]]*=' "$KATA_FC_CONFIG" |
		sed 's/^[[:space:]]*/kata-fc-config /'
	printf 'devmapper %s\n' "$(dmsetup status devpool)"
} >"$evidence"
(cd "$EVIDENCE_DIR" && sha256sum install-host.txt >install-host.txt.sha256)
printf 'install-host: installed and verified; evidence in %s\n' "$evidence"

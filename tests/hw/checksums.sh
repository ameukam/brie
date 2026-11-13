# shellcheck shell=bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
#
# Reads the operator-filled checksums file (see checksums.template) for the
# host install scripts. Source this file; do not execute it. It never calls
# gcloud and never invents a URL or digest.

HW_SHA256_PATTERN='^[0-9a-f]{64}$'
HW_SEMVER_PATTERN='^[0-9]+\.[0-9]+\.[0-9]+$'
HW_URL_PATTERN='^https://(github\.com|raw\.githubusercontent\.com|dl\.k8s\.io)/[A-Za-z0-9._/-]+$'
# Versions pinned by the provisional D4 tuple. Other artifacts need only a
# concrete version that appears in their URL.
declare -A HW_PINNED_VERSIONS=(
	[containerd]=2.4.2
	[containerd-service]=2.4.2
	[kata-static]=4.3.1
	[cni-plugins]=1.9.1
	[crictl]=1.37.0
)
# The D9 node-local digest manifest must cover these binaries.
HW_MANIFEST_BINARIES=(
	/opt/kata/bin/firecracker
	/opt/kata/bin/jailer
	/opt/kata/bin/containerd-shim-kata-v2
	/opt/kata/bin/kata-runtime
)
declare -A HW_ARTIFACT_URL=() HW_ARTIFACT_VERSION=() HW_ARTIFACT_SHA=() HW_BINARY_SHA=()

hw_checksums_die() {
	printf '%s: %s\n' "${HW_SCRIPT:-brie-hw}" "$*" >&2
	exit 1
}

# hw_checksums_load validates every line of the file. Any placeholder, unknown
# record, malformed digest, or non-HTTPS URL is refused.
hw_checksums_load() {
	local file=$1 line number=0 kind name version url sha extra
	[[ -f $file && -r $file ]] || hw_checksums_die "checksums file $file is missing or unreadable"
	while IFS= read -r line || [[ -n $line ]]; do
		number=$((number + 1))
		[[ $line =~ ^[[:space:]]*(#|$) ]] && continue
		if [[ $line == *'<'* || $line == *'>'* ]]; then
			hw_checksums_die "$file:$number still contains a placeholder; fill in verified values"
		fi
		read -r kind name version url sha extra <<<"$line"
		case $kind in
		artifact)
			[[ -n $sha && -z $extra ]] || hw_checksums_die "$file:$number: want 'artifact NAME VERSION URL SHA256'"
			[[ $name =~ ^[a-z][a-z0-9-]*$ ]] || hw_checksums_die "$file:$number: invalid artifact name $name"
			[[ -z ${HW_ARTIFACT_URL[$name]:-} ]] || hw_checksums_die "$file:$number: duplicate artifact $name"
			[[ $version =~ $HW_SEMVER_PATTERN ]] || hw_checksums_die "$file:$number: $name version $version is not a release version"
			if [[ -n ${HW_PINNED_VERSIONS[$name]:-} && $version != "${HW_PINNED_VERSIONS[$name]}" ]]; then
				hw_checksums_die "$file:$number: $name must be the pinned version ${HW_PINNED_VERSIONS[$name]}, not $version"
			fi
			[[ $url =~ $HW_URL_PATTERN ]] || hw_checksums_die "$file:$number: $name URL must be an HTTPS release URL"
			[[ $url == *"$version"* ]] || hw_checksums_die "$file:$number: $name URL does not name version $version"
			[[ $sha =~ $HW_SHA256_PATTERN ]] || hw_checksums_die "$file:$number: $name SHA-256 must be 64 lowercase hex digits"
			HW_ARTIFACT_URL[$name]=$url
			HW_ARTIFACT_VERSION[$name]=$version
			HW_ARTIFACT_SHA[$name]=$sha
			;;
		binary)
			# For binary records the second and third fields are path and digest.
			sha=$version
			[[ -n $sha && -z $url ]] || hw_checksums_die "$file:$number: want 'binary PATH SHA256'"
			[[ $name =~ ^/[A-Za-z0-9._/-]+$ && $name != *..* ]] || hw_checksums_die "$file:$number: binary path must be absolute"
			[[ -z ${HW_BINARY_SHA[$name]:-} ]] || hw_checksums_die "$file:$number: duplicate binary $name"
			[[ $sha =~ $HW_SHA256_PATTERN ]] || hw_checksums_die "$file:$number: $name SHA-256 must be 64 lowercase hex digits"
			HW_BINARY_SHA[$name]=$sha
			;;
		*)
			hw_checksums_die "$file:$number: unknown record type $kind"
			;;
		esac
	done <"$file"
}

# hw_checksums_require fails unless every named artifact is present.
hw_checksums_require() {
	local name
	for name in "$@"; do
		[[ -n ${HW_ARTIFACT_URL[$name]:-} ]] || hw_checksums_die "checksums file has no artifact $name"
	done
}

hw_checksums_require_manifest() {
	local path
	for path in "${HW_MANIFEST_BINARIES[@]}"; do
		[[ -n ${HW_BINARY_SHA[$path]:-} ]] || hw_checksums_die "checksums file has no binary digest for $path"
	done
}

# hw_fetch downloads an artifact over HTTPS and verifies its SHA-256.
hw_fetch() {
	local name=$1 dest=$2
	curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
		--retry 3 --output "$dest" "${HW_ARTIFACT_URL[$name]}"
	printf '%s  %s\n' "${HW_ARTIFACT_SHA[$name]}" "$dest" | sha256sum --check --strict --quiet ||
		hw_checksums_die "SHA-256 mismatch for $name from ${HW_ARTIFACT_URL[$name]}"
}

# hw_write_manifest writes the D9 node-local digest manifest in sha256sum
# format from the operator-supplied binary digests.
hw_write_manifest() {
	local manifest=$1 path
	mkdir -p "$(dirname "$manifest")"
	: >"$manifest.tmp"
	for path in "${HW_MANIFEST_BINARIES[@]}"; do
		printf '%s  %s\n' "${HW_BINARY_SHA[$path]}" "$path" >>"$manifest.tmp"
	done
	chmod 0644 "$manifest.tmp"
	mv "$manifest.tmp" "$manifest"
}

# hw_verify_manifest checks every listed binary; any mismatch, missing file,
# or malformed line fails.
hw_verify_manifest() {
	local manifest=$1
	[[ -s $manifest ]] || hw_checksums_die "digest manifest $manifest is missing or empty"
	sha256sum --check --strict "$manifest" || hw_checksums_die "binaries do not match digest manifest $manifest"
}

# hw_artifact_summary prints the validated artifact pins for evidence.
hw_artifact_summary() {
	local name
	for name in $(printf '%s\n' "${!HW_ARTIFACT_URL[@]}" | sort); do
		printf 'artifact %s %s %s %s\n' "$name" "${HW_ARTIFACT_VERSION[$name]}" "${HW_ARTIFACT_URL[$name]}" "${HW_ARTIFACT_SHA[$name]}"
	done
}

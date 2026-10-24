# Security policy

Brie is experimental and is not currently a functional or qualified Kubernetes
node agent. Do not use it for production workloads.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting at:

https://github.com/ameukam/brie/security/advisories/new

Include the affected revision, reproduction steps, impact, and any proposed
mitigation. Do not include credentials or tenant data.

If private reporting is unavailable, open an issue requesting a private contact
channel without disclosing vulnerability details. Do not publish an exploit or
sensitive reproduction data while arranging that channel.

## Supported versions

Before the first stable release, security fixes target `main`. Once stable
releases exist, fixes target `main` and the latest stable release. Older releases
and dated nightlies do not have an independent security-support commitment.

## Release verification

Verify release checksums against their signed GitHub provenance as described
in [README.md](README.md#releases). Checksums alone detect corruption; they do
not authenticate an untrusted download.

The runtime and recovery requirements in [DESIGN.md](DESIGN.md) are design
constraints, not established security guarantees.

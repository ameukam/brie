# Maintaining Brie

Brie has one maintainer. Keep changes small, use pull requests, and check the
results before merging. Do not configure approval requirements that cannot be met.

## Repository settings

Enable private vulnerability reporting, Dependabot alerts, and security updates.
Use the checked-in [CodeQL workflow](../.github/workflows/ci.yml) rather than
enabling a duplicate default setup.

Keep the default Actions token read-only and require approval for outside
contributors' workflows. Protect `main` from force pushes and deletion, and
require the `Tests` and `CodeQL` checks. Review release-tag permissions before
publishing.

## Releases

Run `make build check` and review the changelog before tagging a stable release.
Move the unreleased entries into a dated version section, then push a new
semantic version tag pointing to a commit on `main`.

The [release workflow](../.github/workflows/release.yml) reruns CI, builds Linux
amd64 and arm64 archives, and creates a draft release. It uploads checksums and
signed build provenance before publishing.

Nightlies run at 03:17 UTC. Manual dispatch from `main` also creates a dated
prerelease; neither is marked as the latest stable release.

If a release fails, inspect the run and any draft or tag it left behind. Fix the
cause and use a new version or nightly attempt. Do not overwrite published
assets or move an existing tag.

To check packaging without publishing:

```sh
goreleaser release --snapshot --skip=publish
```

This writes to the ignored `dist/` directory. Move existing output aside before
repeating it. Local snapshots do not include GitHub Actions provenance.

## External reports

Check actual CodeQL and Scorecard results after workflow runs. Configuration
alone does not establish a security score. deps.dev controls its own indexing;
publishing a Go module does not guarantee immediate discovery.

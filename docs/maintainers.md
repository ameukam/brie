# Maintainer setup

The repository files configure checks and releases; they do not create or
configure a GitHub repository. Complete this setup before enabling publication.

## GitHub

1. Create or confirm the public repository `ameukam/brie` with `main` as its
   default branch, and publish the reviewed source. Enable GitHub Actions and
   allow the commit-pinned actions used by the workflows.
2. Enable private vulnerability reporting, Dependabot alerts, and Dependabot
   security updates. Use the checked-in advanced CodeQL workflow rather than
   enabling a second default CodeQL setup.
3. Set the default Actions token to read-only. Keep fork pull requests
   unprivileged and require approval for workflows from outside contributors.
4. Create a ruleset for `main` and any maintained release branches: require pull
   requests, two independent human approvals, code-owner review, passing `Tests`
   and `CodeQL` checks, and an up-to-date branch. Dismiss stale approvals, require
   approval of the most recent push, prohibit force pushes and deletion, and do
   not give administrators or bots a bypass.
5. Protect release tags and enable immutable releases. Keep release creation
   restricted to authorized maintainers and the approved release workflow.
   Ensure tag rules permit the workflow to create its dated nightly tags.
6. Confirm CodeQL and Scorecard results appear under code scanning, and that the
   Scorecard workflow successfully publishes its results.

Rulesets let Scorecard inspect branch requirements with the default GitHub token;
an administrative PAT is not needed. Never add a broad PAT just to improve a
score. `CODEOWNERS` identifies the current owner; the required independent human
reviewers must actually be available.

## Stable and nightly releases

Before a stable release, review the `Unreleased` changelog, move its entries into
a dated version section, and make sure the required checks pass on `main`.
An authorized maintainer then creates and pushes a semantic version tag such as
`v0.1.0` pointing to a commit on `main`.

The release workflow reruns CI before building. GoReleaser creates a draft
GitHub release containing both Linux archives and `checksums.txt`. GitHub
Actions generates signed SLSA build provenance for those artifacts. Only after
the bundle and signed in-toto envelope have been uploaded is the draft
published. This does not claim a particular SLSA assurance level.

Nightlies use the same checks and publishing path at 03:17 UTC, with unique
dated prerelease tags. Manual dispatch also produces a nightly and is allowed
only on `main`. A rerun gets a new attempt suffix rather than rewriting a tag.
Nightlies are not promoted to the latest stable release. GitHub can delay
scheduled workflows or disable them after inactivity; monitor their status.

Release failures can leave a draft or a tag behind. Inspect the failed run;
never overwrite published assets or move an existing tag. Correct the problem
through review and use a new version or nightly attempt.

To check packaging locally from a Git checkout without publishing:

```sh
goreleaser release --snapshot --skip=publish
```

GoReleaser writes `dist/`, which is ignored. Move an existing local `dist/` aside
before repeating the command. Local snapshots have checksums but cannot obtain
the GitHub Actions OIDC provenance used by published releases.

## deps.dev and OpenSSF Scorecard

The target is to satisfy every applicable Scorecard check, not to manufacture
scores or replace operational evidence with configuration.

| Area | Repository support | Remaining requirement |
| --- | --- | --- |
| License and security policy | Apache-2.0 and `SECURITY.md` | Enable and monitor private reporting. |
| Tests, fuzzing, SAST, vulnerabilities | Race tests, CLI fuzzing, CodeQL, govulncheck | Successful real runs, remediation, and ongoing maintenance. |
| Dependencies and workflow safety | Dependabot, pinned Actions, scoped tokens | Review updates; configure repository permissions and rulesets. |
| Packaging and signed releases | GoReleaser, checksums, signed provenance | Publish actual releases and verify the resulting downloads. |
| Branch protection and code review | `CODEOWNERS` and contribution policy | Enforce rules and obtain independent human reviews. |
| Maintenance and contributors | Contributor guidance | Sustained activity and genuine contributor participation. |
| OpenSSF Best Practices | Documented development and security practices | Enroll and supply evidence for the badge; full credit requires the gold criteria. |
| deps.dev discovery | A public Go module path and semantic versions | Publish the module and allow upstream indexing to discover it. |

GoReleaser packages and individual practices may not be recognized by every
Scorecard heuristic. Review the actual results rather than assuming a green
workflow means all checks scored 10. Agent reviews do not count as human
code review. A new project cannot immediately demonstrate a maintenance history
or contributor diversity.

Scorecard's local directory mode recognizes license filenames but does not
classify their text. A canonical `LICENSE` can therefore score 9/10 locally.
Keep the Apache-2.0 text intact and verify GitHub's license identification after
publishing; GitHub-based Scorecard runs use that API.

deps.dev indexing and refresh are controlled by that service; there is no
repository workflow that guarantees immediate inclusion or a particular score.
See the [deps.dev FAQ](https://docs.deps.dev/faq/) and
[Scorecard check definitions](https://github.com/ossf/scorecard/blob/main/docs/checks.md).

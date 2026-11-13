# Observability and serving

This guide describes Brie's structured agent log, kubelet-compatible serving
endpoint, metrics, Events, probes, and container log rotation (DESIGN.md
section 12). Everything here is tested against fakes: an `httptest` API server,
a fake CRI runtime, and local files. None of it is hardware-verified, and it is
not Node Conformance evidence.

## Structured agent log

`internal/agentlog` writes JSON records through `log/slog`. Correlation
fields are `pod_uid`, `operation_id`, and `journal_stage`. Attribute values
whose keys name environment values, commands, arguments, request bodies,
headers, tokens, passwords, secrets, credentials, keys, stdin, or snapshots are
replaced with `[REDACTED]`, including inside groups. Redaction is key-based:
messages and error strings are written verbatim and must never contain tenant
data. Identical warning-or-higher records are limited to five per minute per
message and attributes; the next record after a window reports
`suppressed_repeats`. The limiter tracks at most 1,024 keys.

## Serving endpoint

`internal/serving` implements the kubelet API subset that `kubectl logs`,
`kubectl exec`, `kubectl attach`, `kubectl port-forward`, and metrics scrapers
use. It listens on `:10250` by default with TLS 1.2 or later.

Authentication accepts either a client certificate verified against the
client CA file (user = subject common name, groups = organizations plus
`system:authenticated`) or a bearer token checked with a TokenReview.
Anonymous requests are rejected. Authorization creates a SubjectAccessReview
for `nodes/<subresource>` on the configured Node name, with kubelet's mapping:

| Path | Subresource | Verb |
| --- | --- | --- |
| `/stats/*` | `stats` | from method |
| `/metrics/*` | `metrics` | from method |
| `/logs/*` | `log` | from method |
| `/healthz` | `healthz`, then `proxy` | from method |
| `/exec`, `/attach`, `/portForward` | `proxy` | always `create` |
| anything else, including `/containerLogs` | `proxy` | from method |

The method maps GET to `get`, POST to `create`, PUT to `update`,
PATCH to `patch`, and DELETE to `delete`; other methods are refused. Streaming
endpoints always need `create`, so a WebSocket GET with read-only access
cannot open a session. A TokenReview or SubjectAccessReview error refuses the
request, and an error is never followed by the `healthz` to `proxy` fallback.
Results are not cached, so each request costs one or two API calls. Unclean or
escaped request paths are refused before authorization. At most 64 requests,
including streams, are served at once.

The API server reaches the endpoint with its kubelet client certificate. Give
that identity `nodes/proxy` access, for example through the
`system:kubelet-api-admin` ClusterRole, and set the API server's
`--kubelet-certificate-authority` to the CA that signed the serving certificate.

Endpoints:

- `GET /healthz` returns `ok`.
- `GET /metrics` serves the agent's Prometheus registry.
- `GET /containerLogs/{namespace}/{pod}/{container}` reads the current CRI
  log file. It supports `follow`, `tailLines` (at most 100,000), `limitBytes`,
  `timestamps`, `sinceSeconds`, and `sinceTime`. A `tailLines` start beyond
  the 64 MiB backward scan bound is refused with HTTP 422 before any output.
  Every response, including a follow session, stops at 128 MiB. `previous=true` is refused because Brie
  never restarts containers. Rotated files are not read, except that a follow
  session drains the file being rotated before switching to the new one.
- `/exec/{namespace}/{pod}/{container}` and
  `/attach/{namespace}/{pod}/{container}` call CRI `Exec` or `Attach` with
  kubelet's `input`, `output`, `error`, `tty`, and repeated `command` query
  parameters;
  `/portForward/{namespace}/{pod}` calls CRI `PortForward` with optional
  `port`/`ports` parameters (at most 16). Brie proxies the session to the
  runtime's streaming URL instead of redirecting, refuses URLs that are not
  loopback IP addresses, and strips `Authorization`, `Cookie`,
  `Impersonate-*`, and `X-Remote-*` headers.

A Pod is served only from a sandbox labeled `brie.dev/execution-owner` that
uses the configured runtime handler. When several match a namespace and name,
the newest is used. The log file must be the runtime-reported log path, a
plain regular file directly inside the Pod's own log directory
(`<pod-log-dir>/<sha256(pod UID)>`); other paths and symlinks are refused.

Exec and attach need a ready sandbox and a running container; port-forward
needs a ready sandbox. When a lifecycle-extension state source is configured,
a sandbox in any recorded state other than `Running` is refused with HTTP 409
and the state name, and an unavailable state is refused with HTTP 503. The
server never resumes, pauses, or otherwise mutates a sandbox. Brie has no
lifecycle extension yet, so no state source exists in this build.

Server log records for failed requests carry an operation name and a bounded
error code (a gRPC code, `HTTP<status>` for API server errors, `FileError`,
`NetworkError`, `Canceled`, `DeadlineExceeded`, or `Unknown`), never the text of
runtime, streaming proxy, or review errors, which can echo commands or
credentials.

Not implemented: `/stats`, `/logs`, `/pods`, `/configz`, `/checkpoint`, the
UID forms of the streaming paths, serving certificate rotation, and caching of
authentication and authorization results.

## Metrics

`internal/metrics` builds the registry served at `/metrics`. Every label takes
a value from a fixed set; anything else is reported as `Other` or `other`. No
Pod name, namespace, UID, image, or error text becomes a label.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `brie_node_admission_decisions_total` | `reason`: `Admitted`, `UnsupportedPod`, `RuntimeClassOverheadMismatch`, `UnsupportedResources`, `InsufficientResources`, `Other` | Admission decisions |
| `brie_node_reserved_{cpu_cores,memory_bytes,executions}` | none | Unfinished journal reservations, including overhead; no samples until the first journal save |
| `brie_node_budget_{cpu_cores,memory_bytes,executions}` | none | Configured envelope; no samples until set |
| `brie_node_last_complete_reconcile_age_seconds` | none | Age of the last error-free pass; `NaN` before the first |
| `brie_node_admission_barrier` | `cause`: `IncompletePass`, `UnresolvedExecution`, `RuntimeUnhealthy`, `RecoveryRequired`, `Other` | 1 for the active cause; no samples until first observed |
| `brie_node_oldest_pending_stage_age_seconds` | `stage`: `reserved`, `sandbox-pending`, `container-pending`, `start-pending` | Oldest pending record per stage; `NaN` when a pending record's entry time is unknown; no sample when none is pending |
| `brie_node_cri_request_duration_seconds` | `method`: known CRI methods or `other` | CRI call latency |
| `brie_node_cri_request_errors_total` | `method`, `code` (gRPC code) | CRI call errors |
| `brie_node_image_pull_duration_seconds`, `brie_node_image_pull_failures_total` | none | `PullImage` duration and failures |
| `brie_node_journal_{records,bytes}` and `_limit` | none | Journal size after the last successful save against its 10,000-record and 4 MiB bounds; no samples until the first save |
| `brie_node_lease_renewal_failures_total` | none | Failed Lease renewals |
| `brie_node_probe_results_total` | `probe`, `handler`, `result` | Probe outcomes |
| `brie_node_container_log_rotations_total` | `result`: `rotated`, `reopen_failed`, `failed` | Log rotation attempts |
| `brie_node_events_total` | `result`: `recorded`, `deduplicated`, `dropped` | Events after bounds |

Go runtime and process collectors are also registered. CRI latency, errors,
and pull metrics come from an observer installed on a `cri.Client`
(`SetObserver`); the other agent signals come from executor hooks (see
"Executor hooks" below). A gauge that was never set is not exported, so a
missing sample means "not observed", never zero.

## Events

`internal/events` records core/v1 Events through client-go's
`record.EventBroadcaster` with source component `brie-node` and the node name
as host. Reasons are stable:

| Reason | Object | Type |
| --- | --- | --- |
| Published admission reason, such as `UnsupportedPod` or `InsufficientResources` | Pod | Warning |
| `ImagePullFailed`, `ImagePullBackOff` | Pod | Warning |
| `ExecutionBlocked` condition reason, such as `ExecutionConfigChanged` | Pod | Warning |
| `CleanupBlocked` | Pod | Warning |
| `ReadinessProbeFailed`, `LivenessProbeFailed` | Pod | Warning |
| `AdmissionBarrierRaised` | Node | Warning |
| `AdmissionBarrierLowered` | Node | Normal |

Bounds, in order: an identical Event (object, reason, and message) is
suppressed for five minutes; a token bucket allows one Event per second with a
burst of 25 for the whole agent; client-go's correlator then aggregates
similar Events and spam-filters per object; messages are cut to 1,024 bytes.
The deduplication cache holds at most 4,096 keys. Events supplement status:
every refusal must still be published on the Pod or Node.

Node Events are created in the `default` namespace and are readable by anyone
who can read Events there, so barrier messages carry only the bounded cause,
never a Pod name. Pod Events go to the Pod's namespace.

RBAC: the Node authorizer's static node rules let a node identity
(`system:node:<name>` in group `system:nodes`) `create`, `update`, and `patch`
`events` in the core and `events.k8s.io` groups, in any namespace. Brie needs
no additional role. NodeRestriction does not restrict Events.

## Probes

`internal/probe` runs readiness and liveness probes per Pod UID: `exec`
through CRI `ExecSync`, and `httpGet` and `tcpSocket` against the Pod IP and a
declared container port. It honors `initialDelaySeconds` (from the container
start time), `periodSeconds`, `timeoutSeconds`, and the success and failure
thresholds. At most 16 probes run at once and at most 110 Pods are tracked by
default. HTTP probes send `User-Agent: kube-probe/brie`, use no proxy, read at
most 10 KiB of the body, treat 200-399 as success, and do not follow redirects
to another host (such a redirect counts as success, as in kubelet).

With `--enable-probes`, the executor adds a Pod to the manager when it observes
its container running, removes it on deletion, terminal phase, or container
exit, and gates `Ready` and `ContainersReady` on `Status.Ready` for a Pod with
a readiness probe; a liveness failure stops the container through CRI and
fails the Pod (see `docs/pod-profile.md`). Exec probes use their own CRI client
with a two-minute deadline and 16 call slots. The manager tracks at most
`--max-pods` Pods.

Readiness starts false and becomes true after `successThreshold` consecutive
successes. Liveness failure latches after `failureThreshold` consecutive
failures; the manager never restarts or stops anything. A timeout or a non-zero
exit is a failure. A probe that could not run (for example, CRI unavailable)
counts against readiness but not against liveness, because it is not evidence
about the application. `Suspend(uid, state)` stops a Pod's probes for a
recorded extension state: it cancels probes queued for a concurrency slot or
running and returns once they finish, readiness becomes false, liveness
evidence is kept, and after `Resume` readiness must succeed again. A result
from a probe that started before a `Suspend` or `Resume` is discarded. `Add`
for a UID already probed replaces its entry and stops the previous probes. Probe output is never logged
or stored.

## Container log rotation

`internal/logrotation` checks every running container of an owned sandbox
(label `brie.dev/execution-owner`, configured runtime handler) every ten
seconds. When the runtime-reported log file, which must be a regular file
directly inside the Pod's log directory, exceeds the maximum size (1 MiB to
10 GiB), it removes the oldest rotated files so that at most `max files`
remain including the current one (2 to 100), renames the log to
`<name>.<UTC YYYYMMDD-HHMMSS>`, and calls CRI `ReopenContainerLog`. A reopen
error is ambiguous, because the runtime may have reopened the log and lost the
reply. Brie then restores the original name with a hard link, which never
replaces an existing file: if the runtime already created a new current log,
both files are kept; otherwise the runtime keeps writing to the same name. Rotated files are not compressed. Exited containers
are not rotated.

Directory cleanup removes `<pod-log-dir>/<sha256(UID)>` only for UIDs the
caller reports as journal-finished, only when no sandbox for that UID remains
in the CRI inventory, and never for an entry that is not a real directory.
Directories of unknown Pods are never removed. Without a finished-Pod source,
cleanup is disabled; the executor does not provide one yet.

## Agent flags

All services are off by default. Serving, Events, probes, and rotation require
`--enable-workloads` and its execution configuration; `--log-format` requires
`--run`.

| Flag | Default | Effect |
| --- | --- | --- |
| `--log-format=text\|json` | `text` | `json` turns agent log lines into redacted, rate-limited JSON records |
| `--enable-serving` | off | Serve the endpoint above, with the agent metrics registry |
| `--serving-address` | `:10250` | HTTPS listen address |
| `--tls-cert-file`, `--tls-private-key-file` | none | Absolute PEM serving certificate and key (required with serving) |
| `--client-ca-file` | none | Absolute PEM CA bundle for client certificates (required with serving) |
| `--enable-events` | off | Record bounded Events (see "Events") |
| `--enable-probes` | off | Accept, run, and enforce readiness and liveness probes (see "Probes") |
| `--container-log-max-size` | none | Enables rotation above this size, such as `10Mi` |
| `--container-log-max-files` | `5` | Files kept per container, including the current one |

Serving and rotation each use their own CRI client, separate from the
executor's. A service that fails, for example because the serving address is
in use, stops the agent; as on any exit, running workloads are not stopped.
The serving TLS files are read once at startup and are not reloaded.

What the wiring feeds today: CRI latency, errors, and pull metrics from every
Brie CRI client; the configured budget; the last complete pass age; Lease
renewal failures; the barrier cause (`RecoveryRequired`, `RuntimeUnhealthy`,
or `IncompletePass`) with Events when it changes; admission decisions by
reason, with a Pod Event for each published refusal; reservations, journal
size, and pending-stage age after each journal save; Pod Events for image pull
failure and backoff (with the gRPC code, never the image reference or runtime
error text), for a newly published `ExecutionBlocked` condition, and for
cleanup that waits for deletion or runtime evidence; and, with
`--enable-probes`, probe results and a `LivenessProbeFailed` Event.

## Executor hooks

`nodeagent.ExecuteObserved` passes `ExecutionHooks` into the executor. Each
hook observes a decision the executor already made and must not block:

| Hook | Call site | Fed to |
| --- | --- | --- |
| `CRI` | every executor CRI client | CRI and pull metrics |
| `LeaseRenewal`, `Pass`, `Health` | Lease loop, end of each pass, health loop | Lease failures, last pass age, barrier metric and Events |
| `Admission` | each published decision of DESIGN section 7.2: `Admitted` after the `reserved` record is saved, a refusal after its status is published | `admission_decisions_total`; refusal Event |
| `Journal` | every successful journal save | reservations, journal size, pending-stage age |
| `ImagePullFailed` | a Pod's image wait reports `ErrImagePull` (once per failure) or `ImagePullBackOff` | `ImagePullFailed` or `ImagePullBackOff` Event |
| `ExecutionBlocked` | the `brie.dev/ExecutionBlocked` condition is newly written | Event with the condition reason |
| `CleanupBlocked` | cleanup of an ambiguous execution, a deletion without usable list evidence, or a deleted Pod whose sandbox creation is unresolved | `CleanupBlocked` Event |
| `ProbeResult`, `LivenessFailed` | probe outcomes; a Pod failed after its liveness probe failed | `probe_results_total`; `LivenessProbeFailed` Event |

Pending-stage age needs the time a record entered its stage. Each journal
record carries `PendingStage` and `PendingSince`, stamped by the save that
commits a pending stage before its CRI call and cleared when it leaves. A
pending record from an older journal has no entry time; it keeps
`PendingSince` empty until it leaves the stage, and its stage's age is `NaN`.
These fields are optional, so the journal version is unchanged; an older agent
refuses a journal that carries them, as it refuses any unknown field.

Still missing:

1. Barrier cause detail. `UnresolvedExecution` is never reported because the
   executor folds unresolved executions into its pass error and recovery flag;
   the pass hook must report the cause to separate it.
2. Readiness failure Events. `ReadinessProbeFailed` exists in `internal/events`
   but no hook records it.
3. Finished Pods for log cleanup. Provide `logrotation.FinishedPods` from the
   reconcile loop, because the journal map is not safe for concurrent reads.
   Until then, log directory cleanup stays disabled.
4. Extension states. The lifecycle extension must provide
   `serving.ExtensionStates` and call `probe.Manager.Suspend` and `Resume`.
   Brie has no lifecycle extension yet, so no state is ever reported.

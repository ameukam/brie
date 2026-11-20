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
  `timestamps`, `sinceSeconds`, and `sinceTime`. Every response, including a
  follow session, stops at 128 MiB. `previous=true` is refused because Brie
  never restarts containers. Rotated files are not read, except that a follow
  session drains the file being rotated before switching to the new one.
- `/exec/{namespace}/{pod}/{container}` and
  `/attach/{namespace}/{pod}/{container}` call CRI `Exec` or `Attach`;
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
| `brie_node_reserved_{cpu_cores,memory_bytes,executions}` | none | Reservations, including overhead |
| `brie_node_budget_{cpu_cores,memory_bytes,executions}` | none | Configured envelope |
| `brie_node_last_complete_reconcile_age_seconds` | none | Age of the last error-free pass; `NaN` before the first |
| `brie_node_admission_barrier` | `cause`: `IncompletePass`, `UnresolvedExecution`, `RuntimeUnhealthy`, `RecoveryRequired`, `Other` | 1 for the active cause; no samples until first observed |
| `brie_node_oldest_pending_stage_age_seconds` | `stage`: `reserved`, `sandbox-pending`, `container-pending`, `start-pending` | Oldest pending record per stage; no sample when none is pending |
| `brie_node_cri_request_duration_seconds` | `method`: known CRI methods or `other` | CRI call latency |
| `brie_node_cri_request_errors_total` | `method`, `code` (gRPC code) | CRI call errors |
| `brie_node_image_pull_duration_seconds`, `brie_node_image_pull_failures_total` | none | `PullImage` duration and failures |
| `brie_node_journal_{records,bytes}` and `_limit` | none | Journal size against its 10,000-record and 4 MiB bounds |
| `brie_node_lease_renewal_failures_total` | none | Failed Lease renewals |
| `brie_node_probe_results_total` | `probe`, `handler`, `result` | Probe outcomes |
| `brie_node_container_log_rotations_total` | `result`: `rotated`, `reopen_failed`, `failed` | Log rotation attempts |
| `brie_node_events_total` | `result`: `recorded`, `deduplicated`, `dropped` | Events after bounds |

Go runtime and process collectors are also registered. CRI latency, errors,
and pull metrics come from an observer installed on a `cri.Client`
(`SetObserver`); the other agent signals need executor hooks (see "Executor
hooks" below).

## Events

`internal/events` records core/v1 Events through client-go's
`record.EventBroadcaster` with source component `brie-node` and the node name
as host. Reasons are stable:

| Reason | Object | Type |
| --- | --- | --- |
| Published admission reason, such as `UnsupportedPod` or `InsufficientResources` | Pod | Warning |
| `ImagePullFailed` | Pod | Warning |
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

Readiness starts false and becomes true after `successThreshold` consecutive
successes. Liveness failure latches after `failureThreshold` consecutive
failures; the manager never restarts or stops anything. A timeout or a non-zero
exit is a failure. A probe that could not run (for example, CRI unavailable)
counts against readiness but not against liveness, because it is not evidence
about the application. `Suspend(uid, state)` stops a Pod's probes for a
recorded extension state: readiness becomes false, liveness evidence is kept,
and after `Resume` readiness must succeed again. Probe output is never logged
or stored.

## Container log rotation

`internal/logrotation` checks every running container of an owned sandbox
(label `brie.dev/execution-owner`, configured runtime handler) every ten
seconds. When the runtime-reported log file, which must be a regular file
directly inside the Pod's log directory, exceeds the maximum size (1 MiB to
10 GiB), it removes the oldest rotated files so that at most `max files`
remain including the current one (2 to 100), renames the log to
`<name>.<UTC YYYYMMDD-HHMMSS>`, and calls CRI `ReopenContainerLog`. If the
runtime cannot reopen the log, the rename is undone so the runtime keeps
writing to the same name. Rotated files are not compressed. Exited containers
are not rotated.

Directory cleanup removes `<pod-log-dir>/<sha256(UID)>` only for UIDs the
caller reports as journal-finished, only when no sandbox for that UID remains
in the CRI inventory, and never for an entry that is not a real directory.
Directories of unknown Pods are never removed. Without a finished-Pod source,
cleanup is disabled; the executor does not provide one yet.

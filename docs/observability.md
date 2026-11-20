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

The method maps GET and HEAD to `get`, POST to `create`, PUT to `update`,
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

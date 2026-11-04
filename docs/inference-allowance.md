# Opt-in inference transport allowance

`codex_api::InferenceAllowance` bounds provider request attempts, complete logical
request bytes and an absolute monotonic deadline for one execution. Share the
same `Arc` across retries, HTTP/WebSocket fallback and continuation requests.
Attempts are claimed immediately before a physical transport send; no refund API
exists. Lost acknowledgments still consume their claim. A fresh allowance is not
recovery of an accepted or uncertain execution.

The execution owner must durably reserve its allocation before constructing this
value. The native integration under development adds mandatory
`turn/startBoundedIfIdle` with nested `turn` and required `allowance` fields.
An old server rejects the method; callers must never fall back to ordinary
`turn/start` or discard the allowance. The allowance specifies positive
`maxProviderAttempts`, complete `maxRequestBytes` within64MiB and a future
`deadlineUnixMs` within600seconds. The existing idle-only admission controls
busy refusal; ordinary turn methods retain their behavior.

Core prepares one shared allocation before settings are accepted, attaches it to
that physical turn rather than a sticky thread setting, and binds its sampling,
local compaction and foreground remote compaction clients to it. Speculative
compaction is suppressed for bounded turns because its ordinary lifetime is
session-owned; ordinary speculative compaction remains unchanged.
The regular-task lifetime is bounded by the same deadline and closes the
allocation on exit. Human steering does not erase the running turn's limits;
a subsequent independent ordinary turn inherits no allocation. Bounded
attribution is retained through history/compaction only to refuse reconstruction
on recovery. Automatic daemon recovery, direct `ContinueIfIdle` of that exact
bounded predecessor and queue-only durable-sleep restoration cannot manufacture
a fresh allocation. Independent `StartIfIdle` and explicit trigger-mail work keep
their existing authority; a previous bounded turn does not prohibit unrelated
persisted goal continuation. Chronological adjacency is not causal ownership.

These are source changes under qualification, not receiving deployment or a
working heartbeat. Durable parent accounting is owned by DaimonApp#452/PR590.
Native descendant/reviewer inference, external tools, actual recovery and
cancellation journeys and the finite producer still require qualification before
enabling a heartbeat producer. Schema/client exports and the durable
producer-to-native allocation binding have focused source qualification, not
receiving acceptance. Bounded lifecycle callbacks leave the existing thread goal
and its accounting untouched. The existing extension-tool execution boundary
rejects bounded `create_goal` and `update_goal`, while retaining `get_goal` and
authenticated human goal controls. An independently authorized goal may resume
while attention is busy and continue afterward with its own authority. No account,
model attention or periodic producer is activated by this component.

For HTTP, build the existing configured client with
`HttpClientBuilder::without_replays()`. It disables redirects and transparent
protocol retries; route-aware TLS replay is also disabled. An unqualified
transport is refused without consuming an attempt. A routed client that loses
this property in legacy construction is rejected before sending. Normal clients
retain their existing redirects, retry and TLS behavior. The native login helper
uses the same existing proxy, cookie, CA and account/network policy.

The HTTP deadline covers authentication, retry/backoff waits, request/headers and
streamed response bytes. Validate the logical body before compression and after
request configuration. Authentication may sign the prepared body and change
headers; it may not replace a bounded request's validated body. WebSocket
callers supply the full logical request size before incremental encoding; the
API separately checks the actual frame. Core includes final metadata and the
complete context in that measurement. The pump claims immediately before send,
and the same absolute deadline bounds queue/socket acquisition and response.

`close()` fences later attempts. It does not cancel already-active inference;
the execution owner must use the existing native cancellation path and retain
terminal versus uncertain observation. These are request/input/time bounds,
not a hard total-token, output-token or monetary ceiling. No model/provider,
account, goal or tool authority is granted by this value.

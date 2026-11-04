# Opt-in inference transport allowance

`codex_api::InferenceAllowance` bounds provider request attempts, complete logical
request bytes and an absolute monotonic deadline for one execution. Share the
same `Arc` across retries, HTTP/WebSocket fallback and continuation requests.
Attempts are claimed immediately before a physical transport send; no refund API
exists. Lost acknowledgments still consume their claim. A fresh allowance is not
recovery of an accepted or uncertain execution.

The execution owner must durably reserve its allocation before constructing this
value. The current library increment does not expose a new native turn parameter,
reserve a heartbeat budget or activate attention. `ModelClientSession` accepts an
already-reserved allowance through its explicit builder; ordinary sessions remain
unchanged. Durable parent/descendant accounting, compaction/reviewer inheritance,
external tools, restart recovery and native admission still require their owning
integration before a heartbeat producer can use this contract.

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

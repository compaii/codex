# Opt-in inference transport allowance

`codex_api::InferenceAllowance` bounds provider request attempts, complete logical
request bytes and an absolute monotonic deadline for one execution. Share the
same `Arc` across retries, HTTP/WebSocket fallback and continuation requests.
Attempts are claimed immediately before a physical transport send; no refund API
exists. Lost acknowledgments still consume their claim. A fresh allowance is not
recovery of an accepted or uncertain execution.

The execution owner must durably reserve its allocation before constructing this
value. The native integration exposes mandatory
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

Actual receiving selection and end-to-end heartbeat evidence belong to the
[Matrix coverage checkpoint](https://github.com/AlterMundi/daimon-matrix/issues/437#issuecomment-6597923440),
not this source guide. The existing DaimonApp owner supplies durable parent
attempt accounting and finite/standing producer controls. Those attempt
reservations are separate from monetary funding. Native descendant/reviewer and
external credential/tool routes retain their own qualification requirements.
Bounded lifecycle callbacks leave the existing thread goal
and its accounting untouched. The existing extension-tool execution boundary
rejects bounded `create_goal` and `update_goal`, while retaining `get_goal` and
authenticated human goal controls. An independently authorized goal may resume
while attention is busy and continue afterward with its own authority. Native agent-tool entry points also refuse a bounded spawn,
resume or message input before recipient loading/submission until an
owner-reserved child admission can carry the allocation. In0.167, V2 queue-only
mail can wake an ordinary recipient with `wake_on_agent_messages`, so it shares
this refusal; the queue-only mode alone does not prove that no inference starts.
Ordinary queue-only wakes also cannot replenish a bounded predecessor or leave
an active reservation after refusing it. This refusal covers both direct and Code Mode
execution; it is not a separately allocated delegation implementation. Independent
ordinary agent work is unchanged. The synchronous approval entry refuses bounded
actions before consulting cached or model contributors; an explicitly cancelled request remains an abort. The
async classifier invalidates old score coverage and returns before preparing
permissions, reserving a classification slot or launching model work. No approval
is inferred from the absent classifier. Positive reserved reviewer and external
routed admission remain unfinished. No account,
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

## Owner-connected per-attempt reservations

A bounded idle start may additionally select `providerReservationCycleId`, the
canonical UUID of an already authorized owner journal cycle. Older bounded-start
readers reject this unknown field; clients must never retry without it. Core
attaches the resulting gate to this physical turn only, not to saved thread
settings or a future human turn. An existing persistent host reservation policy
cannot be replaced by a caller's per-turn gate. The host-only callback types are
shared through `codex_protocol::provider_attempt` and reexported by `codex_api`;
they are not serializable provider or model input.

Before each physical provider send, `turn/providerAttempt/reserve` carries the
thread/cycle/attempt UUID, attempt ordinal, complete configured request-byte
ceiling and original absolute deadline to the initiating connection only. That
owner must use its existing durable accounting journal and verified conservative
cost contract before returning the exact cycle/attempt and `authorized: true`.
An unrelated subscriber cannot answer, reject, adopt or receive a reconnect
replay of the request. Missing/negative/malformed/mismatched responses, owner
disconnect or the original deadline refuse submission; callback cancellation
also covers a backpressured outgoing channel. Ordinary human approvals and
their replay semantics are unchanged. Exec/TUI clients do not implement the
account owner and refuse this request rather than presenting a human prompt.

Authorization is not proof that provider bytes were sent or that usage settled.
A lost response retains journal exposure and the original attempt identity.
This bridge neither creates accounts/prices nor authenticates a provider usage
receipt. Actual owner integration, trusted per-attempt usage, output/exposure
qualification and exclusive credential/tool routes remain separate requirements
before claiming monetary enforcement. No receiving package or account is selected
by this source interface.


## Historical image previews in bounded requests

A long-lived thread can retain inline historical images whose encoded bytes alone
exceed a finite request allowance. Bounded model requests prepare reduced previews
of images positively attributed to older turns through the existing image
resizer/cache. This changes the model request copy only. Persisted native history,
compaction installation inputs, all text, current-turn images and images without
known turn provenance remain unchanged. A harness-attributed observation explains
previews or explicit omission placeholders; it never invents human speech.

Half the complete request bound is reserved for the history projection; the
remaining space accommodates tools, instructions and final framing. Historical
inline images share only the space remaining after preserved input. Existing
finite resize modes are tried before an explicit placeholder. This heuristic
cannot authorize a request: final complete HTTP/WebSocket size validation stays
unchanged and still rejects any request that does not fit. Ordinary unbounded
requests use their original prompt. Local and foreground remote compaction pass
through the same request projection without installing reduced originals.

Qualification must cover oversized historical images reaching the first bounded
provider request within its original cap, unchanged current images/text, unknown
provenance, original history preservation, ordinary requests and both transports.
The source change alone is not installed receiving behavior or a successful
message/reply journey.

## Host-selected durable provider reservations

A trusted native host can construct an allowance with
`with_provider_attempt_gate(Arc<dyn ProviderAttemptGate>)`. Core also accepts that
opaque `Arc<dyn ProviderAttemptGate>` in the existing host-owned
`turn_extension_init`. This is typed in-process host data, never a serialized
turn parameter, model tool, peer message or prompt-selected callback.

Core evaluates the actual host attachment atomically with new-turn settings
admission, rather than using a stale preview. A selected gate requires a finite
turn allowance; refusal changes no thread settings. The admitted turn captures
that exact gate. Human steering keeps the running turn's existing limits and is
not refused merely because it carries no fresh allocation. Hosts select or clear
their attachment for their actual task/profile; a spent cycle gate must not be
copied into another execution or used as an ordinary-human default.

HTTP claims immediately before physical transport dispatch, including every
auth/retry attempt. WebSocket claims in the existing writer pump immediately
before the response-create frame. The same allowance/gate survives fallback.
Each gate request carries a fresh UUID, a monotonic per-execution ordinal, the
original absolute deadline and the complete allowance byte ceiling. The ceiling
is a conservative upper bound, not the current WS delta or measured input tokens.
It contains no credentials or prompt. The host must independently bind the exact
causal account, provider/model/route and qualified maximum charge to this object.

The host returns true only after retaining a durable reservation for that exact
UUID. For Matrix's schema5 journal, use the already implemented
`dispatch_provider_attempt` and exact operation identity; the callback enqueues
only the bounded authorization response, not a model/tool loop under its lock.
False, timeout or an unavailable result closes the allowance before provider
bytes are sent, so neither retries nor HTTP fallback bypass the refusal. The
in-memory claimed-slot count also includes refused/unknown reservations; it is
not proof of physical submission. Retained transport and usage receipts establish
those effects separately. A lost
reservation response retains exposure; cancellation/drop never refunds it.
The original absolute deadline also bounds reservation and is rechecked before
submission. Already admitted/in-flight exposure remains distinct from later pause.

This native gate is not a funding authority, bill estimator or usage receipt.
Owner/account selection, full-request conservative pricing, authenticated
per-attempt usage settlement and credential/tool-route exclusivity must still be
provided and qualified together before claiming real economic fencing. No new
budget, signer, API credential, account/provider switch, service, model attention
or production activation is supplied by this source increment. Legacy callers
without a selected gate retain their attempt/byte/deadline behavior.

## Exact-attempt completion evidence

`claim_authorized_attempt` returns an `AuthorizedProviderAttempt`, keeping the
same UUID and ordinal that the owner reserved, alongside the remaining duration.
The successful HTTP retry returns its own witness with its response stream;
failed physical attempts do not acquire the following response's usage.
The WebSocket writer pump returns the witness for that exact response-create
write. Non-inference control frames do not consume a provider attempt.

A selected host gate may implement `observe_usage(ProviderAttemptUsage)`.
Responses streaming reports the exact attempt/response IDs, optional server model,
optional token usage and unchanged provider usage metadata before model-side
interceptors. These fields are observations, not a signed financial settlement.
No usage, an absent observer or a lost callback retains maximum exposure.
Reporting inherits the original absolute attempt deadline; HTTP consumer closure
cancels it. Parsed WebSocket completion is retained before the callback so a
shorter cancellation-cleanup deadline cannot erase it.

For accounting on a reused WebSocket, a matching `response.created` is required;
a terminal without it remains visible but produces no usage association. Stale
or conflicting response IDs are refused before the observer. Each socket retains
at most1024 completed IDs of at most200bytes; saturation requires a fresh
connection for bounded execution instead of forgetting IDs. Ordinary unmetered
completion delivery remains compatible. The reported WebSocket model comes only
from that terminal's response headers, never unrelated socket metadata or an
assumed requested model. HTTP may use its request-bound response header.

Terminal `ResponseIncomplete` errors (for example `max_output_tokens`) also
report their exact response ID, reported tokens, optional upstream amount string
and raw usage metadata to the same owner before
the failed stream returns. They remain errors; no `Completed` event or zero-cost
settlement is synthesized. HTTP uses its own request binding. WebSocket requires
the same matching `response.created` witness and bounded terminal-ID history as
completion; an unbound incomplete response remains an error without a usage
association. Amount units/meaning require the selected accounting authority;
no price, currency or billed status is inferred from a string. Missing metadata
remains unknown, never inferred from tokens. Content-filter and explicit-interruption semantics remain unchanged.

Remote compaction v2 uses this Responses stream path: foreground through the
client session and background with the same inherited turn allowance. The unary
`EndpointSession::execute_with` route does not provide this completion observer.
Source routing is not a real-model or compaction acceptance claim.

A bounded start may explicitly select `providerUsageObservations: true` together
with its existing `providerReservationCycleId`. The app-server then sends
`turn/providerAttempt/usage` only to the initiating reservation owner. The closed
payload contains thread/cycle/attempt/ordinal/response identity and optional model,
token usage and provider metadata, capped at32KiB. Other subscribers cannot list,
replay or acknowledge it. Cancellation retires the outstanding callback; all
reporting remains inside the original deadline. Omission retains Core17 behavior;
older readers reject the explicit new field instead of silently accepting a
requested observation contract.

A funded owner can additionally select positive `providerMaxOutputTokens` on
that same bounded start. Its immutable gate applies `max_output_tokens` before
HTTP/WS compression, byte validation and sends, including retries and remote
compaction v2's inherited Responses stream. Raw HTTP callers keep a tighter
explicit ceiling; malformed ceilings refuse before a physical attempt. Typed
request serialization preserves raw tool payloads. WebSocket byte admission adds
the inserted-field delta to the complete logical request size, even when its
wire packet carries only a continuation. Old bounded readers reject the new
option; ordinary callers without it retain their prior behavior. An actual
provider must still be qualified to honor the requested ceiling: a synthetically
observed field does not prove real endpoint billing enforcement.

The exact `cycleId`/`attemptId`/`recorded` reply acknowledges retained observation
only. It grants no provider call and settles no money. Signing/verifying exact
usage under the existing accounting authority, full exposure/credential-route
qualification and receiving adoption remain unfinished. No new balance, signer,
account, provider selection, model call or runtime activation follows here.


## Host-only transient tool ceiling

An embedding host can additionally set `TurnStartOptions.tool_ceiling` to an
explicit list of `ToolName` values. `None` preserves ordinary selection; an empty
list exposes no tools. This option requires a finite inference allowance and
idle-only admission. It is not currently an app-server JSON option.

Core captures the list in a private turn attachment, intersects it with the
startup `ToolPolicy`, and uses that intersection before constructing core,
extension, hosted, dynamic and MCP tool exposure. Generated Code Mode tools must
also be explicitly included. Restrictive sandbox/unified-exec requirements are
combined with OR; exposing additional permissions requires both policies to allow
it. The ceiling never changes persistent thread configuration or the next
independent human turn. Review-context construction retains the attachment.

Dispatch checks the effective policy before invocation reservation, environment
selection, hooks or handlers. A retained restricted context requires the same
currently active turn; an older unrestricted context cannot use its authority
while a restricted turn is active. Existing unrestricted dispatch remains
compatible when neither context is restricted.

Bounded turns, including allowance-only starts, can resolve and interact with
only terminals launched under their exact originating turn ID. Ownership is
captured privately at launch. Resolution checks it before returning a terminal
handle; use checks it again for cached handles before acquiring the terminal's
interaction lock or taking empty-poll, interrupt or disabled-approval shortcuts.
Rejected borrowing neither consumes output nor writes, interrupts or terminates
the other turn's process. Ordinary human turns keep their existing cross-turn
terminal access and approval policy.

A unified-exec process opened by a finite turn is bound to that same allowance's
close and absolute deadline, before any post-spawn await or store insertion.
Normal return, cancellation/forced task drop and explicit allowance refusal all
use the existing close path. A closure racing startup still triggers cleanup;
an already closed or expired execution cannot start another process. Completing
tools after the final allowed provider attempt does not reserve another attempt.

Each retained process owns one cleanup task, cancelled when that process is
dropped; the existing 64-process store bound remains unchanged. The task waits
without holding the process strongly, then uses existing termination with a
two-second response bound and records failure before fallback termination. It
neither closes other turns' processes nor installs a producer or model wakeup.
Remote termination confirmation, detached descendants/independent process groups
and Code Mode sessions retain their separate qualification requirements; this is
not a process-containment or complete financial-isolation claim.

A host can also set `TurnStartOptions.permission_ceiling` to an already resolved
`PermissionProfile` (including a profile resolved from the existing named-profile
configuration). This requires managed restrictive permissions, explicit tools and
a finite allowance. It remains host-only; app-server starts leave it unset.

The ceiling intersects the captured local environment's original permissions,
then is captured privately with the turn. Each new step and resolved tool
argument reapplies it to the current environment view. Grants remain stored with
their original scope: filesystem access and native/executor command preparation
reapply the ceiling **after** merging session, turn, requested and internal grants.
Patch writes use the same final intersection. Cached approvals and exec-policy
skip rules cannot remove the required sandbox, broaden permissions or trigger an
unsandboxed retry. Missing platform enforcement refuses execution. An offline
ceiling removes managed proxy exceptions as well as network grants.

Captured shell snapshots and rebuilders from the broader environment are not
imported into the restricted view. Unqualified or starting environments retain
an attributed unavailable state for that step; they do not gate independent
qualified environments. A later step recaptures and qualifies the current view.
No qualified environment causes admission to fail. Remote executor qualification
for this host-local intersection is still unsupported. The next ordinary human
turn retains its original settings, grants and terminal access.

This is a tool-selection and dispatch boundary, not complete financial isolation.
This does not qualify every route used by an allowed tool or trusted host.
Running-process lifetime, Code Mode resources, trusted hooks/startup, local credential files,
AF_UNIX sockets, owner RPCs and provider reauthentication routes need separate
qualification. Restricting advertised names alone does not prove these routes
are inaccessible. No public bounded-start field, receiver configuration, paid
call, account change or production activation is introduced by this host option.

### Bounded Code Mode sessions

A trusted embedding host supplies `BoundedCodeModeOptions(CodeModeSessionOptions)`
through the existing `WithTurnExtensionData` initializer. Core privately captures
that policy for each new turn; later attachment changes cannot replace its limits.
Selecting this policy, a host tool ceiling or a permission ceiling makes positive
heap and observation limits mandatory for Code Mode. Ordinary turns and legacy
allowance-only attention retain their existing thread session; the legacy path
is not qualified for monetary isolation and must migrate before funded rollout.
This is not a model-selected option or a public bounded-start JSON field.

Host-restricted bounded turns use the provider's process-isolated session operation. Unsupported
providers refuse before opening an ordinary/shared session. Core's local gRPC
provider gives each bounded turn its own host process. Session close, turn drop,
service shutdown and the original allocation deadline close only that session;
private host teardown also prevents transport re-spawn. Bounded task interruption
must not interrupt the older human session's cells.

Cell identifiers, responses and nested-tool/notification callbacks carry a
turn-specific namespace. Wait and terminate reject another execution's identifiers
before contacting its host, even when separate runtime sessions both use numeric
cell IDs such as `1`. Output truncation follows the selected session's actual mode.

The existing heap option now reaches each V8 isolate instead of being discarded.
The gRPC opening event acknowledges the applied limits; a heap-limited client
rejects an older host that omits or changes that acknowledgement. V8 heap exhaustion
can abort a host process, which is why a shared-host fallback is forbidden here.
The isolate heap limit does not bound total host RSS, external buffers or retained
JSON state; owning process/container resource qualification remains necessary.
Credentials/IPC, trusted startup/hooks and exclusive provider routes remain
separate unfinished requirements before claiming complete financial isolation.

Local Code Mode host builds use the Codex-owned V8 artifacts selected by
`.github/actions/setup-rusty-v8/action.yml`, with the release manifest pinned in
`third_party/v8`. Set `RUSTY_V8_ARCHIVE` and `RUSTY_V8_SRC_BINDING_PATH` to those
checksum-verified artifacts, as CI does; the generic Deno release does not publish
this Codex sandbox variant. Retain a single useful version in bounded build
storage. Do not substitute an unverified artifact or start a source rebuild merely
because the generic downloader fails.

### Offline process preparation

The host permission ceiling is applied when selecting an execution proxy, before
Unified Exec prepares shell snapshots or broker credentials. An offline ceiling
withholds both an execution-owned proxy and the thread fallback; the broker cannot
select a credential-enabled shell on the basis of a broader thread configuration.
Final sandbox enforcement remains mandatory. This closes proxy-assisted credential
preparation, not inherited environment values, private credential files, arbitrary
IPC or provider routes; those still require complete-profile qualification. Human
and network-enabled host policies retain their existing behavior.

### Restricted broker fallback

Host-permission-ceiling Unified Exec turns refuse local fallback that restores
real brokered credentials when protected shell startup is missing or unsupported.
The rejection occurs before environment restoration and before process
launch. When the broker remains active, the existing unmasked-credential `Error`
mode also checks the final command environment. Ordinary turns retain their
existing fallback and warnings. Remote environments remain unsupported for this
host permission profile; this does not qualify arbitrary credentials/files/IPC,
shell startup, provider routes or the full funded profile.

### Actual Linux raw-process profile witness

The Core unit journey
`host_profile_raw_process_enforces_private_files_offline_ipc_and_empty_inheritance`
uses the real Unified Exec preparation, sandbox launcher and child process. An
existing finite host permission ceiling combines a denied fixture credential
file, one writable work directory and restricted networking. The ordinary shell
environment policy selects `inherit = "none"` with an explicit trusted
`PATH = "/usr/bin:/bin"`, and the command selects non-login startup. Empty
inheritance alone is insufficient when a development package relies on system
bubblewrap: removing PATH made launcher discovery fail before the command ran.
Do not restore the whole parent environment to repair that dependency.

The actual process cannot read the denied file or its parent's process environment,
cannot connect to the existing Unix owner-socket fixture, and can produce its
permitted work file. The following ordinary turn can read the fixture and connect
to that same socket; host restrictions did not become sticky human settings.
There is no provider request or live owner service in this journey.

This qualifies that Linux configuration and execution path, not every credential
location, startup file, inherited descriptor, Code Mode host, enabled-network
proxy/provider route or receiving package. The production owner must still select
and qualify its exact private roots, trusted binaries/startup, complete exposure
and real accounting authority. It is not automatic funded-profile activation.

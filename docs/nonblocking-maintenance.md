# Nonblocking native maintenance

Routine maintenance must preserve shared human admission while another turn is
running. Availability takes precedence over completing an update. A deployment
request never authorizes forced shutdown, paused goals, replay or global Stop.

## Incident and correction

On 2026-10-27 the selected older server handled SIGHUP by closing admission
immediately and waiting for live work. This rejected independent human requests
while another session was genuinely working. A separate diagnostic loaded an
entire large rollout into memory and triggered OOM; previous-package fallback
restored availability. Neither event proves candidate adoption or graceful exit.

The correction keeps maintenance pending with admission open for at most120s.
It claims an idle boundary only with no preparing starts, no Core task lifetime
guards and no host-reported running turns. Shared guards cover both the task
future and its abort owner through terminal persistence; delayed lifecycle events
cannot manufacture idle. Mailbox/durable-sleep starts acquire admission before
consuming pending input. Explicit parent-delegated child work can complete
while another Core task remains live; it cannot bypass a claimed idle boundary.
Mailbox delegation requires unambiguous parent lineage, revalidated atomically
against the queue consumed so a later unrelated message remains pending.

Final snapshot preparation has a five-second deadline. Maintenance abandons on
root persistence failure, a still-interrupted root, write/publish error or timeout,
reopening admission without disconnecting clients. Attempt-private staging paths
prevent late abandoned writers from publishing startup recovery. Explicit stop
retains its separate best-effort semantics; repeated maintenance is never force.
The final atomic rename is a local filesystem commit: no application watchdog can
guarantee progress if the operating system itself blocks that syscall.

## Receiving operation

Do not use the older SIGHUP path to close admission while unrelated work is live.
Human correction2026-10-27 removes the blanket signal prohibition: a controlled
scoped legacy replacement at verified idle is permitted. Do not hold the shared
daemon operation lock while
waiting for unrelated turns; it can itself block ordinary CLI attachment.
Retired adoption helpers are historical evidence, not executable recovery routes.

Use `codex app-server daemon maintenance` against a qualified receiver. It uses
only the private managed control socket, without acquiring daemon.lock. The
receiver must advertise the versioned nonblocking capability and its process ID
before the caller sends any lifecycle request. Managed Unix receivers enable
maintenance independently of the platform-specific explicit-shutdown socket;
unmanaged receivers expose neither maintenance capability nor its control route. Older receivers are refused
without sending a signal or a maintenance command. The caller waits at most133s
and reports the exact receiver's outcome: `deferred`, `alreadyPending`,
`supersededByExplicitStop`, or `handoffReady`. The last confirms a saved handoff,
not process exit or installation. A lost/expired response is unconfirmed. Never force, resend, select a package or start another writer after
an uncertain result. A fixed lease begins at socket receipt; enqueueing is bounded, and the receiver
rejects expired or abandoned requests before arming maintenance. Queue delay can
never create a fresh pending window after the observer expires. Preserve the exact snapshot and all
uncertain originals. Initial pinned local-package selection/restart is a separate
operation only after confirmed old-process exit, using the supported complete
local-package route.

Scheduled automatic updates prepare/select the available complete package through
the existing installer, but adopt it in the running daemon only after nonblocking
maintenance and observed exit of the retained process generation. The updater
releases the shared operation lock before waiting. Unsupported, deferred, lost or
handoff-ready-without-exit outcomes leave the running process alone and return to
the ordinary update schedule, without force or a tight retry loop. After exit it
reacquires the lock and revalidates the selected package and absence of another
receiver before starting a replacement; it preserves the recovery snapshot.
After `handoffReady`, a private atomic journal retains the exact process,
selected package and snapshot identity across bounded exit observations, lock
contention and updater re-exec. Later scheduled passes resume that continuation
without sending another maintenance request. Accepted handoffs use the existing
updater loop at a one-second continuation cadence, before installer/updater
adoption; pre-handoff deferral retains the ordinary update interval. Explicit Stop retires both recovery
and the journal; changed/consumed recovery cancels automatic replacement.
Explicit manual update, Stop and Restart retain their separate semantics.

For the initial older receiver, wait with admission open and inspect actual loaded
thread activity. When every loaded thread is idle, recheck the exact receiver
identity and perform one controlled SIGTERM, without force or repeated signals.
If busy or unknown, defer; do not substitute paused goals or global Stop. Require
confirmed exit and a fresh complete recovery snapshot without interrupted turns
before package selection. Retain the previous package and snapshot.

Use the existing complete-package install confirmation, not a new approval gate:
the installer describes whether a running daemon would restart. Cancel if it
would restart another receiver; confirm only a stopped replacement. Its existing
operation-lock recheck rejects a changed running state. Then use supported
`restart` so recovery is preserved; fresh `start` clears it. This is a finite
initial compatibility bridge, not the routine path for the repaired receiver.
Native metadata readiness and actual CLI/Telegram/Interaction admission are
separate receiving observations. Keep qualification, publication, installation and
human acceptance distinct.

## Qualification and diagnosis

The focused native-maintenance workflow exercises admission/shutdown units,
independent conversation admission while another turn is held, actual managed
idle snapshots, publish failure reopening, blocked persistence timeout and late
completion, plus Core input and task-lifetime contracts. Use mock providers;
production conversations and input replay are unnecessary for these regressions.

Read retained outcomes before retrying a failed operation. Inspect rollout tails
with bounded seek/read windows or stream selected records; never use whole-file
read_text/read_bytes/splitlines to inspect a long-lived session. A bounded-tail
miss is not proof that a terminal record does not exist. Keep diagnostics below
available host memory and preserve original evidence.

Parent plan: https://github.com/AlterMundi/telecodex/issues/301 (milestone9).
Continuous-voice delivery resumes after this availability repair; staged latency
optimizations are separate and must not override it.

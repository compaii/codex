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
consuming pending input.

Final snapshot preparation has a five-second deadline. Maintenance abandons on
root persistence failure, a still-interrupted root, write/publish error or timeout,
reopening admission without disconnecting clients. Attempt-private staging paths
prevent late abandoned writers from publishing startup recovery. Explicit stop
retains its separate best-effort semantics; repeated maintenance is never force.
The final atomic rename is a local filesystem commit: no application watchdog can
guarantee progress if the operating system itself blocks that syscall.

## Receiving operation

Do not send SIGHUP to an older server to install this correction. Refuse that
operation before acquiring the shared daemon lock. Do not hold that lock while
waiting for unrelated turns; it can itself block ordinary CLI attachment.
Retired adoption helpers are historical evidence, not executable recovery routes.

Use `codex app-server daemon maintenance` against a qualified receiver. It uses
only the private managed control socket, without acquiring daemon.lock. The
receiver must advertise the versioned nonblocking capability and its process ID
before the caller sends any lifecycle request. Older receivers are refused
without sending a signal or a maintenance command. The caller waits at most133s
and reports the exact receiver's outcome: `deferred`, `alreadyPending`,
`supersededByExplicitStop`, or `handoffReady`. The last confirms a saved handoff,
not process exit or installation. A lost/expired response is unconfirmed. Never force, resend, select a package or start another writer after
an uncertain result. A fixed lease begins at socket receipt; enqueueing is bounded, and the receiver
rejects expired or abandoned requests before arming maintenance. Queue delay can
never create a fresh pending window after the observer expires. Preserve the exact snapshot and all
uncertain originals. Package selection/restart is a separate operation only after
confirmed old-process exit, using the supported complete local-package route.

For the initial older receiver, retain a deployment gap until a supported safe
handoff actually exists. Do not substitute a global pause or destructive shutdown.
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

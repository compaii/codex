# Conditional goal controls (Interaction fork)

This additive native contract supports selected human goal controls from Daimon
Interaction. It does not grant authority or expand the model's goal tool policy.
The source is based on official `rust-v0.163.0` (83086188a); other memory forks
remain separate. Receiving adoption and real-session qualification are required.

- `thread/goal/snapshot` takes `threadId` and returns `goal` (possibly null) and
  an opaque `revision` from one consistent database snapshot.
- `thread/goal/conditionalSet` takes the existing goal-set fields plus required
  `expectedRevision`, returning `goal` and the revision of the matched mutation.
- `thread/goal/conditionalClear` takes `threadId`, required `expectedRevision`
  and optional provenance `origin`, returning `cleared` and the new revision.
- `thread/goal/conditionalActivate` takes `threadId`, `expectedRevision`,
  optional objective/budget/provenance and required `additionalContext`. It
  activates the matched goal and admits the current invocation context to its
  first useful turn, or to the exact existing regular turn. The response retains
  the committed goal/revision and a separate activation disposition. It requires
  a loaded goal runtime; an unloaded selection is rejected before mutation.

Conditional activation holds the existing goal-state permit through publication
and context admission. Busy admission uses the core's internal context queue;
it does not manufacture human input to satisfy `turn/steer`. A successful metadata
write alone does not prove admission: retain `notStarted`/`uncertain` separately
and never replay an unknown effect. Additional context is limited to eight
nonempty keys of at most 128 characters and 32 KiB serialized content.

Use these method names for selected operations; never downgrade to a legacy
mutation when the native server rejects an unsupported conditional method. An
ignored extra field on the old goal-set method is not a precondition. Existing
legacy methods remain available for their existing callers.

A revision is scoped to database identity and native thread. It changes on
insert, clear, replacement and actual objective/status/budget changes, including
changes made by legacy APIs or model tools. Returning to an earlier definition
still invalidates the old revision. Ordinary accounting and timestamp updates
alone do not invalidate it; reaching a limit changes status and does. Tombstones
retain absence transitions across clear/recreate and restart. Rebuilding the
entire goals database creates a new revision identity.

The SQLite writer transaction validates the revision before recording user
intent and applying set/clear. A stale operation records no user instruction.
Failed instruction persistence leaves goal metadata unchanged. History uses its
independent state/rollout storage, not writes to the locked goals database. A
later database/persistence error can still retain a genuine attempted instruction,
like the legacy contract; an uncertain client outcome must not be replayed.

Set preserves existing goal identity, creation time and usage; exhausted budgets
remain enforced. Clear removes metadata, not work, history or completed effects.
The existing runtime continuation/notification path is reused after a successful
mutation. Revision matching is not authorization, proof of inference, delivery or
hearing. The integrator separately retains original human intent, confirmed
assistant proposals, target binding and operation identity.

Development checks cover migration of existing rows, accounting stability,
definition ABA, replaced/absent goals, restart, concurrent conditional writers,
failed instruction recording and budget preservation. API checks cover loaded
and unloaded native contexts and rejected stale intent. Publication of this
source does not claim those API checks or receiving adoption passed.

## Idle resume and unchanged configuration

Interaction repeatedly resumes its native thread with explicit routing settings.
An idle unsubscribed thread now resolves those overrides through the normal full
configuration loader before deciding to replace its runtime. That loading stays
outside the shared thread-list permit and retains the existing stored-state
recheck. A loaded runtime is reusable only when the complete resolved configuration
matches both its startup and current configuration, and its startup-owned runtime
settings remain unchanged. Opaque host initialization data prevents this shortcut.

This is an in-memory optimization, not a new input, permission or persistence
format. Changed settings, unknown equivalence or changed runtime state retain
shutdown and a fresh history read. Already subscribed/running threads retain
existing rejoin behavior. No model/provider/effort is selected by the shortcut.
Do not skip actual selected-profile verification in the receiving client.

Qualification must exercise the processor/Core/rollout path, including identical
reuse, changed-config rebuild and an arriving subscriber/turn during loading.
Digital timing does not establish first audible sound or human hearing. A source
change is not adopted by an already-running native daemon; use the maintained
writer-preserving native package route and record receiving adoption separately.

The native qualification workflow also accepts `workflow_call` with an explicit
`source_ref`, so the owning Interaction CI can reuse the same build/tests when
native-repository Actions cannot be dispatched. Checkout remains fixed to
`compaii/codex`; callers must pin both the workflow and source to reviewed commit
SHAs. No deployment or secrets are inherited. Artifacts retain the checked-out
source ID separately from the caller's workflow revision.

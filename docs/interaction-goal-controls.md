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

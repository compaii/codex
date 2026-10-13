# Experimental extraction-only candidates

`memories.extraction_only = true` is an opt-in fork capability. It defaults to
false and requires normal memories/startup eligibility. This mode runs phase one
without native workspace seeding, retention or phase-two consolidation. Set
`memories.use_memories = false` for a producer-only profile. Source contribution
(`generate_memories`) is independent of processing already eligible sources.

The initial target is V2 with an isolated fictional home. The official 0.162.1
V1/V2 quality gate and the fork's compiled integration are separate evidence;
this fork retains its own version/source identity. Do not enable this on existing
private profiles as part of a test.

## Durable contract

Migration 0003 adds `memory_extraction_stream` and `memory_extraction_events` to
the versioned memory database. V2 uses `memories_v2_1.sqlite`. An owned job and
its immutable candidate envelope commit atomically; no stage1 output mutation,
consolidation enqueue or canonical admission is implied. The stream UUID and
monotonic sequence support finite replay; event UUIDs identify accepted commits.

The `codex.extraction-candidate/v1` JSON envelope preserves original UTF-8 rollout
text, model input and instructions/schema, exact generated output, parsed output,
version/model/effort and available token usage. Source filtering and output
truncation remain inspectable. Unsupported compressed sources, parse errors or
source changes while loading fail rather than silently committing mismatched
source evidence. Once input is prepared, it is a frozen revision: later source loss does
not discard the model result or its preserved input. Full originals remain
private to the explicitly authorized local profile; they are not ordinary recalled
memory.

Native retention/reset does not delete these candidate tables. The archive has
a 256 MiB envelope-byte cap; reaching it fails the job without completing its
watermark. Export/verified owner archiving is required to free capacity. There is
no automatic deletion after acknowledgement. The cap excludes SQLite overhead.

A consumer reads committed rows in sequence order and durably verifies original
bytes before setting `acknowledged_receipt`. It must bind the receipt to the exact
immutable row and its receiving identity. Acknowledgement means preservation,
not acceptance of model claims. Replay is required after uncertain delivery.
An empty extraction never means a previously selected lived event was retracted.

The first consumer is the explicitly invoked HMK `codex_candidates.py`, which
uses existing CaptureLedger and learning-artifact APIs. Selection, correction,
novelty checks and authority remain outside Codex. No hook, timer or production
learner is installed by this change.

## Rollback

Disable the opt-in configuration using the same schema-aware build to stop
candidate production; preserve the candidate database and any receiving artifacts.
Older binaries may reject a newer migration. Binary rollback therefore needs a
separate retained pre-migration profile, preserving later candidate data alongside
it rather than overwriting it. Qualification uses isolated profiles only.

# Foreground Interaction notices

This optional TUI integration reads the existing owner-local Interaction journal.
It adds no app-server method and changes no goal, native input, model step or
thread history. The owning read contract and receiving requirements are maintained
in [Telecodex passive control notices](https://github.com/AlterMundi/telecodex/blob/master/docs/interaction-notices.md).

A local operator may set `CODEX_FOREGROUND_NOTICES_SOCKET` to the receiving
owner socket's absolute path. Unset is disabled. This local binding is not a
portable default, browser login or a credential handoff. Reuse the configured
owner service; do not start another listener or infer a remote body's source.

Only the displayed local thread is queried using the existing foreground draw
scheduler. Remote workspaces are excluded. Thread/request fences discard late
responses, and closing the UI cancels outstanding work. The reader checks socket
ownership, permissions and peer UID, bounds a reply to 256 KiB and 750 ms, and
accepts only the expected schema, source, thread and increasing cursor. Text is
plain display data; terminal controls and oversized/invalid data are refused.

The latest initial notice is labeled retained history. New outcomes appear once
as information lines without submitting agent operations. Failures report once
and back off; ordinary conversation remains usable. These lines do not claim
human reading, work incorporation or delivery on another client.

`cargo test --locked --profile dev-small -j 2 -p codex-tui --lib foreground_notices` covers scope/order/text, stale
and duplicate replies, remote exclusion, disabled defaults, actual Unix read-only
transport and information rendering without agent input. Source and fixture
checks do not substitute for the qualified receiving executable and foreground
client journey. No native daemon update is part of enabling this reader.

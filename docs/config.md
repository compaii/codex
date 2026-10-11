# Configuration

For basic configuration instructions, see [this documentation](https://developers.openai.com/codex/config-basic).

For advanced configuration instructions, see [this documentation](https://developers.openai.com/codex/config-advanced).

For a full configuration reference, see [this documentation](https://developers.openai.com/codex/config-reference).

## Memory startup processing

`memories.startup_processing` defaults to `true`. Set it to `false` when consuming
an externally managed memory projection to skip the startup extraction,
consolidation and retention pipeline before it touches memory files or job state.
Memory context and retrieval remain independently controlled by `use_memories`
and the memories feature flag.

```toml
[features]
memories = true

[memories]
startup_processing = false
generate_memories = false
use_memories = true
```

`generate_memories = false` alone excludes newly created threads from future
extraction; it does not stop startup processing of existing eligible memories.
Disabling startup processing alone leaves source eligibility unchanged, so those
threads may be processed later by an enabled pipeline. Explicit memory notes and
reset are separate operations: this setting does not make the memory surface
read-only, provide an external backend, or authenticate projected content.

This controls newly scheduled startup work; it does not cancel jobs already
running in another session. Stop all writers before publishing an external view
and restart those sessions with startup processing disabled.

## Lifecycle hooks

Admins can set top-level `allow_managed_hooks_only = true` in
`requirements.toml` to ignore user, project, and session hook configs while
still allowing managed hooks from requirements and managed config layers. This
setting is only supported in `requirements.toml`; putting it in `config.toml`
does not enable managed-hooks-only mode.

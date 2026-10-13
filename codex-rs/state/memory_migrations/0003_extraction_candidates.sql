-- Opt-in producer output is independent of mutable stage1_outputs and native reset.
-- Sequence is delivery order, event_id is immutable identity. Acknowledgement never
-- deletes evidence; explicit owner archiving is required when the byte cap is reached.
CREATE TABLE memory_extraction_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    thread_id TEXT NOT NULL,
    source_updated_at INTEGER NOT NULL,
    envelope_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    acknowledged_receipt TEXT
);
CREATE TABLE memory_extraction_stream (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    stream_id TEXT NOT NULL
);

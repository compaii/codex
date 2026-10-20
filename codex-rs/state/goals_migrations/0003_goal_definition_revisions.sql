-- A rebuilt goals database must not accidentally reuse old snapshot tokens.
CREATE TABLE thread_goal_revision_identity (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    identity TEXT NOT NULL
);
INSERT INTO thread_goal_revision_identity VALUES(1, lower(hex(randomblob(16))));

-- Keep tombstones after clear: absent -> present -> absent is a real change.
-- Accounting alone leaves the definition revision unchanged.
CREATE TABLE thread_goal_revisions (
    thread_id TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL CHECK(revision >= 0)
);
INSERT INTO thread_goal_revisions(thread_id, revision)
SELECT thread_id, 1 FROM thread_goals;

CREATE TRIGGER thread_goal_revision_insert AFTER INSERT ON thread_goals
BEGIN
    INSERT INTO thread_goal_revisions(thread_id, revision) VALUES(NEW.thread_id, 1)
    ON CONFLICT(thread_id) DO UPDATE SET revision = revision + 1;
END;
CREATE TRIGGER thread_goal_revision_delete AFTER DELETE ON thread_goals
BEGIN
    INSERT INTO thread_goal_revisions(thread_id, revision) VALUES(OLD.thread_id, 1)
    ON CONFLICT(thread_id) DO UPDATE SET revision = revision + 1;
END;
CREATE TRIGGER thread_goal_revision_update
AFTER UPDATE OF goal_id, objective, status, token_budget ON thread_goals
WHEN OLD.goal_id IS NOT NEW.goal_id OR OLD.objective IS NOT NEW.objective
    OR OLD.status IS NOT NEW.status OR OLD.token_budget IS NOT NEW.token_budget
BEGIN
    INSERT INTO thread_goal_revisions(thread_id, revision) VALUES(NEW.thread_id, 1)
    ON CONFLICT(thread_id) DO UPDATE SET revision = revision + 1;
END;

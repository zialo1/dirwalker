-- MariaDB 10.x: stop binarycheck workers before applying.
-- Existing pairs refer to two directories on the same host.
-- Re-running preserves hostidB values already assigned to cross-host pairs.
ALTER TABLE tmp_doubles_letchecked
    ADD COLUMN IF NOT EXISTS hostidB SMALLINT NULL DEFAULT NULL AFTER hostid;
UPDATE tmp_doubles_letchecked SET hostidB = hostid WHERE hostidB IS NULL;
ALTER TABLE tmp_doubles_letchecked
    MODIFY COLUMN hostidB SMALLINT NOT NULL DEFAULT 0;

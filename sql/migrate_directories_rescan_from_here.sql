-- MariaDB 10.x. Stop scanners before applying this migration.
-- Marks a directory as a requested rescan starting point; existing rows are false.
-- This migration only adds the flag; it does not trigger or clear rescans.
ALTER TABLE directories
    ADD COLUMN IF NOT EXISTS rescan_from_here BOOLEAN NOT NULL DEFAULT FALSE;

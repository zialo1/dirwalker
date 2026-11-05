-- MariaDB 10.x. Stop scanners before applying this migration.
-- Latest modification time (UTC) of an included regular file directly in this directory.
-- NULL means not scanned, no eligible files, or an incomplete scan.
-- Existing rows are populated on their next detailed scan, not from scanned_at.
ALTER TABLE directories
    ADD COLUMN IF NOT EXISTS files_lastwrite DATETIME(6) NULL DEFAULT NULL;

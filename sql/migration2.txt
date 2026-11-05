-- MariaDB 10.x. Stop scanners before applying this migration.
-- No inventory or job records are deleted. Existing rows start visible.
ALTER TABLE directories
    ADD COLUMN IF NOT EXISTS zero_content_only BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_directories_double_filter
    ON directories (hostid, zero_content_only);
-- Populate from existing completed scan metadata, with the usual DB environment:
-- python3 -c 'import dirwalker; print(dirwalker.database_connection.refresh_zero_content_flags())'
-- New scans invalidate flags before changing metadata and refresh them at the end.

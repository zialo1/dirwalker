-- MariaDB 10.x. Stop workers before applying.
-- UTC directory modification time; NULL baselines are conservatively marked changed.
ALTER TABLE directories ADD COLUMN IF NOT EXISTS time_inode_changed DATETIME(6) NULL;
ALTER TABLE IF EXISTS tmp_modified
    ADD COLUMN IF NOT EXISTS subfolders_excluded BOOLEAN NOT NULL DEFAULT FALSE;
-- tmp_modified is also recreated with this flag on each modification check.

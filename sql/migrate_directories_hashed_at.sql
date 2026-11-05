-- MariaDB 10.x. Stop scanners before applying.
ALTER TABLE directories ADD COLUMN IF NOT EXISTS hashed_at DATETIME(6) NULL;
-- Fresh historical pair results initialize this field when preparing comparisons.

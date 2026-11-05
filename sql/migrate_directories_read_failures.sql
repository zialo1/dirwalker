-- MariaDB 10.x. Stop server/localserver before applying.
-- Count consecutive permission failures during marked rescans.
ALTER TABLE directories
    ADD COLUMN IF NOT EXISTS read_failures BIGINT UNSIGNED NOT NULL DEFAULT 0;

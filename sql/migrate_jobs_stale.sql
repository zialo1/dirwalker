-- Select the dirwalker database before running this script.
-- MariaDB with enforced CHECK constraints, or MySQL 8.0.19 and later.
-- For MySQL 8.0.16-8.0.18, replace DROP CONSTRAINT below with DROP CHECK.
-- Expands chk_job_status without dropping/truncating jobs or changing any rows.
-- The named CHECK must already exist (as in the current project schema).
-- ALTER TABLE commits implicitly; do not rely on an outer transaction to undo it.
-- After this migration, stop ALL workers before running serverinit, which marks
-- active jobs stale and also performs its existing hosts/comparison-table resets.

ALTER TABLE `jobs`
    DROP CONSTRAINT `chk_job_status`,
    ADD CONSTRAINT `chk_job_status` CHECK (
        `status` IN (
            'insert', 'pending', 'active', 'paused',
            'stopped', 'closed', 'error', 'stale'
        )
    );

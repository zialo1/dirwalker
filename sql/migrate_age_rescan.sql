-- MariaDB 10.x. Stop all workers and back up the database before applying.
-- Prerequisite: jobs, directories and tmp_modified already exist.
-- Missing tables can be created with --init-jobs after earlier migrations.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_jobid BIGINT UNSIGNED NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs (hostid, source_jobid, task, status);

ALTER TABLE tmp_modified
    ADD COLUMN IF NOT EXISTS batch_jobid BIGINT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hostid SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unreadable BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS path TEXT NULL,
    ADD COLUMN IF NOT EXISTS subfolders_excluded BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE tmp_modified m
JOIN directories d ON d.dirid = m.dirid
SET m.hostid = d.hostid, m.path = d.path
WHERE m.batch_jobid = 0;

ALTER TABLE tmp_modified
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (batch_jobid, hostid, dirid);

ALTER TABLE jobs
    DROP CONSTRAINT IF EXISTS chk_job_task,
    DROP CONSTRAINT IF EXISTS chk_job_status,
    ADD CONSTRAINT chk_job_task CHECK (
        task IN ('scan', 'rescan', 'check-modifications', 'rescan-after-days', 'check', 'verify', 'binarycheck')
    ),
    ADD CONSTRAINT chk_job_status CHECK (
        status IN ('insert', 'pending', 'active', 'paused', 'stopped', 'closed', 'error', 'stale')
    );

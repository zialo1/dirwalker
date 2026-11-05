-- MariaDB 10.x. Stop server/localserver before applying.
-- Enable queued modification checks; existing jobs are preserved.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS chk_job_task;
-- Also normalize any task names from the first marked-rescan implementation.
UPDATE jobs SET task = 'rescan' WHERE task = 'directories-rescan-marked';
UPDATE jobsrunid SET task = 'rescan' WHERE task = 'directories-rescan-marked';
ALTER TABLE jobs ADD CONSTRAINT chk_job_task
    CHECK (task IN ('scan', 'rescan', 'check-modifications', 'rescan-after-days', 'check', 'verify', 'binarycheck'));

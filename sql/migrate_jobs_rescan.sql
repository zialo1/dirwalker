-- MariaDB 10.x. Stop server/localserver before applying.
-- Allow the rescan task and rename any jobs queued with the earlier task name.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS chk_job_task;
UPDATE jobs SET task = 'rescan' WHERE task = 'directories-rescan-marked';
UPDATE jobsrunid SET task = 'rescan' WHERE task = 'directories-rescan-marked';
ALTER TABLE jobs ADD CONSTRAINT chk_job_task
    CHECK (task IN ('scan', 'rescan', 'check-modifications', 'rescan-after-days', 'check', 'verify', 'binarycheck'));

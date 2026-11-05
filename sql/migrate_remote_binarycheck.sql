-- MariaDB 10.11: stop localserver/serverplus before applying.
-- This does not start workers or change existing comparison outcomes.
ALTER TABLE jobsrunid ADD COLUMN IF NOT EXISTS remotehost SMALLINT NULL;
ALTER TABLE tmp_doubles_letchecked
    ADD COLUMN IF NOT EXISTS hostidB SMALLINT NULL DEFAULT NULL AFTER hostid;
UPDATE tmp_doubles_letchecked SET hostidB = hostid WHERE hostidB IS NULL;
ALTER TABLE tmp_doubles_letchecked
    MODIFY COLUMN hostidB SMALLINT NOT NULL DEFAULT 0,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (jobid, hostid, hostidB, left_dirid, right_dirid);
ALTER TABLE tmp_doubles_letchecked DROP CONSTRAINT IF EXISTS chk_double_jobs_pair;
ALTER TABLE tmp_doubles_letchecked ADD CONSTRAINT chk_double_jobs_pair
    CHECK (hostid <> hostidB OR left_dirid < right_dirid);
CREATE TABLE IF NOT EXISTS tmp_remotedoubles (
                runid BIGINT UNSIGNED NOT NULL,
                jobid BIGINT UNSIGNED NOT NULL,
                listener_runid BIGINT UNSIGNED NULL,
                hostid SMALLINT NOT NULL,
                dirid BIGINT UNSIGNED NOT NULL,
                algorithm VARCHAR(32) NOT NULL DEFAULT 'sha256-tree-v1',
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                checksum CHAR(64) CHARACTER SET ascii NULL,
                error_message TEXT NULL,
                claim_token CHAR(32) CHARACTER SET ascii NULL,
                timeupdated DATETIME(6) NOT NULL,
                PRIMARY KEY (runid, hostid, dirid),
                KEY idx_remote_pending (hostid, status, timeupdated),
                CONSTRAINT fk_remote_job FOREIGN KEY (jobid)
                    REFERENCES jobs(jobid) ON DELETE CASCADE,
                CONSTRAINT fk_remote_run FOREIGN KEY (runid)
                    REFERENCES jobsrunid(runid) ON DELETE CASCADE,
                CONSTRAINT chk_remote_status CHECK (status IN ('pending','active','done','error'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE tmp_remotedoubles ADD COLUMN IF NOT EXISTS listener_runid BIGINT UNSIGNED NULL AFTER jobid;

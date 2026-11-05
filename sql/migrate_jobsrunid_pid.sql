-- Run once in the dirwalker database before starting the updated server.
-- Existing run history is retained; older entries have NULL process metadata.
-- This changes the schema only and does not drop tables or delete job data.
-- Processes launched by older releases cannot be identified retrospectively.
ALTER TABLE `jobsrunid`
    ADD COLUMN `pid` BIGINT UNSIGNED NULL,
    ADD COLUMN `process_host` VARCHAR(255) NULL,
    ADD COLUMN `process_start` VARCHAR(128) NULL;

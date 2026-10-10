-- Experiment finalization pipeline.
-- Experiments transition WAITING/RUNNING -> FINALIZING -> STOPPED/FORCE_STOPPED.
-- termination_id is the stable identity of the finalization process, shared by
-- the scheduled stop and the early (manual) stop. final_result_id references
-- the one confirmed experiment_result row the terminal status is bound to;
-- once set the result is frozen and never recalculated.
ALTER TABLE `experiment`
    ADD COLUMN `termination_id` VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN `final_result_id` VARCHAR(255) NOT NULL DEFAULT '';

-- One row per experiment finalization. The unique key on (environment_id,
-- experiment_id) makes stop triggers idempotent: repeated triggers resume the
-- same termination instead of producing a second final result.
CREATE TABLE IF NOT EXISTS `experiment_termination` (
    `id` VARCHAR(255) NOT NULL,                  -- stable termination identity (UUID)
    `environment_id` VARCHAR(255) NOT NULL,
    `experiment_id` VARCHAR(255) NOT NULL,
    `reason` TINYINT NOT NULL,                   -- 1: scheduled stop, 2: early (forced) stop
    `phase` TINYINT NOT NULL,                    -- 1: waiting watermark, 2: computing,
                                                 -- 3: result saved, 4: confirmed,
                                                 -- 5: cache published
    `snapshot` JSON NOT NULL,                    -- fixed experiment configuration
    `evaluation_watermark` BIGINT NOT NULL,      -- event timestamp in microseconds
    `goal_watermark` BIGINT NOT NULL,            -- event timestamp in microseconds
    `input_summary` JSON NOT NULL,               -- input counts of the final calculation ('{}' until computed)
    `final_result_id` VARCHAR(255) NOT NULL DEFAULT '',
    `owner_id` VARCHAR(255) NOT NULL DEFAULT '', -- current compute owner
    `owner_expires_at` BIGINT NOT NULL DEFAULT 0,
    `attempts` INT NOT NULL DEFAULT 0,
    `last_error` TEXT NULL,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    `finalized_at` BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE INDEX `uniq_termination_experiment` (`environment_id`, `experiment_id`),
    INDEX `idx_termination_phase` (`environment_id`, `phase`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- High-water mark (microseconds) of event timestamps that have been durably
-- persisted by the subscriber, per environment and event type
-- (1: evaluation events, 2: goal events). The finalizer waits until both
-- watermarks reach the fixed termination watermarks before calculating.
CREATE TABLE IF NOT EXISTS `event_watermark` (
    `environment_id` VARCHAR(255) NOT NULL,
    `event_type` TINYINT NOT NULL,
    `high_water_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    PRIMARY KEY (`environment_id`, `event_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

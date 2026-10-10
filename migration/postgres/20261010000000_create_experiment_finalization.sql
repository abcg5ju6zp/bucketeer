-- Experiment finalization pipeline.
-- Experiments transition WAITING/RUNNING -> FINALIZING -> STOPPED/FORCE_STOPPED.
-- termination_id is the stable identity of the finalization process, shared by
-- the scheduled stop and the early (manual) stop. final_result_id references
-- the one confirmed experiment_result row the terminal status is bound to;
-- once set the result is frozen and never recalculated.
ALTER TABLE experiment
    ADD COLUMN termination_id VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN final_result_id VARCHAR(255) NOT NULL DEFAULT '';

-- One row per experiment finalization. The unique key on (environment_id,
-- experiment_id) makes stop triggers idempotent: repeated triggers resume the
-- same termination instead of producing a second final result.
CREATE TABLE experiment_termination (
    id VARCHAR(255) NOT NULL,                  -- stable termination identity (UUID)
    environment_id VARCHAR(255) NOT NULL,
    experiment_id VARCHAR(255) NOT NULL,
    reason SMALLINT NOT NULL,                  -- 1: scheduled stop, 2: early (forced) stop
    phase SMALLINT NOT NULL,                   -- 1: waiting watermark, 2: computing,
                                               -- 3: result saved, 4: confirmed,
                                               -- 5: cache published
    snapshot JSONB NOT NULL,                   -- fixed experiment configuration
    evaluation_watermark BIGINT NOT NULL,      -- event timestamp in microseconds
    goal_watermark BIGINT NOT NULL,            -- event timestamp in microseconds
    input_summary JSONB NOT NULL,              -- input counts of the final calculation ('{}' until computed)
    final_result_id VARCHAR(255) NOT NULL DEFAULT '',
    owner_id VARCHAR(255) NOT NULL DEFAULT '', -- current compute owner
    owner_expires_at BIGINT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    finalized_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    CONSTRAINT uniq_termination_experiment UNIQUE (environment_id, experiment_id)
);
CREATE INDEX idx_termination_phase ON experiment_termination (environment_id, phase);

-- High-water mark (microseconds) of event timestamps that have been durably
-- persisted by the subscriber, per environment and event type
-- (1: evaluation events, 2: goal events). The finalizer waits until both
-- watermarks reach the fixed termination watermarks before calculating.
CREATE TABLE event_watermark (
    environment_id VARCHAR(255) NOT NULL,
    event_type SMALLINT NOT NULL,
    high_water_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (environment_id, event_type)
);

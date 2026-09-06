-- Persist failed imported reward-basis reconciliation so later reward skips
-- can identify an import failure rather than misreporting a bootstrap gap.
CREATE TABLE IF NOT EXISTS `reward_seed_failure` (
    `epoch` integer NOT NULL,
    `snapshot_type` text NOT NULL,
    `failure_reason` text NOT NULL,
    `captured_slot` integer NOT NULL,
    PRIMARY KEY (`epoch`, `snapshot_type`)
);

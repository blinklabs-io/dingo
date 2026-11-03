CREATE INDEX IF NOT EXISTS `idx_pool_registration_snapshot_current`
    ON `pool_registration`(`pool_id`, `certificate_id`, `registration_index`, `added_slot`, `id`);

CREATE INDEX IF NOT EXISTS `idx_pool_retirement_pool_slot`
    ON `pool_retirement`(`pool_id`, `added_slot`);

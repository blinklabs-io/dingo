CREATE INDEX IF NOT EXISTS `idx_pool_opcert_sequence_slot` ON `pool_opcert_sequence`(`slot`);
CREATE INDEX IF NOT EXISTS `idx_pool_opcert_sequence_pool_sequence` ON `pool_opcert_sequence`(`pool_key_id`,`sequence`);

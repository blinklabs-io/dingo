-- Persist the per-pool block counts a bootstrap snapshot carries for the epochs
-- preceding its anchor, which no local block history can supply, so pool reward
-- performance for those epochs is computed rather than read as zero.
CREATE TABLE IF NOT EXISTS `imported_pool_block_count` (
    `epoch` integer NOT NULL,
    `pool_key_hash` blob NOT NULL,
    `blocks_produced` integer NOT NULL,
    `captured_slot` integer NOT NULL,
    PRIMARY KEY (`epoch`, `pool_key_hash`)
);

-- Records that an epoch's block counts came from a bootstrap snapshot, and the
-- epoch total the per-pool rows must sum to. A BlocksMade map with no entries
-- is a certified zero-block epoch, not an absent one, and only a row here can
-- tell those apart.
CREATE TABLE IF NOT EXISTS `imported_epoch_block_total` (
    `epoch` integer NOT NULL,
    `total_blocks` integer NOT NULL,
    `captured_slot` integer NOT NULL,
    PRIMARY KEY (`epoch`)
);

-- Version 2 (v2alpha1) adds the covering index behind GetChainDepState's
-- operational-certificate counters.
-- This file is immutable after release; changes are detected by its checksum.
CREATE INDEX IF NOT EXISTS `idx_pool_opcert_sequence_pool_sequence` ON `pool_opcert_sequence`(`pool_key_hash`,`sequence`);

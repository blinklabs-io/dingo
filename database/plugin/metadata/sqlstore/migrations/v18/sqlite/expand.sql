-- The exact stake this snapshot's degraded-pool exclusion removed from
-- reward_pool_input, so reward calculation can check the input rows sum to
-- exactly total_active_stake minus this value instead of only checking they
-- do not exceed it. A NULL value means the row predates this tracking (dingo
-- #4025): the exclusion, if any, is unknown, and only the non-exceeding bound
-- can still be checked for it.
ALTER TABLE `reward_snapshot` ADD COLUMN `excluded_active_stake` text;

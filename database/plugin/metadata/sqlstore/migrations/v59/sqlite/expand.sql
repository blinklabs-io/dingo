-- Find the pools whose retirement targets an epoch from the epoch index. The
-- retiring and retired pool lists rank certificates only for those pools, so
-- a page no longer reads every retirement certificate ever recorded.
CREATE INDEX IF NOT EXISTS `idx_pool_retirement_epoch` ON `pool_retirement`(`epoch`);

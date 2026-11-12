-- Serve the epoch stakes count and page. `registered` was not in any index,
-- so the count read every stake row of the epoch. With it after epoch and
-- before the page order, the count reads only matching entries and, outside
-- MySQL, a page needs no sort. MySQL keys the blob columns by prefix, which
-- it cannot use for ORDER BY.
CREATE INDEX IF NOT EXISTS `idx_reward_stake_input_epoch_registered_pool_cred` ON `reward_stake_input`(`epoch`,`registered`,`pool_key_hash`,`credential_tag`,`staking_key`);

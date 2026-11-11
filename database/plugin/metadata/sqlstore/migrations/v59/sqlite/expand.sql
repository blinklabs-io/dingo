-- Drop four indexes that no reader needs or that another index already serves.
--
-- idx_reward_live_stake_updated_slot: updated_slot is written on every
-- refresh and never filtered, joined or ordered on, so the index only added a
-- leaf rewrite per touched credential.
--
-- idx_account_reward_delta_tx_hash and idx_account_reward_delta_withdrawal:
-- every lookup by tx_hash also names withdrawal, so the unique
-- (withdrawal, tx_hash, ...) index serves it, and a boolean-only index
-- narrows nothing.
--
-- idx_account_drep: a prefix of idx_account_drep_active_staking_key.
DROP INDEX IF EXISTS `idx_reward_live_stake_updated_slot`;
DROP INDEX IF EXISTS `idx_account_reward_delta_tx_hash`;
DROP INDEX IF EXISTS `idx_account_reward_delta_withdrawal`;
DROP INDEX IF EXISTS `idx_account_drep`;

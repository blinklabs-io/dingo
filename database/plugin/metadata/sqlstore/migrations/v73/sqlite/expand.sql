CREATE UNIQUE INDEX IF NOT EXISTS `idx_account_reward_delta_credential_slot_kind_source`
    ON `account_reward_delta` (
        `credential_tag`, `staking_key`, `added_slot`, `withdrawal`, `tx_hash`
    );
DROP INDEX IF EXISTS `idx_account_reward_delta_w_tx_s_slot`;
DROP INDEX IF EXISTS `idx_account_reward_delta_credential`;

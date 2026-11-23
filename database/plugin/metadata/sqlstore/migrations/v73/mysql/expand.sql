CREATE UNIQUE INDEX `idx_account_reward_delta_credential_slot_kind_source`
    ON `account_reward_delta` (
        `credential_tag`, `staking_key`(255), `added_slot`, `withdrawal`, `tx_hash`(255)
    );
DROP INDEX `idx_account_reward_delta_w_tx_s_slot` ON `account_reward_delta`;
DROP INDEX `idx_account_reward_delta_credential` ON `account_reward_delta`;

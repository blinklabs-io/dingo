CREATE INDEX `idx_reward_account_output_credential_epoch`
    ON `reward_account_output`
       (`credential_tag`, `staking_key`(255), `epoch`, `pool_key_hash`(255),
        `reward_type`);
CREATE UNIQUE INDEX `idx_reward_account_output_epoch_pool_cred_type`
    ON `reward_account_output`
       (`epoch`, `pool_key_hash`(255), `credential_tag`, `staking_key`(255),
        `reward_type`);
DROP INDEX `idx_reward_account_output_credential_spendable_guarded`
    ON `reward_account_output`;
DROP INDEX `idx_reward_account_output_credential_spendable`
    ON `reward_account_output`;
DROP INDEX `idx_reward_account_output_epoch_cred_pool_type`
    ON `reward_account_output`;
DROP INDEX `idx_reward_account_output_boundary_slot`
    ON `reward_account_output`;

CREATE INDEX IF NOT EXISTS `idx_reward_stake_input_cred_epoch`
    ON `reward_stake_input` (`credential_tag`, `staking_key`, `epoch`);

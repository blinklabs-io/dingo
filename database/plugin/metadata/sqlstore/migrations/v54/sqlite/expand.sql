-- Let the pending reward credit read run on the index alone. The live stake
-- read sums every credential's unfolded credits in credited rounds; on the
-- (credential_tag, staking_key, spendable, guarded, ...) index that is one
-- table-row fetch per matching output to read folded and amount, which on
-- mainnet is millions of random page reads at every epoch boundary.
CREATE INDEX IF NOT EXISTS `idx_reward_account_output_credential_pending`
    ON `reward_account_output`
       (`credential_tag`, `staking_key`, `spendable`, `guarded`, `folded`,
        `epoch`, `amount`);

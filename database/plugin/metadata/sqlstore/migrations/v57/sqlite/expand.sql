-- Replace the credential-leading and epoch-leading reward_account_output
-- indexes with one credential index and a pool-ordered unique index.
-- The credential index serves every per-credential read; spendable, guarded
-- and folded stay out of the keys so a fold or a flag change moves no
-- credential-index entry. The unique index leads with (epoch, pool) so the
-- chunked precompute, which writes one pool at a time in pool order, appends
-- to it. boundary_slot has no reader.
CREATE INDEX IF NOT EXISTS `idx_reward_account_output_credential_epoch`
    ON `reward_account_output`
       (`credential_tag`, `staking_key`, `epoch`, `pool_key_hash`,
        `reward_type`);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_reward_account_output_epoch_pool_cred_type`
    ON `reward_account_output`
       (`epoch`, `pool_key_hash`, `credential_tag`, `staking_key`,
        `reward_type`);
DROP INDEX IF EXISTS `idx_reward_account_output_credential_pending`;
DROP INDEX IF EXISTS `idx_reward_account_output_credential_spendable_guarded`;
DROP INDEX IF EXISTS `idx_reward_account_output_credential_spendable`;
DROP INDEX IF EXISTS `idx_reward_account_output_epoch_cred_pool_type`;
DROP INDEX IF EXISTS `idx_reward_account_output_boundary_slot`;

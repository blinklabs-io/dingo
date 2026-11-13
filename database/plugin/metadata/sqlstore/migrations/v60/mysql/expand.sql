DROP INDEX `idx_utxo_deleted_staking_amount` ON `utxo`;
CREATE INDEX `idx_utxo_deleted_staking_amount` ON `utxo`(`deleted_slot`);

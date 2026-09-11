-- Record the certificate position a pointer address (types 4 and 5) names.
-- Such an address carries no stake credential of its own, so the utxo row
-- cannot say which account the output delegates to, and its lovelace never
-- reached the stake distribution (dingo #3854). Which credential the pointer
-- designates is a function of the certificate history at the slot being
-- evaluated -- a registration may not exist yet, may be de-registered later,
-- and stops conferring stake entirely in Conway -- so only the position is
-- stored here and the credential is resolved when stake is computed.
-- Rows are removed with their utxo, which is how rollback reaches them.
CREATE TABLE IF NOT EXISTS `utxo_pointer` (`utxo_id` integer NOT NULL,`ptr_slot` integer NOT NULL,`ptr_tx_index` integer NOT NULL,`ptr_cert_index` integer NOT NULL,PRIMARY KEY (`utxo_id`),CONSTRAINT `fk_utxo_pointer_utxo` FOREIGN KEY (`utxo_id`) REFERENCES `utxo`(`id`) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS `idx_utxo_pointer_target` ON `utxo_pointer`(`ptr_slot`,`ptr_tx_index`,`ptr_cert_index`);

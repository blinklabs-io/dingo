-- Index the scripts an API client asks about by hash. Both tables exist only
-- in API storage mode and are filled as transactions are applied, so rows
-- written before this version are absent until the transaction is replayed.
--
-- redeemer_script names the script each redeemer of a transaction executes.
-- The redeemer pointer (tag, index) does not carry the script: it resolves
-- through the transaction body (an input's address, a mint policy, a
-- certificate or withdrawal credential, a voter, a proposal guardrail). Slot
-- and tx_index repeat the transaction's chain position so a script's
-- redeemers page in order straight off the index. Rows are removed with their
-- transaction, which is how rollback reaches them.
CREATE TABLE IF NOT EXISTS `redeemer_script` (`transaction_id` integer NOT NULL,`tag` integer NOT NULL,`index` integer NOT NULL,`script_hash` blob NOT NULL,`slot` integer NOT NULL,`tx_index` integer NOT NULL,PRIMARY KEY (`transaction_id`,`tag`,`index`),CONSTRAINT `fk_redeemer_script_transaction` FOREIGN KEY (`transaction_id`) REFERENCES `transaction`(`id`) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS `idx_redeemer_script_position` ON `redeemer_script`(`script_hash`,`slot`,`tx_index`,`tag`,`index`);

-- utxo_reference_script records the script an output carries as a reference
-- script. It is a property of the output, so an output applied twice
-- converges rather than conflicting. Rows are removed with their utxo, which
-- is how rollback reaches them.
CREATE TABLE IF NOT EXISTS `utxo_reference_script` (`utxo_id` integer NOT NULL,`script_hash` blob NOT NULL,PRIMARY KEY (`utxo_id`),CONSTRAINT `fk_utxo_reference_script_utxo` FOREIGN KEY (`utxo_id`) REFERENCES `utxo`(`id`) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS `idx_utxo_reference_script_hash` ON `utxo_reference_script`(`script_hash`);

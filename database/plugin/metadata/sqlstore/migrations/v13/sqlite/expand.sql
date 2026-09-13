-- Preserve every transaction that uses an output as collateral. The legacy
-- utxo.collateral_by_tx_id column can hold only one owner and is retained for
-- compatibility while this association table is authoritative.
CREATE TABLE IF NOT EXISTS `utxo_collateral_input` (
    `utxo_id` integer NOT NULL,
    `transaction_hash` blob NOT NULL,
    PRIMARY KEY (`utxo_id`, `transaction_hash`),
    CONSTRAINT `fk_utxo_collateral_input` FOREIGN KEY (`utxo_id`)
        REFERENCES `utxo`(`id`) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS `idx_utxo_collateral_input_tx`
    ON `utxo_collateral_input`(`transaction_hash`);

-- v1alpha1 databases may already contain the single legacy marker. Copy it
-- before new writes use the many-to-many table; the primary key makes reruns
-- safe and ignores NULL markers.
INSERT INTO `utxo_collateral_input` (`utxo_id`, `transaction_hash`)
SELECT `id`, `collateral_by_tx_id` FROM `utxo`
LEFT JOIN `utxo_collateral_input` AS c
  ON c.`utxo_id` = `utxo`.`id`
 AND c.`transaction_hash` = `utxo`.`collateral_by_tx_id`
WHERE `utxo`.`collateral_by_tx_id` IS NOT NULL
  AND c.`utxo_id` IS NULL;

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

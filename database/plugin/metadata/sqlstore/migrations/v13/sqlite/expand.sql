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
WHERE `collateral_by_tx_id` IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM `utxo_collateral_input` AS c
      WHERE c.`utxo_id` = `utxo`.`id`
        AND c.`transaction_hash` = `utxo`.`collateral_by_tx_id`
  );

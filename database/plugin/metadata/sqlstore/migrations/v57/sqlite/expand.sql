-- Collateral and reference-input edges keyed by the naming transaction's row
-- id instead of its hash. The backfill copies the hash-keyed rows and clears
-- the legacy single-owner utxo columns, which no reader uses; the contract
-- phase drops the hash-keyed tables.
CREATE TABLE IF NOT EXISTS `utxo_collateral_edge` (
    `utxo_id` integer NOT NULL,
    `transaction_id` integer NOT NULL,
    PRIMARY KEY (`utxo_id`, `transaction_id`),
    CONSTRAINT `fk_utxo_collateral_edge` FOREIGN KEY (`utxo_id`)
        REFERENCES `utxo`(`id`) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS `idx_utxo_collateral_edge_tx`
    ON `utxo_collateral_edge`(`transaction_id`);
CREATE TABLE IF NOT EXISTS `utxo_reference_edge` (
    `utxo_id` integer NOT NULL,
    `transaction_id` integer NOT NULL,
    PRIMARY KEY (`utxo_id`, `transaction_id`),
    CONSTRAINT `fk_utxo_reference_edge` FOREIGN KEY (`utxo_id`)
        REFERENCES `utxo`(`id`) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS `idx_utxo_reference_edge_tx`
    ON `utxo_reference_edge`(`transaction_id`);

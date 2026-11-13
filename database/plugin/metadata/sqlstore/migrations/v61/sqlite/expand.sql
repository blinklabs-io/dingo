-- transaction becomes the identity table: one row per transaction hash the
-- store knows, whether the transaction was applied or is named only by an
-- imported or recovered output. Everything that exists only for an applied
-- transaction moves to applied_transaction, keyed by the identity row's id.
-- utxo is rebuilt around integer references to transaction.id and the
-- reference and collateral edges are keyed by that id. The backfill copies
-- the rows, swaps the new tables in for the old ones and builds the
-- secondary indexes; this phase only creates the tables the copy writes to.
CREATE TABLE IF NOT EXISTS `applied_transaction` (
    `tx_id` integer PRIMARY KEY,
    `block_hash` blob,
    `metadata` blob,
    `slot` integer,
    `type` integer,
    `fee` text,
    `collateral_fee` text,
    `ttl` text,
    `block_index` integer,
    `valid` numeric,
    CONSTRAINT `fk_applied_transaction_transaction` FOREIGN KEY (`tx_id`)
        REFERENCES `transaction`(`id`) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS `utxo_identity` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `tx_id` integer NOT NULL,
    `output_idx` integer NOT NULL,
    `collateral_return_for_tx_id` integer,
    `spent_at_tx_id` integer,
    `payment_key` blob,
    `staking_key` blob,
    `credential_tag` integer NOT NULL DEFAULT 0,
    `datum_hash` blob,
    `amount` text,
    `payment_script` numeric,
    `added_slot` integer,
    `deleted_slot` integer,
    CONSTRAINT `fk_utxo_tx` FOREIGN KEY (`tx_id`)
        REFERENCES `transaction`(`id`),
    CONSTRAINT `fk_utxo_collateral_return_tx` FOREIGN KEY
        (`collateral_return_for_tx_id`) REFERENCES `transaction`(`id`),
    CONSTRAINT `fk_utxo_spent_at_tx` FOREIGN KEY (`spent_at_tx_id`)
        REFERENCES `transaction`(`id`)
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_utxo_tx_id_output_idx`
    ON `utxo_identity`(`tx_id`, `output_idx`);
CREATE TABLE IF NOT EXISTS `utxo_reference_edge` (
    `utxo_id` integer NOT NULL,
    `tx_id` integer NOT NULL,
    PRIMARY KEY (`utxo_id`, `tx_id`),
    CONSTRAINT `fk_utxo_reference_edge_utxo` FOREIGN KEY (`utxo_id`)
        REFERENCES `utxo`(`id`) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS `utxo_collateral_edge` (
    `utxo_id` integer NOT NULL,
    `tx_id` integer NOT NULL,
    PRIMARY KEY (`utxo_id`, `tx_id`),
    CONSTRAINT `fk_utxo_collateral_edge_utxo` FOREIGN KEY (`utxo_id`)
        REFERENCES `utxo`(`id`) ON DELETE CASCADE
);

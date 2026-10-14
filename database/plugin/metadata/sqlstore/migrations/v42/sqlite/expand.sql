CREATE TABLE IF NOT EXISTS `utxo_address` (
    `utxo_id` INTEGER PRIMARY KEY,
    `address` BLOB NOT NULL,
    CONSTRAINT `fk_utxo_address_utxo`
        FOREIGN KEY (`utxo_id`) REFERENCES `utxo` (`id`) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_utxo_address_address`
    ON `utxo_address` (`address`);

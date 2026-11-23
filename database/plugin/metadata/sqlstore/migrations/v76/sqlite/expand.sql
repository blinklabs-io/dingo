-- Hold each address once. utxo_address repeated the encoded address on every
-- output and again in each index over it; it now names the address by id, and
-- its page order index is built on that id. The copy is built next to the
-- original and swapped in under its name by the backfill.
CREATE TABLE IF NOT EXISTS `address` (`id` integer PRIMARY KEY AUTOINCREMENT,`address` blob NOT NULL,UNIQUE (`address`));
CREATE TABLE IF NOT EXISTS `utxo_address_dictionary` (`utxo_id` integer PRIMARY KEY,`address_id` integer NOT NULL,`tx_id` integer,`output_idx` integer,`slot` integer,`block_index` integer,CONSTRAINT `fk_utxo_address_dictionary_utxo` FOREIGN KEY (`utxo_id`) REFERENCES `utxo`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_utxo_address_dictionary_address` FOREIGN KEY (`address_id`) REFERENCES `address`(`id`));

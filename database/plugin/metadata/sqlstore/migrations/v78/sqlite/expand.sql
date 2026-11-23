-- Hold each stake and payment credential and each datum hash once. utxo and
-- address_transaction repeated both credentials on every row and again in
-- each index over them, and utxo repeated the datum hash of every output that
-- names one. They now name the credential by id in `credential` and the datum
-- by id in `datum`; the credential kind (key or script) stays on the row, so
-- one hash is one dictionary row whichever kind uses it. The copies are built
-- next to the originals and swapped in under their names by the backfill,
-- which also builds their indexes.
CREATE TABLE IF NOT EXISTS `credential` (`id` integer PRIMARY KEY AUTOINCREMENT,`hash` blob NOT NULL,UNIQUE (`hash`));
CREATE TABLE IF NOT EXISTS `utxo_dictionary` (`id` integer PRIMARY KEY AUTOINCREMENT,`tx_id` integer NOT NULL,`output_idx` integer NOT NULL,`collateral_return_for_tx_id` integer,`spent_at_tx_id` integer,`payment_key_id` integer,`staking_key_id` integer,`credential_tag` integer NOT NULL DEFAULT 0,`datum_id` integer,`amount` text,`payment_script` numeric,`added_slot` integer,`deleted_slot` integer,CONSTRAINT `fk_utxo_dictionary_tx` FOREIGN KEY (`tx_id`) REFERENCES `transaction`(`id`),CONSTRAINT `fk_utxo_dictionary_collateral_return_tx` FOREIGN KEY (`collateral_return_for_tx_id`) REFERENCES `transaction`(`id`),CONSTRAINT `fk_utxo_dictionary_spent_at_tx` FOREIGN KEY (`spent_at_tx_id`) REFERENCES `transaction`(`id`));
CREATE TABLE IF NOT EXISTS `address_transaction_dictionary` (`id` integer PRIMARY KEY AUTOINCREMENT,`payment_key_id` integer,`payment_script` numeric,`staking_key_id` integer,`credential_tag` integer NOT NULL DEFAULT 0,`transaction_id` integer,`slot` integer,`tx_index` integer);

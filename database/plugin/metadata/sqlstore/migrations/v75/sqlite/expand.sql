-- Rebuild the reference and collateral edge tables as clustered tables keyed
-- by (tx_id, utxo_id). The previous tables were rowid tables with a primary
-- key led by utxo_id and a separate tx_id index, so every edge paid three
-- b-tree inserts, one at a random position. Edges are written in transaction
-- order, so the new primary key appends; the utxo_id index that serves the
-- cascade from utxo is built after the copy.
CREATE TABLE IF NOT EXISTS `utxo_reference_edge_clustered` (`tx_id` integer NOT NULL,`utxo_id` integer NOT NULL,PRIMARY KEY (`tx_id`,`utxo_id`),CONSTRAINT `fk_utxo_reference_edge_clustered_utxo` FOREIGN KEY (`utxo_id`) REFERENCES `utxo`(`id`) ON DELETE CASCADE) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS `utxo_collateral_edge_clustered` (`tx_id` integer NOT NULL,`utxo_id` integer NOT NULL,PRIMARY KEY (`tx_id`,`utxo_id`),CONSTRAINT `fk_utxo_collateral_edge_clustered_utxo` FOREIGN KEY (`utxo_id`) REFERENCES `utxo`(`id`) ON DELETE CASCADE) WITHOUT ROWID;

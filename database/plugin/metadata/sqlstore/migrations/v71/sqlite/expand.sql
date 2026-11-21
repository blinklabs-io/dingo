-- utxo_address carries the position a UTxO takes in an address page, so a
-- page reads an index in order instead of sorting the address's whole live
-- set. slot and block_index are the producing transaction's application
-- position, or the output's added_slot and 0 when the producer was never
-- applied. The backfill fills existing rows, then the ordering index is built.
ALTER TABLE `utxo_address` ADD COLUMN `tx_id` integer;
ALTER TABLE `utxo_address` ADD COLUMN `output_idx` integer;
ALTER TABLE `utxo_address` ADD COLUMN `slot` integer;
ALTER TABLE `utxo_address` ADD COLUMN `block_index` integer;

-- An output is identified by its producer, producer_tx_id =
-- COALESCE(transaction_id, collateral_return_for_tx_id), and its output
-- index. The producer id grows with apply order, so inserts append to this
-- index instead of landing at a random leaf the way a hash-keyed index does.
-- An output with no producing transaction row (an import) keeps its tx_id
-- hash lookup in a partial index.
CREATE UNIQUE INDEX IF NOT EXISTS `idx_utxo_producer_output`
    ON `utxo` (`producer_tx_id`, `output_idx`)
    WHERE `producer_tx_id` IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS `idx_utxo_imported_ref`
    ON `utxo` (`tx_id`, `output_idx`)
    WHERE `transaction_id` IS NULL AND `collateral_return_for_tx_id` IS NULL;
DROP INDEX IF EXISTS `tx_id_output_idx`;

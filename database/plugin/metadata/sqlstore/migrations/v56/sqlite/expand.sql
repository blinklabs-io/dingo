-- The spender of an output is stored as the spender's transaction row id.
-- The backfill moves every spent_at_tx_id hash onto it, leaves the legacy
-- column NULL, and builds idx_utxo_spent_by_tx_id once the column is
-- populated unless a deferred-index rebuild is pending. The legacy column and
-- its foreign key stay because SQLite cannot drop a constrained column
-- without rewriting the table.
ALTER TABLE `utxo` ADD COLUMN `spent_by_tx_id` integer;

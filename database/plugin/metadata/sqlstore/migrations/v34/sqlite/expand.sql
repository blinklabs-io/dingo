-- Persist the Midnight indexer's candidate-spend and epoch-transition rollback
-- journals so a rollback after a process restart restores the same state.
CREATE TABLE IF NOT EXISTS `midnight_candidate_removals` (`id` integer PRIMARY KEY AUTOINCREMENT,`block_number` integer NOT NULL,`tx_hash` blob NOT NULL,`output_index` integer NOT NULL,`datum` blob);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_midnight_candidate_removals_block_utxo` ON `midnight_candidate_removals`(`block_number`,`tx_hash`,`output_index`);
CREATE TABLE IF NOT EXISTS `midnight_epoch_transitions` (`block_number` integer PRIMARY KEY,`previous_epoch` integer NOT NULL);

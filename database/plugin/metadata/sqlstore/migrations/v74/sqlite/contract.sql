CREATE INDEX IF NOT EXISTS `idx_utxo_reference_edge_utxo_id` ON `utxo_reference_edge`(`utxo_id`);
CREATE INDEX IF NOT EXISTS `idx_utxo_collateral_edge_utxo_id` ON `utxo_collateral_edge`(`utxo_id`);

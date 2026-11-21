CREATE INDEX IF NOT EXISTS `idx_utxo_address_order`
    ON `utxo_address` (`address`, `slot`, `block_index`, `output_idx`, `tx_id`);

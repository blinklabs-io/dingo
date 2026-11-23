CREATE INDEX IF NOT EXISTS `idx_asset_mint_burn_asset_id` ON `asset_mint_burn`(`asset_id`,`slot`,`tx_index`);
CREATE INDEX IF NOT EXISTS `idx_asset_mint_burn_slot` ON `asset_mint_burn`(`slot`);

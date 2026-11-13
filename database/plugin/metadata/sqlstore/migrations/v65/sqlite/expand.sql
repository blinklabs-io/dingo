-- Split the per-output asset table into one identity row per distinct
-- (policy_id, name) and one holding row per (UTxO, asset). Holdings are keyed
-- by (utxo_id, asset_id), so a UTxO's holdings are contiguous and the primary
-- key also serves the cascade from utxo. idx_asset_holding_asset_id is the
-- only secondary index; the identity table's unique key serves lookups by
-- policy and by (policy, name).
CREATE TABLE IF NOT EXISTS `asset_identity` (`id` integer PRIMARY KEY AUTOINCREMENT,`policy_id` blob NOT NULL,`name` blob NOT NULL,UNIQUE (`policy_id`,`name`));
CREATE TABLE IF NOT EXISTS `asset_holding` (`utxo_id` integer NOT NULL,`asset_id` integer NOT NULL,`amount` text,PRIMARY KEY (`utxo_id`,`asset_id`),CONSTRAINT `fk_asset_holding_utxo` FOREIGN KEY (`utxo_id`) REFERENCES `utxo`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_asset_holding_asset` FOREIGN KEY (`asset_id`) REFERENCES `asset_identity`(`id`)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS `idx_asset_holding_asset_id` ON `asset_holding`(`asset_id`);

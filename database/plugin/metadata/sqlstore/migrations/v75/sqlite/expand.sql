-- Rebuild asset_mint_burn keyed by the ids of what it names. The previous
-- table repeated the transaction hash, policy, name and a fingerprint derived
-- from them on every row, and held them again in a unique index. The pair
-- (transaction, asset) is already held once in `transaction` and
-- `asset_identity`, so a row is now the clustered key and the event itself.
CREATE TABLE IF NOT EXISTS `asset_mint_burn_clustered` (`transaction_id` integer NOT NULL,`asset_id` integer NOT NULL,`slot` integer NOT NULL,`tx_index` integer NOT NULL,`quantity` text NOT NULL,PRIMARY KEY (`transaction_id`,`asset_id`),CONSTRAINT `fk_asset_mint_burn_clustered_transaction` FOREIGN KEY (`transaction_id`) REFERENCES `transaction`(`id`),CONSTRAINT `fk_asset_mint_burn_clustered_asset` FOREIGN KEY (`asset_id`) REFERENCES `asset_identity`(`id`)) WITHOUT ROWID;

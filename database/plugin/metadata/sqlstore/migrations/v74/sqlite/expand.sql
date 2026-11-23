-- applied_transaction names its block by an integer reference into a
-- dictionary that holds each block hash once, instead of repeating the hash
-- on every transaction row and again in an index entry. The backfill fills
-- block_id from block_hash; the contract phase drops block_hash.
CREATE TABLE IF NOT EXISTS `block_identity` (`id` integer PRIMARY KEY AUTOINCREMENT,`hash` blob NOT NULL,UNIQUE (`hash`));
ALTER TABLE `applied_transaction` ADD COLUMN `block_id` integer;

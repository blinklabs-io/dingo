-- block_index follows block_id so one block's transactions are read in block
-- order straight from the index.
CREATE INDEX IF NOT EXISTS `idx_applied_transaction_block_id` ON `applied_transaction`(`block_id`, `block_index`);
DROP INDEX IF EXISTS `idx_applied_transaction_block_hash`;
ALTER TABLE `applied_transaction` DROP COLUMN `block_hash`;

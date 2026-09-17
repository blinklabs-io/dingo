ALTER TABLE `governance_proposal` MODIFY COLUMN `anchor_url` TEXT NULL;
ALTER TABLE `governance_proposal` MODIFY COLUMN `anchor_hash` BLOB NULL;
ALTER TABLE `governance_proposal` MODIFY COLUMN `return_address` BLOB NULL;

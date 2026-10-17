ALTER TABLE `address_transaction` ADD COLUMN `payment_script` boolean;
CREATE TABLE IF NOT EXISTS `address_transaction_rebuild_source` (
    `transaction_id` integer PRIMARY KEY
);

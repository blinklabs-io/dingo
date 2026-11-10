-- Serve the metadata label page query in slot order. The label-only index
-- returns every transaction carrying the label in rowid order, so each page
-- sorted all of them. With slot next in the key, a page reads its first rows
-- in order and stops; transaction_id covers the join key.
CREATE INDEX IF NOT EXISTS `idx_transaction_metadata_label_label_slot` ON `transaction_metadata_label`(`label`,`slot`,`transaction_id`);

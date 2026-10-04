-- Conway RATIFY walks equal-priority governance actions in submission order:
-- block transaction position, then action position within the transaction.
-- governance_proposal only records the slot, so this companion table records
-- the transaction's position within its block (for a proposal imported from a
-- ledger-state snapshot, its position in the snapshot's proposal sequence). A
-- companion table rather than a column keeps this migration clear of v16's
-- rename-and-recreate `SELECT *` copy of governance_proposal (see v17).
CREATE TABLE IF NOT EXISTS `governance_proposal_order` (
    `proposal_id` integer PRIMARY KEY,
    `tx_index` integer NOT NULL,
    FOREIGN KEY (`proposal_id`) REFERENCES `governance_proposal`(`id`) ON DELETE CASCADE
);
-- Existing proposals take their position from the stored transaction. A
-- proposal whose transaction was never stored (a Mithril-imported one) keeps
-- no row and therefore the previous transaction-hash order.
INSERT INTO `governance_proposal_order` (`proposal_id`, `tx_index`)
SELECT `governance_proposal`.`id`, MIN(`transaction`.`block_index`)
FROM `governance_proposal`
JOIN `transaction` ON `transaction`.`hash` = `governance_proposal`.`tx_hash`
WHERE `transaction`.`block_index` IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM `governance_proposal_order`
    WHERE `governance_proposal_order`.`proposal_id` = `governance_proposal`.`id`
  )
GROUP BY `governance_proposal`.`id`;

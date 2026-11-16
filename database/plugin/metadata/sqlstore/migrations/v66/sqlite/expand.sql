-- A deleted vote is journaled as a tombstone entry, so a read at a slot
-- inside the deleted window stays correct after the voter casts again and
-- the live row's deleted_slot is cleared. The tombstone repeats the value
-- that was deleted.
ALTER TABLE `governance_vote_history` ADD COLUMN `deleted` boolean NOT NULL DEFAULT false;
-- Votes soft-deleted now get their tombstone. A vote deleted and then cast
-- again before this version has no deleted_slot left and cannot be
-- reconstructed.
INSERT INTO `governance_vote_history` (
    `vote_id`, `transition_slot`, `vote`, `anchor_url`, `anchor_hash`,
    `deleted`
)
SELECT `governance_vote`.`id`, `governance_vote`.`deleted_slot`,
    `governance_vote`.`vote`, `governance_vote`.`anchor_url`,
    `governance_vote`.`anchor_hash`, true
FROM `governance_vote`
WHERE `governance_vote`.`deleted_slot` IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM `governance_vote_history` AS `history`
    WHERE `history`.`vote_id` = `governance_vote`.`id`
      AND `history`.`deleted` = true
  );

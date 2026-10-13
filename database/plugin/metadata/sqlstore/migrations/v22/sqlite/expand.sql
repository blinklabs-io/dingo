-- Preserve every vote-value transition so chain rollback can restore the
-- exact vote that was current at the target slot, including across repeated
-- vote-replacement cycles by the same voter on the same proposal.
CREATE TABLE IF NOT EXISTS `governance_vote_history` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `vote_id` integer NOT NULL,
    `transition_slot` integer NOT NULL,
    `vote` integer NOT NULL,
    `anchor_url` text,
    `anchor_hash` blob,
    CONSTRAINT `fk_governance_vote_history_vote`
        FOREIGN KEY (`vote_id`) REFERENCES `governance_vote`(`id`)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS `idx_governance_vote_history_transition`
    ON `governance_vote_history`(`transition_slot`);
CREATE INDEX IF NOT EXISTS `idx_governance_vote_history_vote_transition`
    ON `governance_vote_history`(`vote_id`,`transition_slot`,`id`);
INSERT INTO `governance_vote_history` (
    `vote_id`, `transition_slot`, `vote`, `anchor_url`, `anchor_hash`
)
SELECT `governance_vote`.`id`,
    COALESCE(
        `governance_vote`.`vote_updated_slot`,
        `governance_vote`.`added_slot`
    ),
    `governance_vote`.`vote`, `governance_vote`.`anchor_url`,
    `governance_vote`.`anchor_hash`
FROM `governance_vote`
LEFT JOIN `governance_vote_history` AS `history`
    ON `history`.`vote_id` = `governance_vote`.`id`
    AND `history`.`transition_slot` = COALESCE(
        `governance_vote`.`vote_updated_slot`,
        `governance_vote`.`added_slot`
    )
    AND `history`.`vote` = `governance_vote`.`vote`
WHERE `history`.`id` IS NULL;

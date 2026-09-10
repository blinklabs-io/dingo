-- Preserve every ratification-state transition so chain rollback can restore
-- the exact marker that was current at the target slot, including across
-- repeated clear and re-ratify cycles.
CREATE TABLE IF NOT EXISTS `governance_proposal_ratification_history` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `proposal_id` integer NOT NULL,
    `transition_slot` integer NOT NULL,
    `ratified_epoch` integer,
    `ratified_slot` integer,
    CONSTRAINT `fk_governance_proposal_ratification_history_proposal`
        FOREIGN KEY (`proposal_id`) REFERENCES `governance_proposal`(`id`)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS `idx_governance_proposal_ratification_history_transition`
    ON `governance_proposal_ratification_history`(`transition_slot`);
CREATE INDEX IF NOT EXISTS `idx_governance_proposal_ratification_history_proposal_transition`
    ON `governance_proposal_ratification_history`(`proposal_id`,`transition_slot`,`id`);
INSERT INTO `governance_proposal_ratification_history` (
    `proposal_id`, `transition_slot`, `ratified_epoch`, `ratified_slot`
)
SELECT `proposal`.`id`, `proposal`.`ratified_slot`,
    `proposal`.`ratified_epoch`, `proposal`.`ratified_slot`
FROM `governance_proposal` AS `proposal`
LEFT JOIN `governance_proposal_ratification_history` AS `history`
    ON `history`.`proposal_id` = `proposal`.`id`
    AND `history`.`transition_slot` = `proposal`.`ratified_slot`
    AND `history`.`ratified_epoch` = `proposal`.`ratified_epoch`
    AND `history`.`ratified_slot` = `proposal`.`ratified_slot`
WHERE `proposal`.`ratified_epoch` IS NOT NULL
    AND `proposal`.`ratified_slot` IS NOT NULL
    AND `history`.`id` IS NULL;

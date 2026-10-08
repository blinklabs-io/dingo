-- Every write of a DRep's expiry, keyed by the slot it took effect, so a
-- query at an older point and a rollback can recover the expiry in force
-- there. drep.expiry_epoch keeps only the latest value.
CREATE TABLE IF NOT EXISTS `drep_expiry_history` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `credential_tag` integer NOT NULL,
    `credential` blob NOT NULL,
    `added_slot` integer NOT NULL,
    `last_activity_epoch` integer NOT NULL,
    `expiry_epoch` integer NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_drep_expiry_history_credential_slot`
    ON `drep_expiry_history`(`credential_tag`,`credential`,`added_slot`);
CREATE INDEX IF NOT EXISTS `idx_drep_expiry_history_added_slot`
    ON `drep_expiry_history`(`added_slot`);
-- An existing database seeds one row per DRep with its current expiry, dated
-- at the latest event that could have set it: the row's own slot, or the
-- DRep's newest vote, registration or update certificate after it.
INSERT INTO `drep_expiry_history` (
    `credential_tag`, `credential`, `added_slot`, `last_activity_epoch`,
    `expiry_epoch`
)
SELECT `drep`.`credential_tag`, `drep`.`credential`,
    COALESCE(`drep`.`added_slot`, 0),
    COALESCE(`drep`.`last_activity_epoch`, 0),
    COALESCE(`drep`.`expiry_epoch`, 0)
FROM `drep`
WHERE `drep`.`credential` IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM `drep_expiry_history` AS `history`
      WHERE `history`.`credential_tag` = `drep`.`credential_tag`
        AND `history`.`credential` = `drep`.`credential`
  );
UPDATE `drep_expiry_history`
SET `added_slot` = (
    SELECT MAX(COALESCE(`vote`.`vote_updated_slot`, `vote`.`added_slot`))
    FROM `governance_vote` AS `vote`
    WHERE `vote`.`voter_type` = 1
      AND `vote`.`voter_credential_tag` = `drep_expiry_history`.`credential_tag`
      AND `vote`.`voter_credential` = `drep_expiry_history`.`credential`
)
WHERE (
    SELECT MAX(COALESCE(`vote`.`vote_updated_slot`, `vote`.`added_slot`))
    FROM `governance_vote` AS `vote`
    WHERE `vote`.`voter_type` = 1
      AND `vote`.`voter_credential_tag` = `drep_expiry_history`.`credential_tag`
      AND `vote`.`voter_credential` = `drep_expiry_history`.`credential`
) > `added_slot`;
UPDATE `drep_expiry_history`
SET `added_slot` = (
    SELECT MAX(`registration`.`added_slot`)
    FROM `registration_drep` AS `registration`
    WHERE `registration`.`credential_tag` = `drep_expiry_history`.`credential_tag`
      AND `registration`.`drep_credential` = `drep_expiry_history`.`credential`
)
WHERE (
    SELECT MAX(`registration`.`added_slot`)
    FROM `registration_drep` AS `registration`
    WHERE `registration`.`credential_tag` = `drep_expiry_history`.`credential_tag`
      AND `registration`.`drep_credential` = `drep_expiry_history`.`credential`
) > `added_slot`;
UPDATE `drep_expiry_history`
SET `added_slot` = (
    SELECT MAX(`update_drep`.`added_slot`)
    FROM `update_drep`
    WHERE `update_drep`.`credential_tag` = `drep_expiry_history`.`credential_tag`
      AND `update_drep`.`credential` = `drep_expiry_history`.`credential`
)
WHERE (
    SELECT MAX(`update_drep`.`added_slot`)
    FROM `update_drep`
    WHERE `update_drep`.`credential_tag` = `drep_expiry_history`.`credential_tag`
      AND `update_drep`.`credential` = `drep_expiry_history`.`credential`
) > `added_slot`;

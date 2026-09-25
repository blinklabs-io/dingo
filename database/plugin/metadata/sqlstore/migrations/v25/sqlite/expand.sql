CREATE TABLE IF NOT EXISTS `drep_delegator` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `drep_credential_tag` INTEGER NOT NULL,
    `drep_credential` BLOB NOT NULL,
    `stake_credential_tag` INTEGER NOT NULL,
    `stake_credential` BLOB NOT NULL,
    `added_slot` INTEGER NOT NULL,
    `removed_slot` INTEGER
);

CREATE INDEX IF NOT EXISTS `idx_drep_delegator_active`
    ON `drep_delegator` (`drep_credential_tag`, `drep_credential`, `removed_slot`);
CREATE INDEX IF NOT EXISTS `idx_drep_delegator_rollback`
    ON `drep_delegator` (`added_slot`, `removed_slot`);

INSERT INTO `drep_delegator` (
    `drep_credential_tag`, `drep_credential`, `stake_credential_tag`,
    `stake_credential`, `added_slot`
)
SELECT account.drep_type, account.drep, account.credential_tag,
       account.staking_key, 0
FROM account
JOIN drep ON drep.credential_tag = account.drep_type
         AND drep.credential = account.drep
WHERE account.drep IS NOT NULL
  AND account.drep_type IN (0, 1)
  AND NOT EXISTS (
      SELECT 1 FROM drep_delegator
      WHERE drep_credential_tag = account.drep_type
        AND drep_credential = account.drep
        AND stake_credential_tag = account.credential_tag
        AND stake_credential = account.staking_key
        AND removed_slot IS NULL
  );

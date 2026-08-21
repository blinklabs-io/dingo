-- The account state a Mithril snapshot import or a Shelley genesis stake
-- delegation established, retained as the baseline `RestoreAccountStateAtSlot`
-- restores to. These accounts have no certificate history in this database --
-- the import deliberately writes none, because the snapshot cannot prove which
-- slot the registration happened in -- so a rollback had nothing to re-derive
-- `active`, `pool`, or `drep` from and kept whatever a rolled-away
-- deregistration or delegation had last written (issue #3260). `added_slot` is
-- the slot the baseline was established at; it orders the baseline against
-- certificate positions, so a deregistration at or after it still supersedes
-- it. `staking_key` is NOT NULL because the baseline is only ever read back by
-- equality on the credential, which no NULL key can match.
CREATE TABLE IF NOT EXISTS `account_import_baseline` (`credential_tag` integer NOT NULL DEFAULT 0,`staking_key` blob NOT NULL,`pool` blob,`drep` blob,`drep_type` integer DEFAULT 0,`active` numeric NOT NULL DEFAULT true,`added_slot` integer NOT NULL DEFAULT 0,PRIMARY KEY (`credential_tag`,`staking_key`));
-- Backfill for a database bootstrapped before this table existed, so an
-- already-running Mithril node does not stay exposed to the bug until it
-- re-bootstraps. `created_slot = 0` selects exactly the imported and genesis
-- accounts; every other row was created by a certificate whose own history
-- rebuilds its state. `active` is known to be true for those rows -- both
-- writers only ever import a registered account.
--
-- The NOT EXISTS clauses restrict the backfill to accounts that have no
-- account certificate at all, which is the only case where the live row still
-- holds the state the import established. Once any certificate has been
-- applied, `pool`, `drep`, and `added_slot` on the live row describe that
-- certificate rather than the import: `updateCertificateAccount` overwrites
-- the delegation columns and sets `added_slot` to the certificate slot while
-- leaving `created_slot` at 0. Recording those as the baseline would claim a
-- provenance the row does not have -- a rollback to before the certificate
-- would restore its pool or DRep, and a baseline slot bumped past an earlier
-- deregistration would let the virtual registration outrank it and mark a
-- deregistered credential active. Such an account is left with no baseline,
-- which keeps the derivation from its real certificate history and leaves
-- `pool` and `drep` exactly where the pre-fix rollback left them. The
-- certificate tables are the same ones the restore path itself reads, and each
-- is probed through its `idx_*_credential` index.
--
-- A legacy row with a NULL `staking_key` is skipped rather than backfilled: it
-- could not satisfy this table's NOT NULL key, no credential equality matches
-- it in any query, and inserting it would break re-runnability because NULL
-- never matches the LEFT JOIN below.
--
-- That LEFT JOIN, rather than a NOT EXISTS subquery, is what keeps the
-- statement re-runnable after an interrupted upgrade on all three backends:
-- MySQL rejects a subquery over an INSERT's own target table but permits that
-- table in the SELECT's FROM clause.
INSERT INTO `account_import_baseline` (`credential_tag`,`staking_key`,`pool`,`drep`,`drep_type`,`active`,`added_slot`)
SELECT `account`.`credential_tag`,`account`.`staking_key`,`account`.`pool`,`account`.`drep`,`account`.`drep_type`,true,COALESCE(`account`.`added_slot`,0)
FROM `account`
LEFT JOIN `account_import_baseline` ON `account_import_baseline`.`credential_tag` = `account`.`credential_tag` AND `account_import_baseline`.`staking_key` = `account`.`staking_key`
WHERE `account`.`created_slot` = 0
AND `account`.`staking_key` IS NOT NULL
AND `account_import_baseline`.`staking_key` IS NULL
AND NOT EXISTS (SELECT 1 FROM `stake_registration` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `stake_registration_delegation` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `stake_vote_registration_delegation` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `vote_registration_delegation` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `registration` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `stake_deregistration` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `deregistration` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `stake_delegation` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `stake_vote_delegation` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`)
AND NOT EXISTS (SELECT 1 FROM `vote_delegation` cert WHERE cert.`credential_tag` = `account`.`credential_tag` AND cert.`staking_key` = `account`.`staking_key`);

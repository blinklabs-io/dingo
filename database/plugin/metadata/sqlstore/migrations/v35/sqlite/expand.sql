CREATE TABLE IF NOT EXISTS `account_drep_clear` (
    `credential_tag` integer NOT NULL DEFAULT 0,
    `staking_key` blob NOT NULL,
    `added_slot` integer NOT NULL,
    PRIMARY KEY (`credential_tag`, `staking_key`, `added_slot`)
);
CREATE INDEX IF NOT EXISTS `idx_account_drep_clear_added_slot`
    ON `account_drep_clear`(`added_slot`);

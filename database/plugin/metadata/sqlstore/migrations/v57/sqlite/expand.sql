-- Stake-reward credits: one row per reward_account_output row applied to an
-- account at a boundary slot, keyed so that a round's credits are inserted in
-- account order. output_id is opaque (negative for rows migrated from
-- account_reward_delta).
CREATE TABLE IF NOT EXISTS `reward_credit` (
    `boundary_slot` integer NOT NULL,
    `account_id` integer NOT NULL,
    `output_id` integer NOT NULL,
    `amount` text NOT NULL,
    PRIMARY KEY (`boundary_slot`, `account_id`, `output_id`)
) WITHOUT ROWID;

-- The boundary slots that hold reward_credit rows, so a reader enumerates
-- rounds without scanning the credits.
CREATE TABLE IF NOT EXISTS `reward_credit_slot` (
    `boundary_slot` integer PRIMARY KEY NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS `reward_source` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `hash` blob NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_reward_source_hash`
    ON `reward_source`(`hash`);

CREATE TABLE IF NOT EXISTS `reward_event_credit` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `source_id` integer NOT NULL,
    `account_id` integer NOT NULL,
    `slot` integer NOT NULL,
    `amount` text NOT NULL,
    `post_snapshot` numeric NOT NULL DEFAULT false,
    `after_withdrawal` numeric NOT NULL DEFAULT false
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_reward_event_credit_source`
    ON `reward_event_credit`(`source_id`, `account_id`, `slot`);
CREATE INDEX IF NOT EXISTS `idx_reward_event_credit_slot`
    ON `reward_event_credit`(`slot`, `account_id`);
CREATE INDEX IF NOT EXISTS `idx_reward_event_credit_account`
    ON `reward_event_credit`(`account_id`, `slot`);

CREATE TABLE IF NOT EXISTS `reward_withdrawal` (
    `tx_id` integer NOT NULL,
    `account_id` integer NOT NULL,
    `slot` integer NOT NULL,
    `amount` text NOT NULL,
    `previous_reward` text NOT NULL,
    PRIMARY KEY (`tx_id`, `account_id`)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS `idx_reward_withdrawal_account_slot`
    ON `reward_withdrawal`(`account_id`, `slot`);
CREATE INDEX IF NOT EXISTS `idx_reward_withdrawal_slot`
    ON `reward_withdrawal`(`slot`);

CREATE TABLE IF NOT EXISTS `reward_credit` (
    `boundary_slot` BIGINT NOT NULL,
    `account_id` BIGINT NOT NULL,
    `output_id` BIGINT NOT NULL,
    `amount` VARCHAR(20) NOT NULL,
    PRIMARY KEY (`boundary_slot`, `account_id`, `output_id`)
);

CREATE TABLE IF NOT EXISTS `reward_credit_slot` (
    `boundary_slot` BIGINT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS `reward_source` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `hash` VARBINARY(64) NOT NULL,
    UNIQUE KEY `idx_reward_source_hash` (`hash`)
);

CREATE TABLE IF NOT EXISTS `reward_event_credit` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `source_id` BIGINT NOT NULL,
    `account_id` BIGINT NOT NULL,
    `slot` BIGINT NOT NULL,
    `amount` VARCHAR(20) NOT NULL,
    `post_snapshot` BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE KEY `idx_reward_event_credit_source` (`source_id`, `account_id`, `slot`),
    KEY `idx_reward_event_credit_slot` (`slot`, `account_id`),
    KEY `idx_reward_event_credit_account` (`account_id`, `slot`)
);

CREATE TABLE IF NOT EXISTS `reward_withdrawal` (
    `transaction_id` BIGINT NOT NULL,
    `account_id` BIGINT NOT NULL,
    `slot` BIGINT NOT NULL,
    `amount` VARCHAR(20) NOT NULL,
    `previous_reward` VARCHAR(20) NOT NULL,
    PRIMARY KEY (`transaction_id`, `account_id`),
    KEY `idx_reward_withdrawal_account_slot` (`account_id`, `slot`),
    KEY `idx_reward_withdrawal_slot` (`slot`)
);

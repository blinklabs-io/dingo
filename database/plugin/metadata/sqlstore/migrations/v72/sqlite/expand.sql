CREATE TABLE IF NOT EXISTS `reward_likelihood_state` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `epoch` integer NOT NULL,
    `reward_pot` text NOT NULL,
    `captured_slot` integer NOT NULL,
    `boundary_slot` integer NOT NULL,
    `complete` boolean NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_reward_likelihood_state_epoch`
    ON `reward_likelihood_state` (`epoch`);
CREATE INDEX IF NOT EXISTS `idx_reward_likelihood_state_boundary`
    ON `reward_likelihood_state` (`boundary_slot`);

CREATE TABLE IF NOT EXISTS `reward_pool_likelihood` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `epoch` integer NOT NULL,
    `pool_key_hash` blob NOT NULL,
    `log_weights` blob NOT NULL,
    `performance_estimate` real NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_reward_pool_likelihood_epoch_pool`
    ON `reward_pool_likelihood` (`epoch`, `pool_key_hash`);

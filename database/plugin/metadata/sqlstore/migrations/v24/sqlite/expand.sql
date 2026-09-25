CREATE TABLE IF NOT EXISTS `drep_dormancy_state` (
    `id` INTEGER PRIMARY KEY CHECK (`id` = 1),
    `dormant_epochs` INTEGER NOT NULL
);

INSERT INTO `drep_dormancy_state` (`id`, `dormant_epochs`)
VALUES (1, 0)
ON CONFLICT (`id`) DO NOTHING;

CREATE TABLE IF NOT EXISTS `drep_dormancy_history` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `added_slot` INTEGER NOT NULL,
    `previous_dormant_epochs` INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS `idx_drep_dormancy_history_slot`
    ON `drep_dormancy_history` (`added_slot`);

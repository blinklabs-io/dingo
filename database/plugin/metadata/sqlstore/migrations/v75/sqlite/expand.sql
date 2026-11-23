CREATE TABLE IF NOT EXISTS `drep_distribution_snapshot` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `epoch` integer NOT NULL,
    `distribution` blob NOT NULL,
    `boundary_slot` integer NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_drep_distribution_snapshot_epoch`
    ON `drep_distribution_snapshot` (`epoch`);
CREATE INDEX IF NOT EXISTS `idx_drep_distribution_snapshot_boundary`
    ON `drep_distribution_snapshot` (`boundary_slot`);

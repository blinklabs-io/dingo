CREATE TABLE IF NOT EXISTS `governance_boundary_snapshot` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `epoch` integer NOT NULL,
    `boundary_slot` integer NOT NULL,
    `snapshot` blob NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_governance_boundary_snapshot_epoch`
    ON `governance_boundary_snapshot` (`epoch`);
CREATE INDEX IF NOT EXISTS `idx_governance_boundary_snapshot_slot`
    ON `governance_boundary_snapshot` (`boundary_slot`);

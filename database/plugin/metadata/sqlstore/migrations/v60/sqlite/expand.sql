-- One row per minted asset, holding the id of its first mint/burn event.
-- The asset listing pages and counts this table instead of grouping and
-- sorting the whole event history on every request.
CREATE TABLE IF NOT EXISTS `minted_asset` (
    `policy_id` blob NOT NULL,
    `name` blob NOT NULL,
    `first_event_id` integer NOT NULL,
    PRIMARY KEY (`policy_id`, `name`)
);
CREATE INDEX IF NOT EXISTS `idx_minted_asset_first_event` ON `minted_asset`(`first_event_id`);
CREATE INDEX IF NOT EXISTS `idx_minted_asset_policy_first_event` ON `minted_asset`(`policy_id`,`first_event_id`);

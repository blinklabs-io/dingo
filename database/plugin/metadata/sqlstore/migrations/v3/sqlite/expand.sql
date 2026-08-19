-- CIP-26 off-chain token registry properties, keyed by registry subject
-- (hex policy ID followed by the hex-encoded asset name). Backs the
-- `metadata` field of the Blockfrost GET /assets/{asset} response, which is
-- distinct from `onchain_metadata` (CIP-25/CIP-68 mint metadata read from the
-- chain). Rows are a best-effort cache of a periodically synced upstream
-- registry, not consensus data, and are only written in API storage mode with
-- the token registry sync enabled. `logo` is NULL unless the operator opted
-- into storing logos; logos are roughly 90% of registry bytes.
CREATE TABLE IF NOT EXISTS `token_registry_entry` (`created_at` datetime,`updated_at` datetime,`subject` text NOT NULL,`name` text,`ticker` text,`description` text,`url` text,`logo` text,`id` integer PRIMARY KEY AUTOINCREMENT,`decimals` integer);
CREATE UNIQUE INDEX IF NOT EXISTS `idx_token_registry_entry_subject` ON `token_registry_entry`(`subject`);

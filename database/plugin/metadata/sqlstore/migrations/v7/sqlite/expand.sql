-- Preserve the historical stake-key deposit carried by a Mithril account
-- baseline. Existing baselines stay NULL because their imports discarded the
-- value and substituting today's protocol parameter would invent history.
ALTER TABLE `account_import_baseline` ADD COLUMN `deposit_amount` text;

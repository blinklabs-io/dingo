-- Running total of live script-locked lovelace: one row per slot whose
-- commit changed it, holding the total after that slot. The backfill seeds it
-- from a full scan; the contract drops the index the scan used to need.
CREATE TABLE IF NOT EXISTS `script_locked_supply` (`slot` integer PRIMARY KEY,`amount` integer NOT NULL);

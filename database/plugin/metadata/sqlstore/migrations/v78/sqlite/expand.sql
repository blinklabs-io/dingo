-- Rebuild block_nonce as a clustered table keyed by (slot, hash). The previous
-- table was a rowid table with a unique index on the same two columns, so each
-- block paid a table insert and an index insert. The row's place among the
-- blocks of its slot, which the rowid gave, is the ordinal column. The copy is
-- built next to the original and swapped in under its name by the backfill.
CREATE TABLE IF NOT EXISTS `block_nonce_clustered` (`slot` integer NOT NULL,`hash` blob NOT NULL,`ordinal` integer NOT NULL,`nonce` blob,`is_checkpoint` numeric,PRIMARY KEY (`slot`,`hash`)) WITHOUT ROWID;

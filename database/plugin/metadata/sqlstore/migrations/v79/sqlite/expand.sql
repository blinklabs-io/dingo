-- Rebuild pool_opcert_sequence as a clustered table keyed by (pool credential
-- id, slot). The previous table was a rowid table that held the pool key hash
-- on every block and a unique index on the same two columns, so each block
-- paid a table insert and an index insert and repeated the hash in both. The
-- pool key is named by its id in `credential`, and the primary key is the row.
-- The copy is built next to the original and swapped in under its name by the
-- backfill; the secondary indexes are built after the swap.
CREATE TABLE IF NOT EXISTS `pool_opcert_sequence_clustered` (`pool_key_id` integer NOT NULL,`slot` integer NOT NULL,`sequence` integer,PRIMARY KEY (`pool_key_id`,`slot`)) WITHOUT ROWID;

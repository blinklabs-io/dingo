-- idx_datum_hash duplicates the index behind uni_datum_hash on the same
-- column. The unique index serves every lookup by hash and is the
-- ON CONFLICT (hash) target of the datum insert, so the second index only adds
-- a random-position write per insert.
DROP INDEX IF EXISTS `idx_datum_hash`;

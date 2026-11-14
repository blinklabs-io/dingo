-- idx_script_hash duplicates the index behind uni_script_hash on the same
-- column. The unique index serves every lookup by hash and is the
-- ON CONFLICT (hash) target of the script insert, so the second index only
-- adds a random-position write per insert.
DROP INDEX IF EXISTS `idx_script_hash`;

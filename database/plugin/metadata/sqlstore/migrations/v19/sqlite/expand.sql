-- asset.name_hex duplicated hex.EncodeToString(asset.name) at write time.
-- No query filters on it -- every asset lookup keys on policy_id/name -- so
-- it was write-only weight on every asset row and one indexed column with no
-- reader (dingo#4464). Every external consumer (api/blockfrost's
-- NodeAdapter.Asset, api/mesh's appendUtxoOps) already recomputes the hex
-- encoding from the asset name on the fly, so dropping the stored column and
-- its index changes no observable behavior.
--
-- The index must be dropped before the column: SQLite refuses to drop a
-- column an index still references.
DROP INDEX IF EXISTS `idx_asset_name_hex`;
ALTER TABLE `asset` DROP COLUMN `name_hex`;

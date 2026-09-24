-- idx_asset_amount and idx_asset_fingerprint index columns nothing ever
-- filters, joins, or orders on: every asset lookup keys on
-- policy_id/name/utxo_id, and asset.amount/asset.fingerprint are only
-- SELECTed and returned (api/blockfrost's NodeAdapter.Asset, api/mesh's
-- appendUtxoOps, GetAssetsByUtxoID, GetAssetByPolicyAndName,
-- GetAssetQuantityByPolicyAndName). A real WAL-frame-churn measurement during
-- genesis sync found idx_asset_amount responsible for 23.4% of all frame
-- writes to the metadata database -- the single largest contributor of any
-- index or table -- and idx_asset_fingerprint for 3.4% (dingo#4598, following
-- the dingo#4482 asset.name_hex investigation from dingo#4464). Unlike
-- name_hex, the amount and fingerprint columns themselves are genuinely read
-- and returned via the blockfrost/mesh API adapters, so only the indexes are
-- dropped here.
DROP INDEX IF EXISTS `idx_asset_amount`;
DROP INDEX IF EXISTS `idx_asset_fingerprint`;

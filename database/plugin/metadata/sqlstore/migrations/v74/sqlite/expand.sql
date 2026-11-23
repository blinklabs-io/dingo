-- idx_utxo_address_address is a strict prefix of idx_utxo_address_order, which
-- serves every lookup by address equally, so the narrower index only doubles
-- the address-keyed writes and the deferred-index rebuild at the anchor.
DROP INDEX IF EXISTS `idx_utxo_address_address`;

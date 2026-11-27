-- History expiry asks, once per block, whether a live output names a datum or
-- carries a reference script the block holds, and whether an unspent Midnight
-- registration names the block. Each lookup needs an index to stay a point
-- read on a large utxo table.
CREATE INDEX IF NOT EXISTS "idx_utxo_live_datum_hash"
    ON "utxo" ("datum_hash")
    WHERE "datum_hash" IS NOT NULL AND "deleted_slot" = 0;
CREATE INDEX IF NOT EXISTS "idx_script_created_slot"
    ON "script" ("created_slot");
CREATE INDEX IF NOT EXISTS "idx_midnight_registrations_block_hash"
    ON "midnight_registrations" ("block_hash");

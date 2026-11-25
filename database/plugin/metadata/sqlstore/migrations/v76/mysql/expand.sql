-- MySQL has no partial indexes and indexes blob columns by prefix.
CREATE INDEX `idx_utxo_live_datum_hash`
    ON `utxo` (`datum_hash`(32), `deleted_slot`);
CREATE INDEX `idx_script_created_slot`
    ON `script` (`created_slot`);
CREATE INDEX `idx_midnight_registrations_block_hash`
    ON `midnight_registrations` (`block_hash`(32));

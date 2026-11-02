ALTER TABLE backfill_checkpoint ADD COLUMN last_index_id INTEGER;
ALTER TABLE backfill_checkpoint ADD COLUMN last_block_hash BLOB;

UPDATE backfill_checkpoint
SET last_slot = 0,
    total_slots = 0,
    last_index_id = NULL,
    last_block_hash = NULL
WHERE phase = 'metadata' AND completed IS NOT TRUE;

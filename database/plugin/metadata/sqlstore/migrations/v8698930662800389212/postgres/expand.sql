-- The DRep list resolves each DRep's registration epoch as the newest epoch
-- starting at or before a slot. Without an index on start_slot that lookup
-- walks the epoch table once per DRep.
CREATE INDEX IF NOT EXISTS "idx_epoch_start_slot"
    ON "epoch" ("start_slot");

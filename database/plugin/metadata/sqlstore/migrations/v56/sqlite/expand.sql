-- Lead the block_nonce uniqueness index with slot. Block hashes are uniformly
-- random, so every insert into the (hash, slot) index landed on an unrelated
-- leaf: each 50-block apply chunk rewrote about 50 index pages. Slots only
-- grow, so inserts into (slot, hash) hit the right edge. Every reader of
-- block_nonce supplies a slot or a slot range, so none needs a hash-leading
-- key; the same index also serves the range, latest-row and prune reads that
-- previously scanned the table. (slot, hash) is unique exactly when
-- (hash, slot) is, so existing rows always satisfy it.
CREATE UNIQUE INDEX IF NOT EXISTS `idx_block_nonce_slot_hash` ON `block_nonce`(`slot`,`hash`);
DROP INDEX IF EXISTS `hash_slot`;

-- Dijkstra/Leios: registered BLS voting key and its proof of possession,
-- carried alongside a pool's existing vrf_key_hash. NULL means no
-- registered leios_key field. A key whose proof of possession is invalid
-- is still stored here as-is; that check happens only when the key is read
-- back out for committee construction (see ledger/leios), not at write time.
ALTER TABLE `pool` ADD COLUMN `leios_key_public` blob;
ALTER TABLE `pool` ADD COLUMN `leios_key_possession_proof` blob;
ALTER TABLE `pool_registration` ADD COLUMN `leios_key_public` blob;
ALTER TABLE `pool_registration` ADD COLUMN `leios_key_possession_proof` blob;

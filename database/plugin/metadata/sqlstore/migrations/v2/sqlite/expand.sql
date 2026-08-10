-- Dijkstra/Leios: registered BLS voting key and its proof of possession,
-- carried alongside a pool's existing vrf_key_hash. NULL means no
-- currently-valid registered key (absent leios_key field, or a leios_key
-- present on-chain whose proof of possession failed verification).
ALTER TABLE `pool` ADD COLUMN `leios_key_public` blob;
ALTER TABLE `pool` ADD COLUMN `leios_key_possession_proof` blob;
ALTER TABLE `pool_registration` ADD COLUMN `leios_key_public` blob;
ALTER TABLE `pool_registration` ADD COLUMN `leios_key_possession_proof` blob;

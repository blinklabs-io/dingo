-- Freeze each pool's optional Dijkstra/Leios BLS verification key and proof of
-- possession with the stake snapshot that selected its committee seat. Legacy
-- rows remain NULL/keyless; deriving a historical key from current pool state
-- would make the result depend on when the node first read the snapshot.
ALTER TABLE `pool_stake_snapshot` ADD COLUMN `leios_key_public` blob;
ALTER TABLE `pool_stake_snapshot` ADD COLUMN `leios_key_possession_proof` blob;

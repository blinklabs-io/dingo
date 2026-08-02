-- name: GetTip :one
SELECT id, hash, slot, block_number
FROM tip
WHERE id = 1;

-- name: SetTip :exec
INSERT INTO tip (id, hash, slot, block_number)
VALUES (1, ?, ?, ?)
ON CONFLICT (id) DO UPDATE SET
    hash = excluded.hash,
    slot = excluded.slot,
    block_number = excluded.block_number;

-- name: GetLatestNetworkState :one
SELECT id, treasury, reserves, slot
FROM network_state
ORDER BY slot DESC
LIMIT 1;

-- name: SetNetworkState :exec
INSERT INTO network_state (treasury, reserves, slot)
VALUES (?, ?, ?)
ON CONFLICT (slot) DO UPDATE SET
    treasury = excluded.treasury,
    reserves = excluded.reserves;

-- name: DeleteNetworkStateAfterSlot :exec
DELETE FROM network_state
WHERE slot > ?;

-- name: GetSyncState :one
SELECT value
FROM sync_state
WHERE sync_key = ?;

-- name: SetSyncState :exec
INSERT INTO sync_state (sync_key, value)
VALUES (?, ?)
ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value;

-- name: DeleteSyncState :exec
DELETE FROM sync_state
WHERE sync_key = ?;

-- name: ClearSyncState :exec
DELETE FROM sync_state;

-- name: GetEpoch :one
SELECT id, epoch_id, start_slot, nonce, evolving_nonce, candidate_nonce,
       last_epoch_block_nonce, era_id, slot_length, length_in_slots
FROM epoch
WHERE epoch_id = ?;

-- name: GetEpochsByEra :many
SELECT id, epoch_id, start_slot, nonce, evolving_nonce, candidate_nonce,
       last_epoch_block_nonce, era_id, slot_length, length_in_slots
FROM epoch
WHERE era_id = ?
ORDER BY epoch_id;

-- name: GetEpochs :many
SELECT id, epoch_id, start_slot, nonce, evolving_nonce, candidate_nonce,
       last_epoch_block_nonce, era_id, slot_length, length_in_slots
FROM epoch
ORDER BY epoch_id;

-- name: GetEpochBySlot :one
SELECT id, epoch_id, start_slot, nonce, evolving_nonce, candidate_nonce,
       last_epoch_block_nonce, era_id, slot_length, length_in_slots
FROM epoch
WHERE start_slot <= ? AND ? < start_slot + length_in_slots
ORDER BY start_slot DESC
LIMIT 1;

-- name: DeleteEpochsAfterSlot :exec
DELETE FROM epoch
WHERE start_slot > ?;

-- name: SetEpoch :exec
INSERT INTO epoch (
    epoch_id, start_slot, nonce, evolving_nonce, candidate_nonce,
    last_epoch_block_nonce, era_id, slot_length, length_in_slots
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch_id) DO UPDATE SET
    start_slot = excluded.start_slot,
    nonce = excluded.nonce,
    evolving_nonce = excluded.evolving_nonce,
    candidate_nonce = excluded.candidate_nonce,
    last_epoch_block_nonce = excluded.last_epoch_block_nonce,
    era_id = excluded.era_id,
    slot_length = excluded.slot_length,
    length_in_slots = excluded.length_in_slots;

-- name: SetBlockNonce :exec
INSERT INTO block_nonce (hash, slot, nonce, is_checkpoint)
VALUES (?, ?, ?, ?)
ON CONFLICT (hash, slot) DO UPDATE SET
    nonce = excluded.nonce,
    is_checkpoint = block_nonce.is_checkpoint OR excluded.is_checkpoint;

-- name: GetBlockNonce :one
SELECT nonce
FROM block_nonce
WHERE hash = ? AND slot = ?;

-- name: GetBlockNoncesInSlotRange :many
SELECT hash, nonce, id, slot, is_checkpoint
FROM block_nonce
WHERE slot >= ? AND slot < ?
ORDER BY slot ASC;

-- name: GetLastBlockNonceInRange :one
SELECT nonce
FROM block_nonce
WHERE slot >= ? AND slot < ?
ORDER BY slot DESC, hash DESC
LIMIT 1;

-- name: DeleteBlockNoncesBeforeSlot :exec
DELETE FROM block_nonce
WHERE slot < ?;

-- name: DeleteBlockNoncesBeforeSlotWithoutCheckpoints :exec
DELETE FROM block_nonce
WHERE slot < ? AND is_checkpoint = FALSE;

-- name: DeleteBlockNoncesAfterOrigin :exec
DELETE FROM block_nonce
WHERE slot >= ?;

-- name: DeleteBlockNoncesAfterPoint :exec
DELETE FROM block_nonce
WHERE slot > ? OR (slot = ? AND hash <> ?);

-- name: GetDatum :one
SELECT hash, raw_datum, id, added_slot
FROM datum
WHERE hash = ?;

-- name: SetDatum :exec
INSERT INTO datum (hash, raw_datum, added_slot)
VALUES (?, ?, ?)
ON CONFLICT (hash) DO UPDATE SET raw_datum = excluded.raw_datum;

-- name: GetScript :one
SELECT hash, content, id, created_slot, type
FROM script
WHERE hash = ?;

-- name: GetPParams :many
SELECT cbor, id, added_slot, epoch, era_id
FROM pparams
WHERE epoch <= ? AND era_id = ?
ORDER BY epoch DESC, id DESC
LIMIT 1;

-- name: GetPParamUpdates :many
SELECT genesis_hash, cbor, id, added_slot, epoch
FROM pparam_update
WHERE epoch = ? OR epoch = ?
ORDER BY id DESC;

-- name: SetPParams :exec
INSERT INTO pparams (cbor, added_slot, epoch, era_id)
VALUES (?, ?, ?, ?);

-- name: SetPParamUpdate :exec
INSERT INTO pparam_update (genesis_hash, cbor, added_slot, epoch)
VALUES (?, ?, ?, ?);

-- name: DeletePParamsAfterSlot :exec
DELETE FROM pparams
WHERE added_slot > ?;

-- name: DeletePParamUpdatesAfterSlot :exec
DELETE FROM pparam_update
WHERE added_slot > ?;

-- name: AddNetworkDonation :exec
INSERT INTO network_donation (slot, epoch, amount)
VALUES (?, ?, ?)
ON CONFLICT (slot) DO UPDATE SET
    epoch = excluded.epoch,
    amount = excluded.amount;

-- name: SumNetworkDonationsForEpoch :many
SELECT amount
FROM network_donation
WHERE epoch = ?;

-- name: DeleteNetworkDonationsAfterSlot :exec
DELETE FROM network_donation
WHERE slot > ?;

-- name: GetImportCheckpoint :one
SELECT id, import_key, phase
FROM import_checkpoint
WHERE import_key = ?;

-- name: SetImportCheckpoint :exec
INSERT INTO import_checkpoint (import_key, phase)
VALUES (?, ?)
ON CONFLICT (import_key) DO UPDATE SET phase = excluded.phase;

-- name: GetBackfillCheckpoint :one
SELECT id, phase, last_slot, total_slots, started_at, updated_at, completed
FROM backfill_checkpoint
WHERE phase = ?;

-- name: SetBackfillCheckpoint :one
INSERT INTO backfill_checkpoint (
    phase, last_slot, total_slots, started_at, updated_at, completed
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (phase) DO UPDATE SET
    last_slot = excluded.last_slot,
    total_slots = excluded.total_slots,
    updated_at = excluded.updated_at,
    completed = excluded.completed
RETURNING id;

-- name: GetConstitution :one
SELECT id, anchor_url, anchor_hash, policy_hash, added_slot, deleted_slot
FROM constitution
WHERE deleted_slot IS NULL
ORDER BY added_slot DESC
LIMIT 1;

-- name: SetConstitution :one
INSERT INTO constitution (
    anchor_url, anchor_hash, policy_hash, added_slot, deleted_slot
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (added_slot) DO UPDATE SET
    anchor_url = excluded.anchor_url,
    anchor_hash = excluded.anchor_hash,
    policy_hash = excluded.policy_hash,
    deleted_slot = excluded.deleted_slot
RETURNING id;

-- name: DeleteConstitutionsAddedAfterSlot :exec
DELETE FROM constitution
WHERE added_slot > ?;

-- name: RestoreConstitutionsDeletedAfterSlot :exec
UPDATE constitution
SET deleted_slot = NULL
WHERE deleted_slot > ?;

-- name: SetCommitteeMember :one
INSERT INTO committee_member (
    cold_cred_hash, expires_epoch, added_slot, deleted_slot
) VALUES (?, ?, ?, ?)
ON CONFLICT (cold_cred_hash) DO UPDATE SET
    expires_epoch = excluded.expires_epoch,
    added_slot = excluded.added_slot,
    deleted_slot = excluded.deleted_slot
RETURNING id;

-- name: SetCommitteeQuorum :exec
INSERT INTO committee_quorum (quorum, added_slot)
VALUES (?, ?)
ON CONFLICT (added_slot) DO UPDATE SET quorum = excluded.quorum;

-- name: GetCommitteeQuorum :one
SELECT quorum
FROM committee_quorum
ORDER BY added_slot DESC, id DESC
LIMIT 1;

-- name: GetCommitteeMembers :many
SELECT id, cold_cred_hash, expires_epoch, added_slot, deleted_slot
FROM committee_member
WHERE deleted_slot IS NULL
ORDER BY id;

-- name: GetCommitteeMembersIncludeDeleted :many
SELECT id, cold_cred_hash, expires_epoch, added_slot, deleted_slot
FROM committee_member
ORDER BY id;

-- name: SoftDeleteCommitteeMember :exec
UPDATE committee_member
SET deleted_slot = ?
WHERE cold_cred_hash = ? AND deleted_slot IS NULL;

-- name: SoftDeleteAllCommitteeMembers :exec
UPDATE committee_member
SET deleted_slot = ?
WHERE deleted_slot IS NULL;

-- name: DeleteCommitteeMembersAddedAfterSlot :exec
DELETE FROM committee_member
WHERE added_slot > ?;

-- name: DeleteCommitteeQuorumsAfterSlot :exec
DELETE FROM committee_quorum
WHERE added_slot > ?;

-- name: RestoreCommitteeMembersDeletedAfterSlot :exec
UPDATE committee_member
SET deleted_slot = NULL
WHERE deleted_slot > ?;

-- name: CreatePoolStakeSnapshot :one
INSERT INTO pool_stake_snapshot (
    epoch, snapshot_type, pool_key_hash, total_stake, stake_denominator,
    delegator_count, captured_slot, calculation_version,
    reward_account_auto_vote,
    reward_account_auto_vote_resolved
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;

-- name: SavePoolStakeSnapshot :one
INSERT INTO pool_stake_snapshot (
    epoch, snapshot_type, pool_key_hash, total_stake, stake_denominator,
    delegator_count, captured_slot, calculation_version,
    reward_account_auto_vote,
    reward_account_auto_vote_resolved
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch, snapshot_type, pool_key_hash) DO UPDATE SET
    total_stake = excluded.total_stake,
    stake_denominator = excluded.stake_denominator,
    delegator_count = excluded.delegator_count,
    captured_slot = excluded.captured_slot,
    calculation_version = excluded.calculation_version,
    reward_account_auto_vote = excluded.reward_account_auto_vote,
    reward_account_auto_vote_resolved =
        excluded.reward_account_auto_vote_resolved
RETURNING id;

-- name: GetPoolStakeSnapshot :one
SELECT id, epoch, snapshot_type, pool_key_hash, total_stake,
       stake_denominator, delegator_count, captured_slot,
       calculation_version, reward_account_auto_vote,
       reward_account_auto_vote_resolved
FROM pool_stake_snapshot
WHERE epoch = ? AND snapshot_type = ? AND pool_key_hash = ?;

-- name: GetPoolStakeSnapshotsByEpoch :many
SELECT id, epoch, snapshot_type, pool_key_hash, total_stake,
       stake_denominator, delegator_count, captured_slot,
       calculation_version, reward_account_auto_vote,
       reward_account_auto_vote_resolved
FROM pool_stake_snapshot
WHERE epoch = ? AND snapshot_type = ?
ORDER BY id;

-- name: SumPoolStakeSnapshots :many
SELECT total_stake
FROM pool_stake_snapshot
WHERE epoch = ? AND snapshot_type = ?;

-- name: SaveEpochSummary :one
INSERT INTO epoch_summary (
    epoch, total_active_stake, total_pool_count, total_delegators,
    epoch_nonce, boundary_slot, snapshot_ready
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch) DO UPDATE SET
    total_active_stake = excluded.total_active_stake,
    total_pool_count = excluded.total_pool_count,
    total_delegators = excluded.total_delegators,
    epoch_nonce = excluded.epoch_nonce,
    boundary_slot = excluded.boundary_slot,
    snapshot_ready = epoch_summary.snapshot_ready OR excluded.snapshot_ready
RETURNING id;

-- name: GetEpochSummary :one
SELECT id, epoch, total_active_stake, total_pool_count, total_delegators,
       epoch_nonce, boundary_slot, snapshot_ready
FROM epoch_summary
WHERE epoch = ?;

-- name: GetLatestEpochSummary :one
SELECT id, epoch, total_active_stake, total_pool_count, total_delegators,
       epoch_nonce, boundary_slot, snapshot_ready
FROM epoch_summary
ORDER BY epoch DESC
LIMIT 1;

-- name: DeletePoolStakeSnapshotsForEpoch :exec
DELETE FROM pool_stake_snapshot
WHERE epoch = ? AND snapshot_type = ?;

-- name: DeletePoolStakeSnapshotsAfterEpoch :exec
DELETE FROM pool_stake_snapshot
WHERE epoch > ?;

-- name: DeletePoolStakeSnapshotsBeforeEpoch :exec
DELETE FROM pool_stake_snapshot
WHERE epoch < ?;

-- name: DeleteEpochSummariesAfterEpoch :exec
DELETE FROM epoch_summary
WHERE epoch > ?;

-- name: SaveRewardAdaPots :one
INSERT INTO reward_ada_pots (
    epoch, treasury, reserves, fees, rewards, captured_slot
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch) DO UPDATE SET
    treasury = excluded.treasury,
    reserves = excluded.reserves,
    fees = excluded.fees,
    rewards = excluded.rewards,
    captured_slot = excluded.captured_slot
RETURNING id;

-- name: GetRewardAdaPots :one
SELECT id, epoch, treasury, reserves, fees, rewards, captured_slot
FROM reward_ada_pots
WHERE epoch = ?;

-- name: SaveRewardSnapshot :one
INSERT INTO reward_snapshot (
    epoch, snapshot_type, total_active_stake, total_pool_count,
    total_delegators, captured_slot, boundary_slot, epoch_nonce,
    protocol_version, authoritative, calculation_version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch, snapshot_type) DO UPDATE SET
    total_active_stake = excluded.total_active_stake,
    total_pool_count = excluded.total_pool_count,
    total_delegators = excluded.total_delegators,
    captured_slot = excluded.captured_slot,
    boundary_slot = excluded.boundary_slot,
    epoch_nonce = excluded.epoch_nonce,
    protocol_version = excluded.protocol_version,
    authoritative = excluded.authoritative,
    calculation_version = excluded.calculation_version
RETURNING id;

-- name: InsertRewardSnapshot :one
INSERT INTO reward_snapshot (
    epoch, snapshot_type, total_active_stake, total_pool_count,
    total_delegators, captured_slot, boundary_slot, epoch_nonce,
    protocol_version, authoritative, calculation_version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch, snapshot_type) DO NOTHING
RETURNING id;

-- name: UpdateFallbackRewardSnapshot :exec
UPDATE reward_snapshot
SET total_active_stake = ?,
    total_pool_count = ?,
    total_delegators = ?,
    captured_slot = ?,
    boundary_slot = ?,
    epoch_nonce = ?,
    protocol_version = ?,
    authoritative = FALSE,
    calculation_version = ?
WHERE epoch = ? AND snapshot_type = ?;

-- name: GetRewardSnapshot :one
SELECT id, epoch, snapshot_type, total_active_stake, total_pool_count,
       total_delegators, captured_slot, boundary_slot, epoch_nonce,
       protocol_version, authoritative, calculation_version
FROM reward_snapshot
WHERE epoch = ? AND snapshot_type = ?;

-- name: ReleaseFallbackRewardSnapshotGuard :execrows
DELETE FROM reward_snapshot
WHERE id = ? AND authoritative = FALSE;

-- name: SaveRewardPoolInput :one
INSERT INTO reward_pool_input (
    margin, pool_key_hash, reward_account, blocks_produced,
    total_blocks_in_epoch, epoch, pledge, delegated_stake, owner_stake,
    cost, delegator_count, reward_account_credential_tag, captured_slot,
    boundary_slot
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch, pool_key_hash) DO UPDATE SET
    blocks_produced = excluded.blocks_produced,
    total_blocks_in_epoch = excluded.total_blocks_in_epoch,
    pledge = excluded.pledge,
    delegated_stake = excluded.delegated_stake,
    owner_stake = excluded.owner_stake,
    cost = excluded.cost,
    margin = excluded.margin,
    reward_account = excluded.reward_account,
    reward_account_credential_tag = excluded.reward_account_credential_tag,
    delegator_count = excluded.delegator_count,
    captured_slot = excluded.captured_slot,
    boundary_slot = excluded.boundary_slot
RETURNING id;

-- name: GetRewardPoolInputs :many
SELECT margin, pool_key_hash, reward_account, blocks_produced,
       total_blocks_in_epoch, id, epoch, pledge, delegated_stake,
       owner_stake, cost, delegator_count, reward_account_credential_tag,
       captured_slot, boundary_slot
FROM reward_pool_input
WHERE epoch = ?
ORDER BY pool_key_hash ASC;

-- name: SaveRewardStakeInput :one
INSERT INTO reward_stake_input (
    pool_key_hash, staking_key, epoch, credential_tag, stake, owner,
    registered, captured_slot, boundary_slot
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch, pool_key_hash, credential_tag, staking_key) DO UPDATE SET
    stake = excluded.stake,
    owner = excluded.owner,
    registered = excluded.registered,
    captured_slot = excluded.captured_slot,
    boundary_slot = excluded.boundary_slot
RETURNING id;

-- name: GetRewardStakeInputs :many
SELECT pool_key_hash, staking_key, id, epoch, credential_tag, stake, owner,
       registered, captured_slot, boundary_slot
FROM reward_stake_input
WHERE epoch = ?
ORDER BY pool_key_hash ASC, credential_tag ASC, staking_key ASC;

-- name: DeleteRewardPoolInputsForEpoch :exec
DELETE FROM reward_pool_input WHERE epoch = ?;

-- name: DeleteRewardStakeInputsForEpoch :exec
DELETE FROM reward_stake_input WHERE epoch = ?;

-- name: SaveRewardPoolOutput :one
INSERT INTO reward_pool_output (
    apparent_performance, pool_key_hash, epoch, optimal_reward,
    total_reward, leader_reward, member_reward_total, owner_stake,
    undistributed, unspendable, captured_slot, boundary_slot
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (epoch, pool_key_hash) DO UPDATE SET
    apparent_performance = excluded.apparent_performance,
    optimal_reward = excluded.optimal_reward,
    total_reward = excluded.total_reward,
    leader_reward = excluded.leader_reward,
    member_reward_total = excluded.member_reward_total,
    owner_stake = excluded.owner_stake,
    undistributed = excluded.undistributed,
    unspendable = excluded.unspendable,
    captured_slot = excluded.captured_slot,
    boundary_slot = excluded.boundary_slot
RETURNING id;

-- name: GetRewardPoolOutputs :many
SELECT apparent_performance, pool_key_hash, id, epoch, optimal_reward,
       total_reward, leader_reward, member_reward_total, owner_stake,
       undistributed, unspendable, captured_slot, boundary_slot
FROM reward_pool_output
WHERE epoch = ?
ORDER BY pool_key_hash ASC;

-- name: SaveRewardAccountOutput :one
INSERT INTO reward_account_output (
    staking_key, pool_key_hash, reward_type, epoch, credential_tag,
    amount, spendable, guarded, captured_slot, boundary_slot
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (
    epoch, credential_tag, staking_key, pool_key_hash, reward_type
) DO UPDATE SET
    amount = excluded.amount,
    spendable = excluded.spendable,
    guarded = excluded.guarded,
    captured_slot = excluded.captured_slot,
    boundary_slot = excluded.boundary_slot
RETURNING id;

-- name: GetRewardAccountOutputs :many
SELECT staking_key, pool_key_hash, reward_type, id, epoch, credential_tag,
       amount, spendable, guarded, captured_slot, boundary_slot
FROM reward_account_output
WHERE epoch = ?
ORDER BY credential_tag ASC, staking_key ASC, pool_key_hash ASC,
         reward_type ASC;

-- name: DeleteRewardPoolOutputsForEpoch :exec
DELETE FROM reward_pool_output WHERE epoch = ?;

-- name: DeleteRewardAccountOutputsForEpoch :exec
DELETE FROM reward_account_output WHERE epoch = ?;

-- name: DeleteRewardAdaPotsAfterSlot :exec
DELETE FROM reward_ada_pots WHERE captured_slot > ?;

-- name: DeleteRewardSnapshotsAfterSlot :exec
DELETE FROM reward_snapshot
WHERE captured_slot > ? OR boundary_slot > ?;

-- name: DeleteRewardPoolInputsAfterSlot :exec
DELETE FROM reward_pool_input
WHERE captured_slot > ? OR boundary_slot > ?;

-- name: DeleteRewardStakeInputsAfterSlot :exec
DELETE FROM reward_stake_input
WHERE captured_slot > ? OR boundary_slot > ?;

-- name: DeleteRewardPoolOutputsAfterSlot :exec
DELETE FROM reward_pool_output
WHERE captured_slot > ? OR boundary_slot > ?;

-- name: DeleteRewardAccountOutputsAfterSlot :exec
DELETE FROM reward_account_output
WHERE captured_slot > ? OR boundary_slot > ?;

-- name: DeleteRewardStakeInputsBeforeEpoch :exec
DELETE FROM reward_stake_input WHERE epoch < ?;

-- name: DeleteRewardAccountOutputsBeforeEpoch :exec
DELETE FROM reward_account_output WHERE epoch < ?;

-- name: CreateMidnightAssetCreate :one
INSERT INTO midnight_asset_creates (
    address, quantity, tx_hash, output_index, block_number, block_hash,
    tx_index, block_timestamp_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: CreateMidnightAssetSpend :one
INSERT INTO midnight_asset_spends (
    address, quantity, spending_tx_hash, utxo_tx_hash, utxo_index,
    block_number, block_hash, tx_index, block_timestamp_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: CreateMidnightRegistration :one
INSERT INTO midnight_registrations (
    full_datum, tx_hash, output_index, block_number, block_hash, tx_index,
    block_timestamp_ms
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: CreateMidnightDeregistration :one
INSERT INTO midnight_deregistrations (
    full_datum, tx_hash, utxo_tx_hash, utxo_index, block_number, block_hash,
    tx_index, block_timestamp_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: FindUnspentMidnightAssetCreates :many
SELECT c.id, c.address, c.quantity, c.tx_hash, c.output_index,
       c.block_number, c.block_hash, c.tx_index, c.block_timestamp_ms
FROM midnight_asset_creates c
WHERE NOT EXISTS (
    SELECT 1 FROM midnight_asset_spends s
    WHERE s.utxo_tx_hash = c.tx_hash AND s.utxo_index = c.output_index
)
ORDER BY c.id;

-- name: FindUnspentMidnightRegistrations :many
SELECT r.id, r.full_datum, r.tx_hash, r.output_index, r.block_number,
       r.block_hash, r.tx_index, r.block_timestamp_ms
FROM midnight_registrations r
WHERE NOT EXISTS (
    SELECT 1 FROM midnight_deregistrations d
    WHERE d.utxo_tx_hash = r.tx_hash AND d.utxo_index = r.output_index
)
ORDER BY r.id;

-- name: GetMidnightAssetCreatesByBlock :many
SELECT id, address, quantity, tx_hash, output_index, block_number,
       block_hash, tx_index, block_timestamp_ms
FROM midnight_asset_creates
WHERE block_number = ?
ORDER BY id;

-- name: DeleteMidnightAssetCreatesByBlock :exec
DELETE FROM midnight_asset_creates WHERE block_number = ?;

-- name: GetMidnightAssetSpendsByBlock :many
SELECT id, address, quantity, spending_tx_hash, utxo_tx_hash, utxo_index,
       block_number, block_hash, tx_index, block_timestamp_ms
FROM midnight_asset_spends
WHERE block_number = ?
ORDER BY id;

-- name: DeleteMidnightAssetSpendsByBlock :exec
DELETE FROM midnight_asset_spends WHERE block_number = ?;

-- name: GetMidnightRegistrationsByBlock :many
SELECT id, full_datum, tx_hash, output_index, block_number, block_hash,
       tx_index, block_timestamp_ms
FROM midnight_registrations
WHERE block_number = ?
ORDER BY id;

-- name: DeleteMidnightRegistrationsByBlock :exec
DELETE FROM midnight_registrations WHERE block_number = ?;

-- name: GetMidnightDeregistrationsByBlock :many
SELECT id, full_datum, tx_hash, utxo_tx_hash, utxo_index, block_number,
       block_hash, tx_index, block_timestamp_ms
FROM midnight_deregistrations
WHERE block_number = ?
ORDER BY id;

-- name: DeleteMidnightDeregistrationsByBlock :exec
DELETE FROM midnight_deregistrations WHERE block_number = ?;

-- name: InsertMidnightGovernanceDatum :one
INSERT INTO midnight_governance_datums (
    datum_type, tx_hash, output_index, datum, block_number
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: DeleteMidnightGovernanceDatumsByBlock :exec
DELETE FROM midnight_governance_datums WHERE block_number = ?;

-- name: GetLatestMidnightGovernanceDatum :one
SELECT id, datum_type, tx_hash, output_index, datum, block_number
FROM midnight_governance_datums
WHERE datum_type = ? AND block_number <= ?
ORDER BY block_number DESC, id DESC
LIMIT 1;

-- name: GetLatestMidnightAriadneParams :one
SELECT id, epoch, datum
FROM midnight_ariadne_params
ORDER BY epoch DESC
LIMIT 1;

-- name: GetMidnightAriadneParamsByEpoch :one
SELECT id, epoch, datum
FROM midnight_ariadne_params
WHERE epoch = ?;

-- name: GetMidnightAriadneParamsAtOrBeforeEpoch :one
SELECT id, epoch, datum
FROM midnight_ariadne_params
WHERE epoch <= ?
ORDER BY epoch DESC
LIMIT 1;

-- name: UpsertMidnightAriadneParams :one
INSERT INTO midnight_ariadne_params (epoch, datum)
VALUES (?, ?)
ON CONFLICT (epoch) DO UPDATE SET datum = excluded.datum
RETURNING id;

-- name: DeleteMidnightAriadneParamsByEpoch :exec
DELETE FROM midnight_ariadne_params WHERE epoch = ?;

-- name: CreateMidnightAriadneRollback :one
INSERT INTO midnight_ariadne_rollbacks (
    block_number, epoch, previous_exists, previous_datum
) VALUES (?, ?, ?, ?)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: FindMidnightAriadneRollbacksByBlock :many
SELECT id, block_number, epoch, previous_exists, previous_datum
FROM midnight_ariadne_rollbacks
WHERE block_number = ?
ORDER BY epoch ASC;

-- name: DeleteMidnightAriadneRollbacksByBlock :exec
DELETE FROM midnight_ariadne_rollbacks WHERE block_number = ?;

-- name: DeleteMidnightAriadneRollbacksBeforeBlock :exec
DELETE FROM midnight_ariadne_rollbacks WHERE block_number < ?;

-- name: UpsertMidnightEpochCandidates :one
INSERT INTO midnight_epoch_candidates (epoch, block_number, candidates_cbor)
VALUES (?, ?, ?)
ON CONFLICT (epoch) DO UPDATE SET
    block_number = excluded.block_number,
    candidates_cbor = excluded.candidates_cbor
RETURNING id;

-- name: DeleteMidnightEpochCandidatesByBlock :exec
DELETE FROM midnight_epoch_candidates WHERE block_number = ?;

-- name: GetMidnightEpochCandidatesByEpoch :one
SELECT id, epoch, block_number, candidates_cbor
FROM midnight_epoch_candidates
WHERE epoch = ?;

-- name: InsertMidnightCommitteeCandidateRegistration :one
INSERT INTO midnight_committee_candidate_registrations (
    tx_hash, output_index, block_number, slot_number, tx_index, tx_inputs_cbor
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: DeleteMidnightCommitteeCandidateRegistrationsByBlock :exec
DELETE FROM midnight_committee_candidate_registrations
WHERE block_number = ?;

-- name: InsertOffchainMetadataPointer :execrows
INSERT INTO offchain_metadata (
    created_at, updated_at, url, source_type, status, hash, next_fetch_after,
    fetch_attempts, last_http_status
) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
ON CONFLICT (source_type, url, hash) DO NOTHING;

-- name: GetOffchainMetadataFetchCandidates :many
SELECT fetched_at, next_fetch_after, created_at, updated_at, url,
       source_type, status, content_type, last_error, hash, body_hash,
       content, id, fetch_attempts, last_http_status
FROM offchain_metadata
WHERE status IN ('pending', 'failed')
  AND (next_fetch_after IS NULL OR next_fetch_after <= ?)
ORDER BY next_fetch_after ASC, id ASC
LIMIT ?;

-- name: ClaimOffchainMetadataFetch :execrows
UPDATE offchain_metadata
SET next_fetch_after = ?, updated_at = ?
WHERE id = ?
  AND status IN ('pending', 'failed')
  AND (next_fetch_after IS NULL OR next_fetch_after <= ?);

-- name: SetOffchainMetadataFetchResult :exec
UPDATE offchain_metadata
SET status = ?, content_type = ?, last_error = ?, body_hash = ?,
    content = ?, fetched_at = ?, next_fetch_after = ?, fetch_attempts = ?,
    last_http_status = ?, updated_at = ?
WHERE id = ? AND updated_at = ?;

-- name: GetOffchainMetadata :one
SELECT fetched_at, next_fetch_after, created_at, updated_at, url,
       source_type, status, content_type, last_error, hash, body_hash,
       content, id, fetch_attempts, last_http_status
FROM offchain_metadata
WHERE source_type = ? AND url = ? AND hash = ?;

-- name: GetLiveUtxo :one
SELECT transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
       staking_key, credential_tag, datum_hash, spent_at_tx_id,
       referenced_by_tx_id, collateral_by_tx_id, id, added_slot,
       deleted_slot, amount, output_idx, payment_script
FROM utxo
WHERE tx_id = ? AND output_idx = ? AND deleted_slot = 0;

-- name: GetUtxoIncludingSpent :one
SELECT transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
       staking_key, credential_tag, datum_hash, spent_at_tx_id,
       referenced_by_tx_id, collateral_by_tx_id, id, added_slot,
       deleted_slot, amount, output_idx, payment_script
FROM utxo
WHERE tx_id = ? AND output_idx = ?;

-- name: GetAssetsByUtxoID :many
SELECT name, name_hex, policy_id, fingerprint, id, utxo_id, amount
FROM asset
WHERE utxo_id = ?
ORDER BY id;

-- name: GetUtxosAddedAfterSlot :many
SELECT transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
       staking_key, credential_tag, datum_hash, spent_at_tx_id,
       referenced_by_tx_id, collateral_by_tx_id, id, added_slot,
       deleted_slot, amount, output_idx, payment_script
FROM utxo
WHERE added_slot > ?
ORDER BY id DESC;

-- name: GetLiveUtxoRefsBySlot :many
SELECT tx_id, output_idx
FROM utxo
WHERE deleted_slot = 0 AND added_slot = ?
ORDER BY id;

-- name: GetUtxoRefsBySlot :many
SELECT tx_id, output_idx
FROM utxo
WHERE added_slot = ?
ORDER BY id;

-- name: GetUtxosDeletedBeforeSlot :many
SELECT transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
       staking_key, credential_tag, datum_hash, spent_at_tx_id,
       referenced_by_tx_id, collateral_by_tx_id, id, added_slot,
       deleted_slot, amount, output_idx, payment_script
FROM utxo
WHERE deleted_slot > 0 AND deleted_slot <= ?
LIMIT ?;

-- name: GetControlledAmountByCredential :many
SELECT amount
FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0;

-- name: GetScriptLockedSupply :many
SELECT amount
FROM utxo
WHERE payment_script = TRUE AND deleted_slot = 0;

-- name: GetLiveUtxosByAssetPolicy :many
SELECT u.transaction_id, u.collateral_return_for_tx_id, u.tx_id,
       u.payment_key, u.staking_key, u.credential_tag, u.datum_hash,
       u.spent_at_tx_id, u.referenced_by_tx_id, u.collateral_by_tx_id,
       u.id, u.added_slot, u.deleted_slot, u.amount, u.output_idx,
       u.payment_script
FROM utxo u
WHERE u.deleted_slot = 0
  AND u.id IN (SELECT utxo_id FROM asset WHERE policy_id = ?)
ORDER BY u.id;

-- name: GetLiveUtxosByAsset :many
SELECT u.transaction_id, u.collateral_return_for_tx_id, u.tx_id,
       u.payment_key, u.staking_key, u.credential_tag, u.datum_hash,
       u.spent_at_tx_id, u.referenced_by_tx_id, u.collateral_by_tx_id,
       u.id, u.added_slot, u.deleted_slot, u.amount, u.output_idx,
       u.payment_script
FROM utxo u
WHERE u.deleted_slot = 0
  AND u.id IN (
      SELECT utxo_id FROM asset WHERE policy_id = ? AND name = ?
  )
ORDER BY u.id;

-- name: CreateUtxo :one
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_at_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;

-- name: CreateAsset :one
INSERT INTO asset (
    name, name_hex, policy_id, fingerprint, utxo_id, amount
) VALUES (?, ?, ?, ?, ?, ?)
RETURNING id;

-- name: GetAssetByPolicyAndName :one
SELECT name, name_hex, policy_id, fingerprint, id, utxo_id, amount
FROM asset
WHERE policy_id = ? AND name = ?
ORDER BY id
LIMIT 1;

-- name: GetAssetQuantityByPolicyAndName :many
SELECT asset.amount
FROM asset
INNER JOIN utxo ON asset.utxo_id = utxo.id
WHERE asset.policy_id = ? AND asset.name = ? AND utxo.deleted_slot = 0;

-- name: GetAssetMintBurnInfo :one
SELECT
    (SELECT first_event.tx_hash
     FROM asset_mint_burn AS first_event
     WHERE first_event.policy_id = ? AND first_event.name = ?
     ORDER BY first_event.slot ASC, first_event.tx_index ASC,
              first_event.id ASC
     LIMIT 1) AS initial_tx_hash,
    COUNT(*) AS event_count
FROM asset_mint_burn AS events
WHERE events.policy_id = ? AND events.name = ?;

-- name: CreateAccount :one
INSERT INTO account (
    staking_key, credential_tag, pool, drep, added_slot, created_slot,
    certificate_id, reward, drep_type, active, expiration_epoch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;

-- name: ImportAccount :one
INSERT INTO account (
    staking_key, credential_tag, pool, drep, added_slot, created_slot,
    certificate_id, reward, drep_type, active, expiration_epoch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool = excluded.pool,
    drep = excluded.drep,
    drep_type = excluded.drep_type,
    active = excluded.active,
    reward = excluded.reward
RETURNING id;

-- name: GetActiveAccountByCredential :one
SELECT staking_key, credential_tag, pool, drep, id, added_slot,
       created_slot, certificate_id, reward, drep_type, active,
       expiration_epoch
FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE;

-- name: GetAccountByCredential :one
SELECT staking_key, credential_tag, pool, drep, id, added_slot,
       created_slot, certificate_id, reward, drep_type, active,
       expiration_epoch
FROM account
WHERE credential_tag = ? AND staking_key = ?;

-- name: CreateDrep :one
INSERT INTO drep (
    anchor_url, credential, anchor_hash, added_slot, credential_tag,
    last_activity_epoch, expiry_epoch, active
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;

-- name: CreateUtxoIfAbsent :one
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_at_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tx_id, output_idx) DO NOTHING
RETURNING id;

-- name: ImportAsset :exec
INSERT INTO asset (
    name, name_hex, policy_id, fingerprint, utxo_id, amount
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (name, policy_id, utxo_id) DO NOTHING;

-- name: GetUtxoIDByRef :one
SELECT id FROM utxo WHERE tx_id = ? AND output_idx = ?;

-- name: ImportDrep :one
INSERT INTO drep (
    anchor_url, credential, anchor_hash, added_slot, credential_tag,
    last_activity_epoch, expiry_epoch, active
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, credential) DO UPDATE SET
    anchor_url = excluded.anchor_url,
    anchor_hash = excluded.anchor_hash,
    active = excluded.active
RETURNING id;

-- name: ImportDrepRegistration :one
INSERT INTO registration_drep (
    anchor_url, drep_credential, anchor_hash, certificate_id,
    credential_tag, added_slot, deposit_amount
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, drep_credential, added_slot) DO NOTHING
RETURNING id;

-- name: GetDrepByHash :one
SELECT anchor_url, credential, anchor_hash, id, added_slot, credential_tag,
       last_activity_epoch, expiry_epoch, active
FROM drep
WHERE credential = ?
LIMIT 1;

-- name: GetActiveDrepByHash :one
SELECT anchor_url, credential, anchor_hash, id, added_slot, credential_tag,
       last_activity_epoch, expiry_epoch, active
FROM drep
WHERE credential = ? AND active = TRUE
LIMIT 1;

-- name: GetDrepByCredential :one
SELECT anchor_url, credential, anchor_hash, id, added_slot, credential_tag,
       last_activity_epoch, expiry_epoch, active
FROM drep
WHERE credential_tag = ? AND credential = ?;

-- name: GetActiveDrepByCredential :one
SELECT anchor_url, credential, anchor_hash, id, added_slot, credential_tag,
       last_activity_epoch, expiry_epoch, active
FROM drep
WHERE credential_tag = ? AND credential = ? AND active = TRUE;

-- name: GetActiveDreps :many
SELECT anchor_url, credential, anchor_hash, id, added_slot, credential_tag,
       last_activity_epoch, expiry_epoch, active
FROM drep
WHERE active = TRUE;

-- name: SetDrep :exec
INSERT INTO drep (
    credential_tag, credential, added_slot, anchor_url, anchor_hash, active
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, credential) DO UPDATE SET
    added_slot = excluded.added_slot,
    anchor_url = excluded.anchor_url,
    anchor_hash = excluded.anchor_hash,
    active = excluded.active;

-- name: InsertDrepIfAbsent :exec
INSERT INTO drep (
    credential_tag, credential, added_slot, anchor_url, anchor_hash, active
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;

-- name: GetDRepDelegators :many
SELECT credential_tag, staking_key
FROM account
WHERE drep = ? AND drep_type = ? AND active = TRUE
ORDER BY credential_tag, staking_key;

-- name: UpdateDRepActivity :execrows
UPDATE drep
SET last_activity_epoch = ?, expiry_epoch = ?
WHERE credential_tag = ? AND credential = ?;

-- name: GetExpiredDReps :many
SELECT anchor_url, credential, anchor_hash, id, added_slot, credential_tag,
       last_activity_epoch, expiry_epoch, active
FROM drep
WHERE active = TRUE AND expiry_epoch > 0 AND expiry_epoch <= ?;

-- name: GetDrepLastRegistrationSlot :one
SELECT CAST(COALESCE(MAX(added_slot), 0) AS INTEGER)
FROM registration_drep
WHERE credential_tag = ? AND drep_credential = ?
  AND certificate_id IS NOT NULL AND certificate_id != 0;

-- name: GetTransactionByHash :one
SELECT hash, block_hash, metadata, slot, type, id, fee, collateral_fee,
       ttl, block_index, valid
FROM "transaction"
WHERE hash = ?;

-- name: GetTransactionSlotByHash :one
SELECT slot FROM "transaction" WHERE hash = ?;

-- name: GetTransactionIDByHash :one
SELECT id FROM "transaction" WHERE hash = ?;

-- name: GetTransactionMetadataByHash :one
SELECT metadata FROM "transaction" WHERE hash = ?;

-- name: GetTransactionsByBlockHash :many
SELECT hash, block_hash, metadata, slot, type, id, fee, collateral_fee,
       ttl, block_index, valid
FROM "transaction"
WHERE block_hash = ?
ORDER BY block_index ASC;

-- name: SumTransactionFeesInSlotRange :many
SELECT CASE WHEN valid THEN fee ELSE collateral_fee END
FROM "transaction"
WHERE slot >= ? AND slot <= ?;

-- name: GetTransactionHashesAfterSlot :many
SELECT hash FROM "transaction" WHERE slot > ?;

-- name: DeleteAddressTransactionsAfterSlot :exec
DELETE FROM address_transaction WHERE slot > ?;

-- name: DeleteTransactionMetadataLabelsAfterSlot :exec
DELETE FROM transaction_metadata_label WHERE slot > ?;

-- name: CountTransactionsByPaymentCred :one
SELECT COUNT(DISTINCT transaction_id)
FROM address_transaction
WHERE payment_key = ?;

-- name: CountTransactionsByMetadataLabel :one
SELECT COUNT(*) FROM transaction_metadata_label WHERE label = ?;

-- name: CountTransactionsInSlotRange :one
SELECT COUNT(*)
FROM "transaction"
WHERE slot >= ? AND slot <= ?;

-- name: GetBlockSlotRangeStats :one
SELECT COUNT(*) AS count,
       CAST(COALESCE(MIN(slot), 0) AS INTEGER) AS first_slot,
       CAST(COALESCE(MAX(slot), 0) AS INTEGER) AS last_slot
FROM block_nonce
WHERE slot >= ? AND slot <= ?;

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"context"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type MetadataStore interface {
	// Database management methods

	// Close closes the metadata store and releases all resources.
	Close() error

	// GetCommitTimestamp retrieves the last commit timestamp from the database.
	GetCommitTimestamp() (int64, error)

	// SetCommitTimestamp sets the last commit timestamp in the database.
	// Parameter order is (timestamp, txn) to match other store methods where
	// the transaction is the final parameter.
	SetCommitTimestamp(int64, types.Txn) error

	// GetNodeSettings returns the persisted immutable node settings, or
	// nil if the database has never been initialised.
	GetNodeSettings() (*types.NodeSettings, error)

	// SetNodeSettings persists the immutable node settings via an
	// idempotent insert that succeeds on repeated calls. If the row
	// already exists, implementations must not overwrite immutable
	// fields and should only populate network fields when they are
	// currently unset so callers like checkNodeSettings can perform
	// a one-time network backfill.
	SetNodeSettings(*types.NodeSettings) error

	// Transaction creates a new metadata transaction on the write
	// connection pool. Use ReadTransaction for read-only access to
	// avoid contending with writers.
	Transaction() types.Txn

	// ReadTransaction creates a read-only metadata transaction using
	// the read connection pool (when available). This avoids blocking
	// on the write connection, which is critical for operations like
	// FindIntersect that must complete within protocol timeouts.
	ReadTransaction() types.Txn

	// Ledger state methods

	// AddUtxos adds one or more unspent transaction outputs to the database.
	AddUtxos(
		[]models.UtxoSlot,
		types.Txn,
	) error

	// Bulk import methods (ledger state restore from snapshot)

	// ImportUtxos inserts UTxOs in bulk, ignoring duplicates.
	ImportUtxos([]models.Utxo, types.Txn) error

	// ImportAccount upserts an account (insert or update delegation
	// fields on conflict).
	ImportAccount(*models.Account, types.Txn) error

	// ImportPool upserts a pool and creates a registration record.
	ImportPool(
		*models.Pool,
		*models.PoolRegistration,
		types.Txn,
	) error

	// ImportDrep upserts a DRep and creates a registration record.
	ImportDrep(
		*models.Drep,
		*models.RegistrationDrep,
		types.Txn,
	) error

	// CreateDrep inserts a Drep row directly. Used by callers (e.g.
	// fixture seeding from outside the plugin packages) that already
	// have a fully-populated model and want a single-row insert without
	// the registration-record side effects of ImportDrep.
	CreateDrep(types.Txn, *models.Drep) error

	// CreateAccount inserts an Account row directly. See CreateDrep
	// for the rationale; this is the simple-insert sibling of
	// ImportAccount.
	CreateAccount(types.Txn, *models.Account) error

	// CreateUtxo inserts a Utxo row directly. The normal block-
	// application path uses AddUtxos with UtxoSlot inputs; this is
	// the simple-insert variant for callers that already have a
	// populated model.
	CreateUtxo(types.Txn, *models.Utxo) error

	// CreateMidnightAssetCreate inserts a cNIGHT UTxO creation row.
	CreateMidnightAssetCreate(types.Txn, *models.MidnightAssetCreate) error

	// CreateMidnightAssetSpend inserts a cNIGHT UTxO spend row.
	CreateMidnightAssetSpend(types.Txn, *models.MidnightAssetSpend) error

	// CreateMidnightRegistration inserts a mapping-validator registration row.
	CreateMidnightRegistration(types.Txn, *models.MidnightRegistration) error

	// CreateMidnightDeregistration inserts a mapping-validator deregistration row.
	CreateMidnightDeregistration(types.Txn, *models.MidnightDeregistration) error

	// FindUnspentMidnightAssetCreates returns cNIGHT UTxO create rows that
	// have no matching spend row. Used to restore the in-memory tracked-UTxO
	// set on startup.
	FindUnspentMidnightAssetCreates() ([]models.MidnightAssetCreate, error)

	// FindUnspentMidnightRegistrations returns registration rows that have no
	// matching deregistration row. Used to restore the in-memory tracked-UTxO
	// set on startup.
	FindUnspentMidnightRegistrations() ([]models.MidnightRegistration, error)

	// DeleteMidnightAssetCreatesByBlock removes all cNIGHT create rows for
	// the given block number and returns them so the caller can update the
	// in-memory tracked-UTxO set. Used during chain rollback.
	DeleteMidnightAssetCreatesByBlock(types.Txn, uint64) ([]models.MidnightAssetCreate, error)

	// DeleteMidnightAssetSpendsByBlock removes all cNIGHT spend rows for the
	// given block number and returns them so the caller can restore the
	// in-memory tracked-UTxO set. Used during chain rollback.
	DeleteMidnightAssetSpendsByBlock(types.Txn, uint64) ([]models.MidnightAssetSpend, error)

	// DeleteMidnightRegistrationsByBlock removes all registration rows for
	// the given block number and returns them so the caller can update the
	// in-memory tracked-UTxO set. Used during chain rollback.
	DeleteMidnightRegistrationsByBlock(types.Txn, uint64) ([]models.MidnightRegistration, error)

	// DeleteMidnightDeregistrationsByBlock removes all deregistration rows
	// for the given block number and returns them so the caller can restore
	// the in-memory tracked-UTxO set. Used during chain rollback.
	DeleteMidnightDeregistrationsByBlock(types.Txn, uint64) ([]models.MidnightDeregistration, error)

	// FindMidnightAssetCreatesFrom returns cNIGHT create rows ordered by
	// (block_number, tx_index) ascending, starting strictly after
	// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
	// The result may hold more than limit rows: (block_number, tx_index) is
	// not a unique key (one tx can write several rows to the same table),
	// so implementations extend a page that would otherwise end mid-key to
	// include the rest of that key's rows, keeping the cursor gap-free.
	// Used to serve the MidnightState GetAssetCreates RPC.
	FindMidnightAssetCreatesFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightAssetCreate, error)

	// FindMidnightAssetSpendsFrom returns cNIGHT spend rows ordered by
	// (block_number, tx_index) ascending, starting strictly after
	// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
	// See FindMidnightAssetCreatesFrom for why the result may hold more
	// than limit rows. Used to serve the MidnightState GetAssetSpends RPC.
	FindMidnightAssetSpendsFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightAssetSpend, error)

	// FindMidnightRegistrationsFrom returns registration rows ordered by
	// (block_number, tx_index) ascending, starting strictly after
	// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
	// See FindMidnightAssetCreatesFrom for why the result may hold more
	// than limit rows. Used to serve the MidnightState GetRegistrations RPC.
	FindMidnightRegistrationsFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightRegistration, error)

	// FindMidnightDeregistrationsFrom returns deregistration rows ordered by
	// (block_number, tx_index) ascending, starting strictly after
	// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
	// See FindMidnightAssetCreatesFrom for why the result may hold more
	// than limit rows. Used to serve the MidnightState GetDeregistrations
	// RPC.
	FindMidnightDeregistrationsFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightDeregistration, error)

	// GetImportCheckpoint retrieves the checkpoint for a given
	// import key (e.g., "{digest}:{slot}"). Returns nil if no
	// checkpoint exists.
	GetImportCheckpoint(
		importKey string,
		txn types.Txn,
	) (*models.ImportCheckpoint, error)

	// SetImportCheckpoint creates or updates a checkpoint for
	// the given import key with the completed phase.
	SetImportCheckpoint(
		checkpoint *models.ImportCheckpoint,
		txn types.Txn,
	) error

	// EnsureOffchainMetadataPointers creates pending cache rows for
	// on-chain pool metadata and governance anchor URL/hash pointers.
	EnsureOffchainMetadataPointers(
		ctx context.Context,
		now time.Time,
		txn types.Txn,
	) (int, error)

	// GetOffchainMetadataFetchBatch returns pending or failed
	// off-chain metadata rows that are due to be fetched.
	GetOffchainMetadataFetchBatch(
		ctx context.Context,
		limit int,
		now time.Time,
		txn types.Txn,
	) ([]models.OffchainMetadata, error)

	// SetOffchainMetadataFetchResult updates a cache row after a fetch
	// attempt.
	SetOffchainMetadataFetchResult(
		ctx context.Context,
		doc *models.OffchainMetadata,
		txn types.Txn,
	) error

	// GetOffchainMetadata retrieves a cached off-chain document by its
	// source type and on-chain URL/hash pointer.
	GetOffchainMetadata(
		sourceType string,
		url string,
		hash []byte,
		txn types.Txn,
	) (*models.OffchainMetadata, error)

	// GetPoolRegistrations retrieves all registration certificates for a pool.
	GetPoolRegistrations(
		lcommon.PoolKeyHash,
		types.Txn,
	) ([]lcommon.PoolRegistrationCertificate, error)

	// GetPool retrieves a pool by its key hash, optionally including inactive pools.
	GetPool(
		lcommon.PoolKeyHash,
		bool, // includeInactive
		types.Txn,
	) (*models.Pool, error)

	UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash,
		uint64, // sequence
		uint64, // slot
		types.Txn,
	) error

	LatestPoolOpCertSequence(
		lcommon.PoolKeyHash,
		types.Txn,
	) (uint64, bool, error)

	// GetPoolBlockIssuersInSlotRange returns observed pool/op-cert issuer
	// rows in the inclusive slot range, ordered by slot and pool key hash.
	GetPoolBlockIssuersInSlotRange(
		uint64, // startSlot
		uint64, // endSlot
		types.Txn,
	) ([]models.PoolOpCertSequence, error)

	// CountPoolBlocksInSlotRange counts observed pool-issued blocks in the
	// inclusive slot range, grouped by pool key hash. The total return value
	// counts all observed pool blocks in the range, not only the requested
	// pools.
	CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash,
		uint64, // startSlot
		uint64, // endSlot
		types.Txn,
	) (map[string]uint64, uint64, error)

	// SumTransactionFeesInSlotRange sums the fee-pot contributions in the
	// inclusive slot range: declared fees of valid transactions plus
	// consumed collateral of phase-2-invalid transactions.
	SumTransactionFeesInSlotRange(
		uint64, // startSlot
		uint64, // endSlot
		types.Txn,
	) (uint64, error)

	// GetPools retrieves pools by key hash in batch.
	GetPools(
		[]lcommon.PoolKeyHash,
		types.Txn,
	) ([]models.Pool, error)

	// GetPoolRegistrationsAtSlot retrieves the latest registration for each
	// requested pool at or before the supplied slot. Same-slot ordering must
	// use block_index and cert_index so reward inputs reflect the historical
	// epoch boundary, not the current denormalized pool row.
	GetPoolRegistrationsAtSlot(
		[]lcommon.PoolKeyHash,
		uint64, // slot
		types.Txn,
	) ([]models.PoolRegistration, error)

	// GetPoolRegistrationsEffectiveForEpoch retrieves, per requested pool,
	// the registration whose parameters the ledger's pool-params map held
	// during the ended epoch [epochStartSlot, snapshotSlot]. Re-registrations
	// submitted during that epoch are future params (promoted after SNAP)
	// and excluded; pools that freshly entered the params map during the
	// epoch use their earliest in-epoch certificate.
	GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash,
		uint64, // epochStartSlot
		uint64, // endedEpoch
		uint64, // snapshotSlot
		types.Txn,
	) ([]models.PoolRegistration, error)

	// GetPoolByVrfKeyHash retrieves an active pool by its VRF key hash.
	// Returns nil if no active pool uses this VRF key.
	GetPoolByVrfKeyHash(
		[]byte, // vrfKeyHash
		types.Txn,
	) (*models.Pool, error)

	// GetActivePoolRelays retrieves all relays from currently active pools.
	// This is used for ledger peer discovery.
	GetActivePoolRelays(types.Txn) ([]models.PoolRegistrationRelay, error)

	// GetActivePoolKeyHashes retrieves the key hashes of all currently active pools.
	// A pool is active if it has a registration and either no retirement or
	// the retirement epoch is in the future.
	GetActivePoolKeyHashes(types.Txn) ([][]byte, error)

	// GetActivePoolKeyHashesAtSlot retrieves the key hashes of pools that were
	// active at the given slot. A pool was active at a slot if:
	// 1. It had a registration with added_slot <= slot
	// 2. Either:
	//    a. No retirement with added_slot <= slot, OR
	//    b. The most recent retirement was for an epoch that hadn't started yet, OR
	//    c. A registration occurred AFTER the most recent retirement (re-registration
	//       cancels a pending retirement)
	//
	// When determining order of events in the same slot, block_index (transaction
	// index within block) and cert_index (certificate index within transaction)
	// are used as tie-breakers since cert_index resets per transaction. The full
	// ordering is: added_slot DESC, block_index DESC, cert_index DESC.
	// This handles cases where registration and retirement occur in different
	// transactions within the same block.
	//
	// This is used for stake snapshot calculations at historical points.
	//
	// Returns types.ErrNoEpochData (wrapped) if epoch data has not been synced
	// for the requested slot. Callers should use errors.Is() to check.
	GetActivePoolKeyHashesAtSlot(uint64, types.Txn) ([][]byte, error)

	// GetPoolsRetiringAtEpoch returns the pools whose effective retirement
	// (the latest retirement not cancelled by a later re-registration, as of
	// the boundary slot) takes effect at the given epoch, along with the
	// reward account and deposit from their active registration. Used to apply
	// POOLREAP deposit refunds at the epoch boundary.
	GetPoolsRetiringAtEpoch(
		epoch uint64,
		boundarySlot uint64,
		txn types.Txn,
	) ([]models.PoolRetirementRefund, error)

	// GetMIRCertsInSlotRange returns the processed effects of all MIR
	// certificates whose added_slot is >= startSlot and < endSlot. Used to
	// apply the Shelley-era INSTANT rule at each epoch boundary.
	GetMIRCertsInSlotRange(
		startSlot, endSlot uint64,
		txn types.Txn,
	) ([]models.MIREffect, error)

	// GetStakeByPool returns the total delegated stake and delegator count for a pool.
	// This aggregates all accounts delegated to the pool and sums their UTxO values.
	GetStakeByPool(
		[]byte, // poolKeyHash
		types.Txn,
	) (uint64, uint64, error) // (totalStake, delegatorCount, error)

	// GetStakeByPools returns delegated stake for multiple pools in a single query.
	// Returns maps of pool key hash -> total stake and pool key hash -> delegator count.
	GetStakeByPools(
		[][]byte, // poolKeyHashes
		types.Txn,
	) (map[string]uint64, map[string]uint64, error)

	// GetStakeByPoolsAtSlot returns delegated stake for multiple pools at a
	// historical slot. It uses certificate history plus slot-aware UTxO
	// liveness so epoch-boundary stake snapshots do not read current live
	// stake for an older boundary.
	GetStakeByPoolsAtSlot(
		[][]byte, // poolKeyHashes
		uint64, // slot
		types.Txn,
	) (map[string]uint64, map[string]uint64, error)

	// GetRewardStakeInputsAtSlot returns positive per-account delegated stake
	// for pools from the live reward stake aggregate. Callers use it at epoch
	// boundaries, where the aggregate has already been maintained by block
	// application and rollback repair. The slot is copied to captured fields
	// by callers; this method does not scan historical UTxOs.
	GetRewardStakeInputsAtSlot(
		[][]byte, // poolKeyHashes
		uint64, // slot
		types.Txn,
	) ([]*models.RewardStakeInput, error)

	// RebuildRewardLiveStake rebuilds the live reward stake aggregate from
	// canonical account and live UTxO metadata. It is used after bulk ledger
	// state import and as deterministic rollback/repair backstop.
	RebuildRewardLiveStake(uint64, types.Txn) error

	// RewardLiveStakeNeedsBackfill reports whether the reward_live_stake
	// aggregate needs a one-time RebuildRewardLiveStake pass: true when the
	// account table already has rows but the aggregate is completely empty.
	// This detects an upgraded database that predates the aggregate and
	// never received the backfill (see #1959), without misfiring on a
	// legitimately fresh, empty database.
	RewardLiveStakeNeedsBackfill(types.Txn) (bool, error)

	// GetStakeRegistrationsByCredential retrieves stake registration certificates
	// using the full credential identity: credential tag plus 28-byte hash.
	GetStakeRegistrationsByCredential(
		uint8, // credentialTag
		[]byte, // stakeKey
		types.Txn,
	) ([]lcommon.StakeRegistrationCertificate, error)

	// GetTip retrieves the current chain tip.
	GetTip(types.Txn) (ochainsync.Tip, error)

	// GetAccountByCredential retrieves an account using the full stake credential identity.
	// The credential tag separates key and script credentials that share the same hash.
	GetAccountByCredential(
		uint8, // credentialTag
		[]byte, // stakeKey
		bool, // includeInactive
		types.Txn,
	) (*models.Account, error)

	// GetAccountsByCredential retrieves accounts in batch using tag-aware stake credentials.
	// The returned map is keyed by StakeCredentialRef.MapKey() for tag plus hash lookups.
	GetAccountsByCredential(
		[]models.StakeCredentialRef, // stakeCredentials
		bool, // includeInactive
		types.Txn,
	) (map[string]*models.Account, error)

	// GetAccountsActiveAtSlot returns the subset of stake credentials that
	// were registered and not subsequently deregistered at or before the given
	// slot. The returned map is keyed by StakeCredentialRef.MapKey().
	GetAccountsActiveAtSlot(
		[]models.StakeCredentialRef, // stakeCredentials
		uint64, // slot
		types.Txn,
	) (map[string]struct{}, error)

	// ApplyAccountRewardWithdrawal clears a registered reward account after a
	// validated transaction withdrawal and records rollback state.
	ApplyAccountRewardWithdrawal(
		uint8, // credentialTag
		[]byte, // stakeKey
		uint64, // amount
		uint64, // slot
		[]byte, // txHash
		types.Txn,
	) error

	// AddAccountRewardByCredential credits rewards using the full stake credential identity.
	// The credential tag prevents key and script reward accounts with the same hash from merging.
	//
	// sourceHash uniquely identifies the credit event that produced this
	// reward (the refunded governance proposal identity hash, the reaped pool
	// key hash, or a synthetic MIR event discriminator). It is recorded in the
	// delta journal's tx_hash column so each distinct credit at an epoch
	// boundary is its own rollback-aware row, while re-applying the same
	// boundary on a crash-replay maps onto the existing row and is skipped
	// idempotently instead of colliding on the unique index. Pass nil for
	// callers without a natural per-event discriminator.
	AddAccountRewardByCredential(
		uint8, // credentialTag
		[]byte, // stakeKey
		uint64, // amount
		uint64, // slot
		[]byte, // sourceHash
		types.Txn,
	) error

	// DeleteAccountRewardsAfterSlot reverts reward balance changes recorded
	// after the given slot and deletes their journal entries.
	DeleteAccountRewardsAfterSlot(uint64, types.Txn) error

	// GetBlockNonce retrieves a block nonce for a given point.
	GetBlockNonce(
		ocommon.Point,
		types.Txn,
	) ([]byte, error)

	// GetBlockNoncesInSlotRange retrieves all block nonces in [startSlot, endSlot).
	GetBlockNoncesInSlotRange(
		startSlot uint64,
		endSlot uint64,
		txn types.Txn,
	) ([]models.BlockNonce, error)

	// GetLastBlockNonceInRange retrieves the block nonce with the highest slot
	// in [startSlot, endSlot). Returns nil nonce and no error if none found.
	GetLastBlockNonceInRange(
		startSlot uint64,
		endSlot uint64,
		txn types.Txn,
	) ([]byte, error)

	// GetDatum retrieves a datum by its hash, returning nil if not found.
	GetDatum(
		lcommon.Blake2b256,
		types.Txn,
	) (*models.Datum, error)

	// GetDrep retrieves a DRep by credential hash only (no tag filter).
	// Used for the protocol validation path where only a Blake2b224 hash
	// is available (e.g. LedgerView.DRepRegistration from gouroboros).
	GetDrep(
		[]byte, // credential
		bool, // includeInactive
		types.Txn,
	) (*models.Drep, error)

	// GetDrepByCredential retrieves a DRep using the full credential
	// identity: tag (0=key, 1=script) plus 28-byte hash. Use this for
	// all internal callers that know the credential type.
	GetDrepByCredential(
		uint8, // credentialTag
		[]byte, // credential
		bool, // includeInactive
		types.Txn,
	) (*models.Drep, error)

	// GetActiveDreps retrieves all active DReps.
	GetActiveDreps(types.Txn) ([]*models.Drep, error)

	// GetActiveAccountCredentials returns the stake credentials (tag + key) of
	// every currently active account. Used by Mithril v2 catch-up
	// reconciliation to find accounts absent from a newer snapshot's live set.
	GetActiveAccountCredentials(
		types.Txn,
	) ([]models.StakeCredentialRef, error)

	// DeactivateAccounts marks the given accounts inactive (Active=false). Used
	// by Mithril v2 catch-up reconciliation; rows are never deleted, only
	// tombstoned via the active flag. Credentials that match no row are ignored.
	DeactivateAccounts(types.Txn, []models.StakeCredentialRef) error

	// DeactivateDreps marks the given DReps inactive (Active=false). Used by
	// Mithril v2 catch-up reconciliation; rows are never deleted, only
	// tombstoned via the active flag. Credentials that match no row are ignored.
	DeactivateDreps(types.Txn, []models.StakeCredentialRef) error

	// RetirePools records a retirement at the given epoch (and added slot) for
	// each supplied pool key hash, mirroring a retirement certificate. Used by
	// Mithril v2 catch-up reconciliation to retire pools absent from a newer
	// snapshot's active set; registrations are preserved. Key hashes that match
	// no pool are ignored.
	RetirePools(
		txn types.Txn,
		poolKeyHashes [][]byte,
		epoch uint64,
		addedSlot uint64,
	) error

	// GetPParams retrieves the latest protocol-parameters row at
	// epoch <= the supplied epoch whose stored era_id matches the
	// supplied era. The era filter is required: at era boundaries the
	// rollover path writes both an old-era row (post-pparams-update)
	// and a new-era row (transitionToEra) at the same epoch, and an
	// unfiltered query collapses them to whichever was inserted last
	// — which is the new-era shape. Callers commit to a specific
	// era's struct decoder, so the row's era_id must match the chosen
	// decoder for the CBOR to decode.
	GetPParams(
		uint64, // epoch
		uint, // eraId
		types.Txn,
	) ([]models.PParams, error)

	// GetPParamUpdates retrieves protocol parameter updates for a given epoch.
	GetPParamUpdates(
		uint64, // epoch
		types.Txn,
	) ([]models.PParamUpdate, error)

	// GetGenesisDelegationForSlot returns the latest genesis-key delegation
	// certificate for genesisHash before the supplied block slot.
	GetGenesisDelegationForSlot(
		[]byte, // genesisHash
		uint64, // blockSlot
		types.Txn,
	) (*models.GenesisDelegation, error)

	// GetUtxo retrieves an unspent transaction output by transaction ID and index.
	GetUtxo(
		[]byte, // txId
		uint32, // idx
		types.Txn,
	) (*models.Utxo, error)

	// GetUtxoIncludingSpent retrieves a transaction output by
	// transaction ID and index, including spent outputs.
	GetUtxoIncludingSpent(
		[]byte, // txId
		uint32, // idx
		types.Txn,
	) (*models.Utxo, error)

	// GetTransactionByHash retrieves a transaction by its hash.
	GetTransactionByHash(
		[]byte, // hash
		types.Txn,
	) (*models.Transaction, error)

	// GetTransactionSlotByHash returns the slot of the block that
	// contains the given tx hash. The bool result is false when no
	// such transaction is recorded. Lighter than GetTransactionByHash
	// because it skips loading inputs/outputs/witnesses.
	GetTransactionSlotByHash(
		[]byte, // hash
		types.Txn,
	) (uint64, bool, error)

	// GetTransactionIDByHash returns the primary-key ID of the
	// transaction with the given hash. The bool result is false when
	// no such transaction is recorded. Used by UTxO recovery paths
	// that need to populate the producer transaction FK on rows they
	// re-import without paying the cost of loading every association.
	GetTransactionIDByHash(
		[]byte, // hash
		types.Txn,
	) (uint, bool, error)

	// GetTransactionMetadataByHash returns only the stored (API-mode)
	// CBOR metadata blob for the transaction with the given hash,
	// without loading any associations. Returns (nil, nil) when no such
	// transaction exists or when it has no metadata. Used by the asset
	// endpoint to resolve CIP-25 on-chain metadata without paying for
	// full transaction preloads.
	GetTransactionMetadataByHash(
		[]byte, // hash
		types.Txn,
	) ([]byte, error)

	// GetTransactionsByHashes retrieves transactions by their hashes.
	GetTransactionsByHashes(
		[][]byte, // hashes
		types.Txn,
	) ([]models.Transaction, error)

	// GetTransactionsByBlockHash retrieves all transactions
	// for a given block hash, ordered by block_index.
	GetTransactionsByBlockHash(
		[]byte, // blockHash
		types.Txn,
	) ([]models.Transaction, error)

	// GetTransactionsByAddress retrieves transactions involving
	// the provided payment/staking credential pair with pagination and ordering.
	GetTransactionsByAddress(
		[]byte, // paymentKey
		uint8, // credentialTag
		[]byte, // stakingKey
		int, // limit
		int, // offset
		string, // order (asc|desc)
		types.Txn,
	) ([]models.Transaction, error)

	// CountTransactionsByAddress returns the total number of
	// transactions involving the provided payment/staking credential pair.
	CountTransactionsByAddress(
		[]byte, // paymentKey
		uint8, // credentialTag
		[]byte, // stakingKey
		types.Txn,
	) (int, error)

	// GetAddressesByCredential retrieves distinct address mappings for a stake credential.
	GetAddressesByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		int, // limit
		int, // offset
		string, // order (asc|desc)
		types.Txn,
	) ([]models.AddressTransaction, error)

	// CountAddressesByCredential retrieves the total count of distinct address mappings for a stake credential.
	CountAddressesByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		types.Txn,
	) (int, error)

	// GetAccountDelegationHistoryByCredential retrieves delegation history
	// rows for a stake credential tag/hash pair.
	GetAccountDelegationHistoryByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		int, // limit
		int, // offset
		string, // order (asc|desc)
		types.Txn,
	) ([]models.AccountDelegationHistoryRow, error)

	// CountAccountDelegationHistoryByCredential retrieves the total count of
	// delegation history rows for a stake credential tag/hash pair.
	CountAccountDelegationHistoryByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		types.Txn,
	) (int, error)

	// GetAccountRegistrationHistoryByCredential retrieves registration history
	// rows for a stake credential tag/hash pair.
	GetAccountRegistrationHistoryByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		int, // limit
		int, // offset
		string, // order (asc|desc)
		types.Txn,
	) ([]models.AccountRegistrationHistoryRow, error)

	// CountAccountRegistrationHistoryByCredential retrieves the total count of
	// registration history rows for a stake credential tag/hash pair.
	CountAccountRegistrationHistoryByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		types.Txn,
	) (int, error)

	// GetAccountSumsByCredential retrieves the aggregated withdrawal, reserves,
	// and treasury lovelace totals for a stake credential tag/hash pair.
	GetAccountSumsByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		types.Txn,
	) (models.AccountSums, error)

	// GetTransactionsByMetadataLabel retrieves transactions that include
	// metadata for the given label.
	GetTransactionsByMetadataLabel(
		uint64, // label
		int, // limit
		int, // offset
		bool, // descending
		types.Txn,
	) ([]models.Transaction, error)

	// CountTransactionsByMetadataLabel returns the total number of
	// transactions that include metadata for the given label.
	CountTransactionsByMetadataLabel(
		uint64, // label
		types.Txn,
	) (int, error)

	// GetAssetByPolicyAndName returns a live asset row for the provided
	// policy ID and asset name. Implementations return an empty model and
	// no error when the asset is not found.
	GetAssetByPolicyAndName(
		lcommon.Blake2b224,
		[]byte, // assetName
		types.Txn,
	) (models.Asset, error)

	// GetAssetQuantityByPolicyAndName returns the sum of live quantities for
	// the provided policy ID and asset name across all matching UTxOs.
	GetAssetQuantityByPolicyAndName(
		lcommon.Blake2b224,
		[]byte, // assetName
		types.Txn,
	) (uint64, error)

	// GetAssetMintBurnInfo returns the hash of the earliest transaction that
	// minted the asset (its initial mint) and the total number of recorded
	// mint/burn events for the asset. Returns (nil, 0, nil) when the asset has
	// no recorded mint/burn history (e.g. running in core storage mode).
	GetAssetMintBurnInfo(
		lcommon.Blake2b224,
		[]byte, // assetName
		types.Txn,
	) (initialMintTxHash []byte, mintOrBurnCount int, err error)

	// GetScript retrieves a script by its hash.
	GetScript(
		lcommon.ScriptHash,
		types.Txn,
	) (*models.Script, error)

	// SetBlockNonce stores a block nonce for a given block hash and slot.
	SetBlockNonce(
		[]byte, // blockHash
		uint64, // slotNumber
		[]byte, // nonce
		bool, // isCheckpoint
		types.Txn,
	) error

	// SetDatum stores a datum with its hash and slot.
	SetDatum(
		lcommon.Blake2b256,
		[]byte,
		uint64, // slot
		types.Txn,
	) error

	// SetEpoch sets epoch information.
	SetEpoch(
		uint64, // slot
		uint64, // epoch
		[]byte, // nonce
		[]byte, // evolvingNonce
		[]byte, // candidateNonce
		[]byte, // lastEpochBlockNonce
		uint, // era
		uint, // slotLength
		uint, // lengthInSlots
		types.Txn,
	) error

	// SetPParams stores protocol parameters.
	SetPParams(
		[]byte, // params
		uint64, // slot
		uint64, // epoch
		uint, // eraId
		types.Txn,
	) error

	// SetPParamUpdate stores a protocol parameter update.
	SetPParamUpdate(
		[]byte, // genesis
		[]byte, // update
		uint64, // slot
		uint64, // epoch
		types.Txn,
	) error

	// SetTip sets the current chain tip.
	SetTip(
		ochainsync.Tip,
		types.Txn,
	) error

	// SetTransaction stores a transaction with its metadata.
	SetTransaction(
		lcommon.Transaction,
		ocommon.Point,
		uint32, // idx
		map[int]uint64, // certDeposits: indexed by certificate position in tx.Certificates(); absent keys are treated as zero/no deposit
		types.Txn,
	) error

	// NewBatchAccumulator creates a metadata-plugin-specific accumulator
	// for batched transaction ingestion.
	NewBatchAccumulator() types.MetadataBatchAccumulator

	// FlushBatch writes accumulated batched metadata rows.
	FlushBatch(
		types.MetadataBatchAccumulator,
		types.Txn,
	) error

	// SetTransactionBatched stores transaction metadata while accumulating
	// batchable rows into the provided accumulator for a later FlushBatch.
	SetTransactionBatched(
		lcommon.Transaction,
		ocommon.Point,
		uint32, // idx
		map[int]uint64, // certDeposits
		types.MetadataBatchAccumulator,
		types.Txn,
	) error

	// SetGapBlockTransaction stores a transaction record and its
	// produced outputs without looking up or consuming input UTxOs.
	// This is used for mithril gap blocks where the snapshot's UTxO
	// set already reflects the correct spent/unspent state.
	SetGapBlockTransaction(
		lcommon.Transaction,
		ocommon.Point,
		uint32, // idx
		types.Txn,
	) error

	// RecomputeGapCollateralFee recomputes and persists the collateral fee
	// for a phase-2-invalid gap-block transaction after its consumed
	// collateral inputs have been recovered into the metadata UTxO table.
	// SetGapBlockTransaction computes the collateral fee before those inputs
	// exist, so for a transaction that declares no total collateral the fee
	// is undercounted until this recompute runs. It is a no-op for valid
	// transactions (which have no collateral fee).
	RecomputeGapCollateralFee(
		lcommon.Transaction,
		ocommon.Point,
		types.Txn,
	) error

	// SetGenesisTransaction stores a genesis transaction record.
	// Genesis transactions have no inputs, witnesses, or fees - just outputs.
	SetGenesisTransaction(
		hash []byte,
		blockHash []byte,
		outputs []models.Utxo,
		txn types.Txn,
	) error

	// SetGenesisStaking stores genesis pool registrations and stake
	// delegations from the shelley-genesis.json staking section.
	// pools maps pool key hash (hex) to its registration certificate.
	// stakeDelegations maps staking credential hash (hex) to pool key hash (hex).
	SetGenesisStaking(
		pools map[string]lcommon.PoolRegistrationCertificate,
		stakeDelegations map[string]string,
		blockHash []byte,
		txn types.Txn,
	) error

	// SetGenesisGovernance stores the initial DReps and stake/vote
	// delegations from the conway-genesis.json governance bootstrap
	// section. Records are stamped with slot 0 so they appear in the
	// ledger as having been present since genesis.
	SetGenesisGovernance(
		initialDReps conway.ConwayGenesisInitialDReps,
		delegs conway.ConwayGenesisDelegs,
		blockHash []byte,
		txn types.Txn,
	) error

	// Helper methods

	// DeleteBlockNoncesBeforeSlot removes block nonces older than the given slot.
	DeleteBlockNoncesBeforeSlot(uint64, types.Txn) error

	// DeleteBlockNoncesBeforeSlotWithoutCheckpoints removes block nonces older than the given slot,
	// excluding checkpoint nonces.
	DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		uint64,
		types.Txn,
	) error

	// DeleteBlockNoncesAfterPoint removes block nonces after a rollback
	// point and competing nonces at the same slot.
	DeleteBlockNoncesAfterPoint(ocommon.Point, types.Txn) error

	// DeleteUtxo removes a single unspent transaction output.
	DeleteUtxo(models.UtxoId, types.Txn) error

	// DeleteUtxos removes multiple unspent transaction outputs.
	DeleteUtxos([]models.UtxoId, types.Txn) error

	// DeleteUtxosAfterSlot removes all UTxOs created after the given slot.
	DeleteUtxosAfterSlot(uint64, types.Txn) error

	// GetEpochsByEra retrieves all epochs for a given era.
	GetEpochsByEra(uint, types.Txn) ([]models.Epoch, error)

	// GetEpoch retrieves a single epoch by its ID.
	// Returns nil if the epoch is not found.
	GetEpoch(uint64, types.Txn) (*models.Epoch, error)

	// GetEpochs retrieves all epochs.
	GetEpochs(types.Txn) ([]models.Epoch, error)

	// GetEpochBySlot retrieves the epoch containing the given slot.
	// Returns nil if no matching epoch exists.
	GetEpochBySlot(uint64, types.Txn) (*models.Epoch, error)

	// DeleteEpochsAfterSlot removes all epoch entries whose start slot
	// is after the given slot. Used during chain rollback to discard
	// epoch nonces that were computed from rolled-back blocks.
	DeleteEpochsAfterSlot(uint64, types.Txn) error

	// GetUtxosAddedAfterSlot retrieves all UTxOs added after the given slot.
	GetUtxosAddedAfterSlot(uint64, types.Txn) ([]models.Utxo, error)

	// GetLiveUtxosBySlot returns the references ({TxId, OutputIdx}) of all
	// live UTxOs (deleted_slot = 0) created at the given slot. Used by the
	// pruner to materialize block-referenced UTxO bytes before deleting the
	// source block.
	GetLiveUtxosBySlot(uint64, types.Txn) ([]models.UtxoId, error)

	// GetUtxosBySlot returns the references ({TxId, OutputIdx}) of every
	// UTxO created at the given slot, including rows soft-marked as spent
	// (deleted_slot != 0). Used by the pruner in API storage mode to
	// materialize CBOR bytes for retained spent UTxOs before tombstoning
	// the source block, since API mode keeps spent rows past the stability
	// window for historical transaction queries.
	GetUtxosBySlot(uint64, types.Txn) ([]models.UtxoId, error)

	// GetUtxosByAddress retrieves all UTxOs for a given address.
	GetUtxosByAddress(ledger.Address, types.Txn) ([]models.Utxo, error)

	// GetControlledAmountByCredential returns the sum of live UTxO
	// amounts controlled by the given stake credential.
	GetControlledAmountByCredential(uint8, []byte, types.Txn) (uint64, error)

	// GetMidnightCandidates retrieves live committee-candidate UTxOs with
	// inline datum bytes from metadata rows, without materializing block CBOR.
	GetMidnightCandidates(ledger.Address, types.Txn) ([]models.Utxo, error)

	// GetScriptLockedSupply returns the sum of lovelace held in live
	// UTxOs whose payment credential is a script. This is the network's
	// script-locked supply (blockfrost /network supply.locked).
	GetScriptLockedSupply(types.Txn) (uint64, error)

	// GetUtxosByAddressWithOrdering runs q against live UTxOs with ordering metadata.
	// See models.UtxoWithOrderingQuery. q must be non-nil.
	GetUtxosByAddressWithOrdering(
		*models.UtxoWithOrderingQuery,
		types.Txn,
	) ([]models.UtxoWithOrdering, error)

	// GetUtxosByAddressAtSlot retrieves all UTxOs for a given address at a specific slot.
	GetUtxosByAddressAtSlot(
		lcommon.Address,
		uint64,
		types.Txn,
	) ([]models.Utxo, error)

	// GetUtxosByAssets retrieves all UTxOs that contain the specified assets.
	// Pass nil for assetName to match all assets under the policy, or empty []byte{} to match assets with empty names.
	GetUtxosByAssets(
		policyId []byte,
		assetName []byte,
		txn types.Txn,
	) ([]models.Utxo, error)

	// GetUtxosDeletedBeforeSlot retrieves UTxOs deleted before the given slot, up to the specified limit.
	GetUtxosDeletedBeforeSlot(
		uint64,
		int,
		types.Txn,
	) ([]models.Utxo, error)

	// SetUtxoDeletedAtSlot marks a UTxO as deleted at the given slot
	// and records the hash of the transaction that consumed it.
	SetUtxoDeletedAtSlot(
		input ledger.TransactionInput,
		deletedAtSlot uint64,
		spenderHash []byte,
		txn types.Txn,
	) error

	// SetUtxosNotDeletedAfterSlot marks all UTxOs created after the given slot as not deleted.
	SetUtxosNotDeletedAfterSlot(uint64, types.Txn) error

	// IterateLiveUtxos invokes fn once for each live UTxO row
	// (DeletedSlot == 0) in unspecified order. fn receives a
	// pointer to a row that is reused between callbacks — copy
	// out anything you intend to retain. Returning a non-nil
	// error from fn aborts iteration and that error is propagated
	// up. The intended callers iterate, classify, and (optionally)
	// hand a list of UtxoKeys to MarkUtxosDeletedAtSlot;
	// implementations are free to page or stream the underlying
	// query as long as the callback contract is honored.
	IterateLiveUtxos(
		txn types.Txn,
		fn func(*models.Utxo) error,
	) error

	// MarkUtxosDeletedAtSlot marks every live UTxO row matching one
	// of refs as deleted at atSlot. Refs that don't match any live
	// row are silently ignored (the SQL filter is deleted_slot == 0,
	// so already-deleted rows don't get rewritten). Rollback
	// un-deletion is handled by SetUtxosNotDeletedAfterSlot.
	MarkUtxosDeletedAtSlot(
		txn types.Txn,
		refs []types.UtxoKey,
		atSlot uint64,
	) error

	// Reward state methods

	// SaveRewardAdaPots saves reward-related ADA pots for an epoch.
	SaveRewardAdaPots(
		*models.RewardAdaPots,
		types.Txn,
	) error

	// GetRewardAdaPots retrieves reward-related ADA pots for an epoch.
	GetRewardAdaPots(
		uint64, // epoch
		types.Txn,
	) (*models.RewardAdaPots, error)

	// SaveRewardSnapshot saves reward snapshot metadata for an epoch.
	SaveRewardSnapshot(
		*models.RewardSnapshot,
		types.Txn,
	) error

	// GetRewardSnapshot retrieves reward snapshot metadata for an epoch.
	GetRewardSnapshot(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) (*models.RewardSnapshot, error)

	// SaveRewardPoolInputs saves per-pool reward inputs for an epoch.
	SaveRewardPoolInputs(
		[]*models.RewardPoolInput,
		types.Txn,
	) error

	// GetRewardPoolInputs retrieves all per-pool reward inputs for an epoch.
	GetRewardPoolInputs(
		uint64, // epoch
		types.Txn,
	) ([]*models.RewardPoolInput, error)

	// SaveRewardStakeInputs saves per-credential reward snapshot inputs.
	SaveRewardStakeInputs(
		[]*models.RewardStakeInput,
		types.Txn,
	) error

	// GetRewardStakeInputs retrieves all per-credential reward inputs for an epoch.
	GetRewardStakeInputs(
		uint64, // epoch
		types.Txn,
	) ([]*models.RewardStakeInput, error)

	// DeleteRewardInputsForEpoch deletes reward-calculation input rows for an epoch.
	DeleteRewardInputsForEpoch(uint64, types.Txn) error

	// DeleteRewardOutputsForEpoch deletes reward-calculation output rows for an epoch.
	DeleteRewardOutputsForEpoch(uint64, types.Txn) error

	// SaveRewardPoolOutputs saves per-pool reward calculation outputs.
	SaveRewardPoolOutputs(
		[]*models.RewardPoolOutput,
		types.Txn,
	) error

	// GetRewardPoolOutputs retrieves per-pool reward calculation outputs.
	GetRewardPoolOutputs(
		uint64, // epoch
		types.Txn,
	) ([]*models.RewardPoolOutput, error)

	// SaveRewardAccountOutputs saves per-account reward calculation outputs.
	SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput,
		types.Txn,
	) error

	// GetRewardAccountOutputs retrieves per-account reward calculation outputs.
	GetRewardAccountOutputs(
		uint64, // epoch
		types.Txn,
	) ([]*models.RewardAccountOutput, error)

	// DeleteRewardStateAfterSlot deletes reward-state rows captured from
	// rolled-back blocks.
	DeleteRewardStateAfterSlot(uint64, types.Txn) error

	// DeleteRewardStateBeforeEpoch deletes reward-state rows older than the
	// retained snapshot window.
	DeleteRewardStateBeforeEpoch(uint64, types.Txn) error

	// Stake snapshot methods

	// SavePoolStakeSnapshot saves a single pool stake snapshot.
	SavePoolStakeSnapshot(
		*models.PoolStakeSnapshot,
		types.Txn,
	) error

	// SavePoolStakeSnapshots saves multiple pool stake snapshots in batch.
	SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot,
		types.Txn,
	) error

	// GetPoolStakeSnapshot retrieves a specific pool's stake snapshot for an epoch.
	GetPoolStakeSnapshot(
		uint64, // epoch
		string, // snapshotType ("mark", "set", or "go")
		[]byte, // poolKeyHash
		types.Txn,
	) (*models.PoolStakeSnapshot, error)

	// GetPoolStakeSnapshotsByEpoch retrieves all pool stake snapshots for an epoch.
	GetPoolStakeSnapshotsByEpoch(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) ([]*models.PoolStakeSnapshot, error)

	// GetTotalActiveStake returns the sum of all pool stakes for an epoch.
	GetTotalActiveStake(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) (uint64, error)

	// SaveEpochSummary saves an epoch summary.
	SaveEpochSummary(
		*models.EpochSummary,
		types.Txn,
	) error

	// GetEpochSummary retrieves the summary for a specific epoch.
	GetEpochSummary(
		uint64, // epoch
		types.Txn,
	) (*models.EpochSummary, error)

	// GetLatestEpochSummary retrieves the most recent epoch summary.
	GetLatestEpochSummary(types.Txn) (*models.EpochSummary, error)

	// DeletePoolStakeSnapshotsForEpoch deletes snapshots for a specific epoch and type.
	DeletePoolStakeSnapshotsForEpoch(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) error

	// DeletePoolStakeSnapshotsAfterEpoch deletes all snapshots after a given epoch.
	DeletePoolStakeSnapshotsAfterEpoch(uint64, types.Txn) error

	// DeletePoolStakeSnapshotsBeforeEpoch deletes all snapshots before a given epoch.
	DeletePoolStakeSnapshotsBeforeEpoch(uint64, types.Txn) error

	// DeleteEpochSummariesAfterEpoch deletes all epoch summaries after a given epoch.
	DeleteEpochSummariesAfterEpoch(uint64, types.Txn) error

	// DeleteEpochSummariesBeforeEpoch deletes all epoch summaries before a given epoch.
	DeleteEpochSummariesBeforeEpoch(uint64, types.Txn) error

	// GetTransactionHashesAfterSlot returns transaction hashes for transactions added after the given slot.
	// This is used for blob cleanup during rollback/truncation.
	GetTransactionHashesAfterSlot(uint64, types.Txn) ([][]byte, error)

	// DeleteTransactionsAfterSlot removes transaction records added after the given slot.
	// Child records are automatically removed via CASCADE constraints.
	DeleteTransactionsAfterSlot(uint64, types.Txn) error

	// DeleteAddressTransactionsAfterSlot removes address-transaction mappings
	// for transactions added after the given slot.
	DeleteAddressTransactionsAfterSlot(uint64, types.Txn) error

	// DeleteTransactionMetadataLabelsAfterSlot removes transaction metadata
	// label index records added after the given slot.
	DeleteTransactionMetadataLabelsAfterSlot(uint64, types.Txn) error

	// Governance methods

	// GetGovernanceProposal retrieves a governance proposal by transaction hash and action index.
	GetGovernanceProposal(
		[]byte, // txHash
		uint32, // actionIndex
		types.Txn,
	) (*models.GovernanceProposal, error)

	// GetActiveGovernanceProposals retrieves all governance proposals that
	// are still in the active pool (not expired, not enacted, not marked
	// expired, not soft-deleted).
	GetActiveGovernanceProposals(
		uint64, // epoch
		types.Txn,
	) ([]*models.GovernanceProposal, error)

	// GetRatifiedGovernanceProposals returns proposals that have been
	// ratified but not yet enacted. Used at epoch start by enactment.
	GetRatifiedGovernanceProposals(
		types.Txn,
	) ([]*models.GovernanceProposal, error)

	// GetEnactedGovernanceProposalsAt returns proposals that were enacted at
	// the given epoch-boundary slot. Used to replay enactment side effects when
	// stake reward pot reset is reapplied after a boundary commit crash.
	GetEnactedGovernanceProposalsAt(
		epoch uint64,
		slot uint64,
		txn types.Txn,
	) ([]*models.GovernanceProposal, error)

	// GetExpiringGovernanceProposals returns proposals whose
	// `expires_epoch` is strictly less than the given epoch and that
	// have not yet been enacted, expired, or soft-deleted. Used at
	// epoch boundaries to mark expired proposals and return deposits.
	GetExpiringGovernanceProposals(
		epoch uint64,
		txn types.Txn,
	) ([]*models.GovernanceProposal, error)

	// GetExpiredGovernanceProposalsAt returns proposals that were expired at
	// the given epoch-boundary slot. Used to replay deposit-return side effects
	// when stake reward pot reset is reapplied after a boundary commit crash.
	GetExpiredGovernanceProposalsAt(
		epoch uint64,
		slot uint64,
		txn types.Txn,
	) ([]*models.GovernanceProposal, error)

	// GetLastEnactedGovernanceProposal returns the most recently enacted
	// proposal whose action_type is in the given set, or nil if none
	// exist. Callers pass the set of action types that share a chain
	// root per CIP-1694 (e.g., NoConfidence + UpdateCommittee together).
	// Used to resolve governance action chain roots at ratification
	// time.
	GetLastEnactedGovernanceProposal(
		actionTypes []uint8,
		txn types.Txn,
	) (*models.GovernanceProposal, error)

	// SetGovernanceProposal creates or updates a governance proposal.
	SetGovernanceProposal(
		*models.GovernanceProposal,
		types.Txn,
	) error

	// GetChildGovernanceProposals returns all active proposals whose parent
	// is the given proposal (matched by txHash + actionIndex). Only returns
	// proposals not yet enacted, expired, or soft-deleted. Used during
	// epoch boundary orphan sweeps to find dependents of enacted/expired
	// proposals and remove them transitively.
	GetChildGovernanceProposals(
		parentTxHash []byte,
		parentActionIdx uint32,
		txn types.Txn,
	) ([]*models.GovernanceProposal, error)

	// GetGovernanceVotes retrieves all votes for a governance proposal.
	GetGovernanceVotes(
		uint, // proposalID
		types.Txn,
	) ([]*models.GovernanceVote, error)

	// SetGovernanceVote records a vote on a governance proposal.
	SetGovernanceVote(
		*models.GovernanceVote,
		types.Txn,
	) error

	// Committee methods

	// GetCommitteeMember retrieves a committee member by cold key.
	GetCommitteeMember(
		[]byte, // coldKey
		types.Txn,
	) (*models.AuthCommitteeHot, error)

	// GetActiveCommitteeMembers retrieves all active committee members.
	GetActiveCommitteeMembers(types.Txn) ([]*models.AuthCommitteeHot, error)

	// IsCommitteeMemberResigned checks if a committee member has resigned.
	IsCommitteeMemberResigned(
		[]byte, // coldKey
		types.Txn,
	) (bool, error)

	// GetResignedCommitteeMembers returns the cold credentials whose
	// latest resignation is after their latest authorization.
	GetResignedCommitteeMembers(
		[][]byte, // coldKeys
		types.Txn,
	) (map[string]bool, error)

	// GetCommitteeActiveCount returns the number of active (non-resigned)
	// committee members.
	GetCommitteeActiveCount(types.Txn) (int, error)

	// Snapshot-imported committee member methods

	// SetCommitteeMembers upserts committee members imported from a
	// Mithril snapshot. On conflict (same cold_cred_hash), the
	// expires_epoch and added_slot are updated.
	SetCommitteeMembers(
		[]*models.CommitteeMember,
		types.Txn,
	) error

	// SetCommitteeQuorum stores the quorum threshold enacted with a
	// committee update.
	SetCommitteeQuorum(*types.Rat, uint64, types.Txn) error

	// ClearCommitteeQuorum records that the committee has no
	// enacted quorum as of the given slot. Used by NoConfidence
	// enactment so GetCommitteeQuorum falls back to Conway
	// genesis until a subsequent UpdateCommittee sets a new
	// quorum.
	ClearCommitteeQuorum(uint64, types.Txn) error

	// GetCommitteeQuorum retrieves the latest enacted committee quorum.
	// Returns (nil, nil) when no quorum has been enacted or when the
	// most recent record is a ClearCommitteeQuorum marker.
	GetCommitteeQuorum(types.Txn) (*types.Rat, error)

	// GetCommitteeMembers retrieves all active (non-deleted)
	// snapshot-imported committee members.
	GetCommitteeMembers(types.Txn) ([]*models.CommitteeMember, error)

	// GetCommitteeMembersIncludeDeleted retrieves every committee
	// member row, including rows whose deleted_slot is set. Used to
	// distinguish "committee never seated" from "committee voted out
	// via NoConfidence" — the latter leaves every row soft-deleted,
	// which GetCommitteeMembers would hide.
	GetCommitteeMembersIncludeDeleted(
		types.Txn,
	) ([]*models.CommitteeMember, error)

	// DeleteCommitteeMembersAfterSlot removes committee state added
	// after the given slot and clears deleted_slot for any members
	// soft-deleted after that slot. Used during chain rollbacks.
	DeleteCommitteeMembersAfterSlot(uint64, types.Txn) error

	// SoftDeleteCommitteeMembers marks the given cold credential hashes
	// as removed by setting deleted_slot. Used by governance enactment
	// to remove members (UpdateCommittee/NoConfidence action).
	SoftDeleteCommitteeMembers(
		coldCredHashes [][]byte,
		slot uint64,
		txn types.Txn,
	) error

	// SoftDeleteAllCommitteeMembers marks all active committee members as
	// removed. Used by governance enactment for NoConfidence actions.
	SoftDeleteAllCommitteeMembers(
		slot uint64,
		txn types.Txn,
	) error

	// DRep voting power and activity methods

	// InsertDrepIfAbsent inserts a minimal DRep row when no record
	// exists for the given full credential identity (tag + hash). If a
	// row already exists, it is left untouched: added_slot, anchor_url,
	// anchor_hash, and active are never overwritten. Used on the
	// vote-replay recovery path to recreate rows lost during
	// recovery/bootstrap without clobbering real registration metadata.
	InsertDrepIfAbsent(
		credentialTag uint8,
		cred []byte,
		slot uint64,
		url string,
		hash []byte,
		active bool,
		txn types.Txn,
	) error

	// GetDRepVotingPower calculates the voting power for a DRep by summing
	// the current stake of all delegated accounts, approximated from live
	// UTxO balance plus reward-account balance. credentialTag distinguishes
	// key (0) from script (1) DRep credentials that share the same hash.
	GetDRepVotingPower(
		uint8, // credentialTag
		[]byte, // drepCredential
		types.Txn,
	) (uint64, error)

	// GetDRepDelegators returns the stake credentials currently delegating
	// their voting power to the given DRep, in canonical (tag, hash) order.
	// This populates the `delegators` member of the GetDRepState ledger
	// query result. credentialTag distinguishes key (0) from script (1)
	// DRep credentials that share the same 28-byte hash.
	GetDRepDelegators(
		uint8, // credentialTag
		[]byte, // drepCredential
		types.Txn,
	) ([]models.StakeCredentialRef, error)

	// GetDRepVotingPowerBatch is the batch form of GetDRepVotingPower.
	// Returns a StakeCredentialRef.MapKey()-to-power map; credentials with
	// no delegated stake are omitted. Use StakeCredentialRef to carry both
	// the tag and hash so that key-hash and script-hash DReps sharing a
	// 28-byte hash are tallied independently.
	GetDRepVotingPowerBatch(
		drepCredentials []models.StakeCredentialRef,
		txn types.Txn,
	) (map[string]uint64, error)

	// GetDRepVotingPowerByType returns voting power grouped by DRep
	// delegation type. This is used for predefined DRep options such
	// as AlwaysAbstain and AlwaysNoConfidence, which do not have a
	// credential hash.
	GetDRepVotingPowerByType(
		drepTypes []uint64,
		txn types.Txn,
	) (map[uint64]uint64, error)

	// UpdateDRepActivity updates the DRep's last activity epoch and
	// recalculates the expiry epoch. credentialTag distinguishes key (0)
	// from script (1) DRep credentials that share the same 28-byte hash.
	UpdateDRepActivity(
		uint8, // credentialTag
		[]byte, // drepCredential
		uint64, // activityEpoch
		uint64, // inactivityPeriod
		types.Txn,
	) error

	// GetExpiredDReps retrieves all active DReps whose expiry epoch is at
	// or before the given epoch.
	GetExpiredDReps(
		uint64, // epoch
		types.Txn,
	) ([]*models.Drep, error)

	// Constitution methods

	// GetConstitution retrieves the current constitution.
	GetConstitution(types.Txn) (*models.Constitution, error)

	// SetConstitution sets the constitution.
	SetConstitution(
		*models.Constitution,
		types.Txn,
	) error

	// DeleteConstitutionsAfterSlot removes constitutions added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	// This is used during chain rollbacks.
	DeleteConstitutionsAfterSlot(uint64, types.Txn) error

	// Network state methods

	// SetNetworkState stores the treasury and reserves balances.
	SetNetworkState(
		treasury, reserves uint64,
		slot uint64,
		txn types.Txn,
	) error

	// GetNetworkState retrieves the most recent network state.
	GetNetworkState(types.Txn) (*models.NetworkState, error)

	// DeleteNetworkStateAfterSlot removes network state records
	// added after the given slot. This is used during chain
	// rollbacks.
	DeleteNetworkStateAfterSlot(uint64, types.Txn) error

	// Network donation methods

	// AddNetworkDonation records a block's total Conway treasury
	// donation for the given slot and epoch. Idempotent per slot.
	AddNetworkDonation(
		slot, epoch, amount uint64,
		txn types.Txn,
	) error

	// SumNetworkDonationsForEpoch returns the total donation
	// contributed by blocks in the given epoch.
	SumNetworkDonationsForEpoch(epoch uint64, txn types.Txn) (uint64, error)

	// DeleteNetworkDonationsAfterSlot removes donation records added
	// after the given slot. This is used during chain rollbacks.
	DeleteNetworkDonationsAfterSlot(uint64, types.Txn) error

	// Governance rollback methods

	// DeleteGovernanceProposalsAfterSlot removes proposals added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	DeleteGovernanceProposalsAfterSlot(uint64, types.Txn) error

	// DeleteGovernanceVotesAfterSlot removes votes added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	DeleteGovernanceVotesAfterSlot(uint64, types.Txn) error

	// State rollback methods

	// DeleteCertificatesAfterSlot removes all certificate records added after
	// the given slot. This is used during chain rollbacks to undo certificate
	// state changes.
	DeleteCertificatesAfterSlot(uint64, types.Txn) error

	// RestoreAccountStateAtSlot reverts account delegation state to the given
	// slot. For accounts modified after the slot, this restores their Pool and
	// Drep delegations to the state they had at the given slot, or deletes
	// them if they were registered after that slot.
	RestoreAccountStateAtSlot(uint64, types.Txn) error

	// RestorePoolStateAtSlot reverts pool state to the given slot. Pools
	// registered only after the slot are deleted; remaining pools have their
	// denormalized fields restored from the most recent registration at or
	// before the slot.
	RestorePoolStateAtSlot(uint64, types.Txn) error

	// RestoreDrepStateAtSlot reverts DRep state to the given slot. DReps
	// registered only after the slot are deleted; remaining DReps have their
	// anchor and active status restored.
	RestoreDrepStateAtSlot(uint64, types.Txn) error

	// ClearDanglingDRepDelegations implements the cardano-ledger Conway
	// HARDFORK STS rule for protocol major version 10 (Plomin, mainnet
	// January 2025, Cardano/Conway/Rules/HardFork.hs updateDRepDelegations).
	// For each account with a credential-backed DRep delegation
	// (DrepType 0 or 1), if the target DRep credential is not currently
	// registered as an active DRep, clear the delegation. Pseudo-DRep
	// delegations (AlwaysAbstain, AlwaysNoConfidence) are preserved.
	// Updates Account.AddedSlot to atSlot on every row it modifies so the
	// rewritten row is excluded from a subsequent rollback restore
	// targeting any slot before atSlot (the restore filters on
	// `added_slot <= targetSlot` and falls back to prior certificate
	// history). Returns the number of accounts updated.
	ClearDanglingDRepDelegations(atSlot uint64, txn types.Txn) (int, error)

	// DeletePParamsAfterSlot removes protocol parameter records added after
	// the given slot.
	DeletePParamsAfterSlot(uint64, types.Txn) error

	// DeletePParamUpdatesAfterSlot removes protocol parameter update records
	// added after the given slot.
	DeletePParamUpdatesAfterSlot(uint64, types.Txn) error

	// Sync state methods (ephemeral key-value for one-time operations)

	// GetSyncState retrieves a sync state value by key.
	// Returns empty string if the key does not exist.
	GetSyncState(string, types.Txn) (string, error)

	// SetSyncState stores or updates a sync state value.
	SetSyncState(string, string, types.Txn) error

	// DeleteSyncState removes a sync state key.
	DeleteSyncState(string, types.Txn) error

	// ClearSyncState removes all sync state entries.
	ClearSyncState(types.Txn) error

	// Backfill checkpoint methods

	// GetBackfillCheckpoint retrieves a backfill checkpoint by phase.
	// Returns nil (not error) if no checkpoint exists for the phase.
	GetBackfillCheckpoint(
		phase string,
		txn types.Txn,
	) (*models.BackfillCheckpoint, error)

	// SetBackfillCheckpoint creates or updates a backfill checkpoint,
	// upserting on the Phase column.
	SetBackfillCheckpoint(
		checkpoint *models.BackfillCheckpoint,
		txn types.Txn,
	) error

	// DiskSize returns the on-disk size of the metadata store in bytes.
	// Returns 0 for remote databases where local size is not meaningful.
	DiskSize() (int64, error)

	// Midnight indexer methods
	InsertMidnightGovernanceDatum(types.Txn, *models.MidnightGovernanceDatum) error
	DeleteMidnightGovernanceDatumsByBlock(types.Txn, uint64) error
	GetLatestMidnightGovernanceDatum(string, uint64, types.Txn) (*models.MidnightGovernanceDatum, error)
	GetLatestMidnightAriadneParams(types.Txn) (*models.MidnightAriadneParams, error)
	GetMidnightAriadneParamsByEpoch(uint64, types.Txn) (*models.MidnightAriadneParams, error)
	GetMidnightAriadneParamsAtOrBeforeEpoch(uint64, types.Txn) (*models.MidnightAriadneParams, error)
	UpsertMidnightAriadneParams(types.Txn, *models.MidnightAriadneParams) error
	DeleteMidnightAriadneParamsByEpoch(types.Txn, uint64) error
	CreateMidnightAriadneRollback(types.Txn, *models.MidnightAriadneRollback) error
	FindMidnightAriadneRollbacksByBlock(types.Txn, uint64) ([]models.MidnightAriadneRollback, error)
	DeleteMidnightAriadneRollbacksByBlock(types.Txn, uint64) error
	DeleteMidnightAriadneRollbacksBeforeBlock(types.Txn, uint64) error
	UpsertMidnightEpochCandidates(types.Txn, *models.MidnightEpochCandidates) error
	DeleteMidnightEpochCandidatesByBlock(types.Txn, uint64) error
	GetMidnightEpochCandidatesByEpoch(uint64, types.Txn) (*models.MidnightEpochCandidates, error)
	InsertMidnightCommitteeCandidateRegistration(types.Txn, *models.MidnightCommitteeCandidateRegistration) error
	DeleteMidnightCommitteeCandidateRegistrationsByBlock(types.Txn, uint64) error
	GetMidnightCommitteeCandidateRegistrationsByTxHashes(
		[][]byte,
		types.Txn,
	) ([]models.MidnightCommitteeCandidateRegistration, error)
}

// BulkLoadOptimizer is an optional interface that metadata stores can
// implement to provide optimized settings for bulk loading operations.
// The load command checks for this interface and uses it when available.
type BulkLoadOptimizer interface {
	SetBulkLoadPragmas() error
	RestoreNormalPragmas() error
}

// PlannerStatsUpdater is an optional interface for metadata stores that can
// collect query-planner statistics. SQLite runs ANALYZE; other backends no-op.
type PlannerStatsUpdater interface {
	UpdatePlannerStats() error
}

// New creates a new metadata store instance using the specified plugin
func New(pluginName string) (MetadataStore, error) {
	// Get and start the plugin
	p, err := plugin.StartPlugin(plugin.PluginTypeMetadata, pluginName)
	if err != nil {
		return nil, err
	}

	// Type assert to MetadataStore interface
	metadataStore, ok := p.(MetadataStore)
	if !ok {
		return nil, fmt.Errorf(
			"plugin '%s' does not implement MetadataStore interface",
			pluginName,
		)
	}

	return metadataStore, nil
}

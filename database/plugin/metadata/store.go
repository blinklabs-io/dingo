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
	"errors"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrNotFound is the storage-neutral form of sql.ErrNoRows. Store
// implementations should use it internally and translate it to a
// model-specific error when the public method contract requires one.
var ErrNotFound = errors.New("metadata not found")

// LifecycleStore is the narrow lifecycle capability used by composition code.
type LifecycleStore interface {
	// Close closes the metadata store and releases all resources.
	Close() error
}

// SettingsStore owns singleton metadata about database and node state.
type SettingsStore interface {
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
	// currently unset so callers like CheckNodeSettings can perform
	// a one-time network backfill.
	SetNodeSettings(*types.NodeSettings) error

	// GetNodeSettingsGates returns the persisted node settings gate
	// values, keyed by gate name. These are the values enforced on every
	// startup by database/nodesettings.Evaluate; an empty result means no
	// gates have been recorded yet, which is normal before the first
	// successful start.
	GetNodeSettingsGates() (nodesettings.Values, error)

	// SetNodeSettingsGates persists gates, one row per gate, so that a
	// later call overwrites an earlier value for the same name. The
	// recorded epoch and slot are stamped on every row written by this
	// call and are zero when the write happens before the first block
	// has been processed. A nil or empty gates is a no-op.
	SetNodeSettingsGates(
		gates nodesettings.Values,
		recordedEpoch uint64,
		recordedSlot uint64,
	) error

	// InsertNodeSettingsGateIfAbsent persists a single gate only if no row
	// for name exists yet, returning whether this call created it. This is
	// SetNodeSettingsGates's conditional counterpart, used for a gate's
	// first-ever write specifically: two concurrent openers can both reach
	// SettingsStore with no gates persisted yet -- reachable in practice
	// only with a metadata plugin shared across processes by design
	// (postgres, mysql) -- and an unconditional upsert would let whichever
	// one commits last silently overwrite the other's value with no
	// record that a collision happened. A caller that gets inserted=false
	// lost the race and must re-read what is now actually persisted rather
	// than assume its own write took effect.
	InsertNodeSettingsGateIfAbsent(
		name string,
		value string,
		recordedEpoch uint64,
		recordedSlot uint64,
	) (inserted bool, err error)

	// InsertNodeSettingsGatesIfAbsent persists the complete first-fill set in
	// one metadata transaction. It returns false when another initializer
	// already claimed any member of the set; no partial set is committed.
	InsertNodeSettingsGatesIfAbsent(
		gates nodesettings.Values,
		recordedEpoch uint64,
		recordedSlot uint64,
	) (inserted bool, err error)
}

// TxnStore creates backend-owned read and write snapshots. It is named for
// database/types.Txn, the handle it hands out: TransactionStore is the
// chain-transaction domain, which is a different thing entirely.
type TxnStore interface {
	// Transaction creates a new metadata transaction on the write
	// connection pool, bound to ctx. Per database/sql's own BeginTx
	// contract, canceling ctx rolls the transaction back instead of
	// leaving it to a caller's eventual Commit/Rollback -- see the
	// sqlstore implementation's doc comment for exactly which
	// statements that covers today. Use ReadTransaction for read-only
	// access to avoid contending with writers. A nil ctx is treated as
	// context.Background().
	Transaction(ctx context.Context) types.Txn

	// ReadTransaction creates a read-only metadata transaction using
	// the read connection pool (when available), bound to ctx the same
	// way Transaction is. This avoids blocking on the write connection,
	// which is critical for operations like FindIntersect that must
	// complete within protocol timeouts.
	ReadTransaction(ctx context.Context) types.Txn
}

// SlotRangeStats is the canonical block coverage for an inclusive slot range.
// A zero Count means FirstSlot and LastSlot are also zero.
type SlotRangeStats struct {
	Count     int
	FirstSlot uint64
	LastSlot  uint64
}

// SlotRangeStore exposes the small aggregate surface used by API adapters.
// Keeping these queries here avoids leaking a concrete SQL or ORM handle out
// of a metadata provider.
type SlotRangeStore interface {
	CountTransactionsInSlotRange(
		startSlot uint64,
		endSlot uint64,
		txn types.Txn,
	) (int, error)

	GetBlockSlotRangeStats(
		startSlot uint64,
		endSlot uint64,
		txn types.Txn,
	) (SlotRangeStats, error)
}

// GovernanceStore owns the Conway governance surface: proposals and the
// votes cast on them, the constitutional committee, DReps, and the
// constitution. These are the tables a governance component needs and the
// only ones it should be able to reach.
//
// Treasury and reserves (SetNetworkState, the network-donation methods)
// stay on MetadataStore despite sitting among the governance sections
// there: they are ledger economics read by reward calculation, not
// governance state. ImportDrep likewise stays with the snapshot bulk-import
// cluster, and ClearDanglingDRepDelegations mutates the account table
// rather than the drep table.
type GovernanceStore interface {
	// Proposal and vote methods

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
	// expiryEpoch is the CIP-0163 reward-account inactivity gate: 0 excludes
	// no accounts (gate off, byte-identical to the pre-CIP query); >0
	// excludes accounts whose expiration_epoch is nonzero and less than
	// expiryEpoch.
	GetDRepVotingPower(
		uint8, // credentialTag
		[]byte, // drepCredential
		uint64, // expiryEpoch
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
	// 28-byte hash are tallied independently. expiryEpoch is the CIP-0163
	// gate; see GetDRepVotingPower.
	GetDRepVotingPowerBatch(
		drepCredentials []models.StakeCredentialRef,
		expiryEpoch uint64,
		txn types.Txn,
	) (map[string]uint64, error)

	// GetDRepVotingPowerByType returns voting power grouped by DRep
	// delegation type. This is used for predefined DRep options such
	// as AlwaysAbstain and AlwaysNoConfidence, which do not have a
	// credential hash. expiryEpoch is the CIP-0163 gate; see
	// GetDRepVotingPower.
	GetDRepVotingPowerByType(
		drepTypes []uint64,
		expiryEpoch uint64,
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

	// Proposal and vote rollback methods

	// DeleteGovernanceProposalsAfterSlot removes proposals added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	DeleteGovernanceProposalsAfterSlot(uint64, types.Txn) error

	// DeleteGovernanceVotesAfterSlot removes votes added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	DeleteGovernanceVotesAfterSlot(uint64, types.Txn) error

	// DRep registration and state methods

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

	// GetDreps retrieves every DRep row, including deregistered ones,
	// ordered by the credential's first on-chain appearance (earliest
	// registration, update, or delegation reference). Used by the
	// Blockfrost DRep list endpoint.
	GetDreps(types.Txn) ([]models.DrepListRow, error)

	// GetPredefinedDrepFirstSeenSlots returns the earliest delegation
	// added_slot per predefined DRep type (AlwaysAbstain,
	// AlwaysNoConfidence). Types never delegated to are absent.
	GetPredefinedDrepFirstSeenSlots(types.Txn) (map[uint64]uint64, error)

	// GetDrepLastRegistrationSlot returns the added_slot of the most
	// recent registration certificate for the DRep credential, or 0
	// when no registration certificate history exists. Blockfrost's
	// active_epoch reports the most recent registration, which the
	// mutable drep.added_slot cannot provide because update and
	// deregistration certificates overwrite it.
	GetDrepLastRegistrationSlot(
		uint8, // credentialTag
		[]byte, // credential
		types.Txn,
	) (uint64, error)

	// CreateDrep inserts a Drep row directly. Used by callers (e.g.
	// fixture seeding from outside the plugin packages) that already
	// have a fully-populated model and want a single-row insert without
	// the registration-record side effects of ImportDrep.
	CreateDrep(types.Txn, *models.Drep) error

	// RestoreDrepStateAtSlot reverts DRep state to the given slot. DReps
	// registered only after the slot are deleted; remaining DReps have their
	// anchor and active status restored.
	RestoreDrepStateAtSlot(uint64, types.Txn) error
}

// UtxoStore owns the UTxO set: the ledger's live outputs, the spent-output
// history retained for rollback, and the address, credential, and asset
// lookups built over them. It is the whole of sqlstore's utxo.go, so a
// caller holding this interface can reach the utxo table and nothing else.
//
// Several methods here take or return a slot: the UTxO set is versioned by
// slot rather than replaced in place, so a spend marks a row deleted at a
// slot and a rollback un-marks it. Callers that only need the live set
// should prefer the Get/Iterate methods that already filter to it.
type UtxoStore interface {
	// AddUtxos adds one or more unspent transaction outputs to the database.
	AddUtxos(
		[]models.UtxoSlot,
		types.Txn,
	) error

	// ImportUtxos inserts UTxOs in bulk, ignoring duplicates.
	ImportUtxos([]models.Utxo, types.Txn) error

	// CreateUtxo inserts a Utxo row directly. The normal block-
	// application path uses AddUtxos with UtxoSlot inputs; this is
	// the simple-insert variant for callers that already have a
	// populated model.
	CreateUtxo(types.Txn, *models.Utxo) error

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

	// GetUtxosByRefs retrieves multiple live unspent transaction outputs
	// by their (transaction ID, index) references in a single batch.
	// Refs with no matching live UTxO are simply absent from the result.
	GetUtxosByRefs(
		[]models.UtxoId, // refs
		types.Txn,
	) ([]models.Utxo, error)

	// DeleteUtxo removes a single unspent transaction output.
	DeleteUtxo(models.UtxoId, types.Txn) error

	// DeleteUtxos removes multiple unspent transaction outputs.
	DeleteUtxos([]models.UtxoId, types.Txn) error

	// DeleteUtxosAfterSlot removes all UTxOs created after the given slot.
	DeleteUtxosAfterSlot(uint64, types.Txn) error

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

	// GetUtxosByAddress retrieves coarse SQL candidates matching any of the
	// given address patterns (OR-joined, mirroring
	// GetUtxosByAddressWithOrdering). The database layer performs full
	// exact-address CBOR filtering when ExactAddress is set. An empty
	// patterns slice returns (nil, nil), matching the coordinated
	// Database.UtxosByAddress's empty-input handling.
	GetUtxosByAddress(
		[]models.UtxoAddressPattern,
		types.Txn,
	) ([]models.Utxo, error)

	// GetControlledAmountByCredential returns the sum of live UTxO
	// amounts controlled by the given stake credential.
	GetControlledAmountByCredential(uint8, []byte, types.Txn) (uint64, error)

	// GetUtxoPaymentScriptByCredential returns, for the given bounded set
	// of payment-key hashes previously observed under a stake credential,
	// whether each payment credential is a script hash (true) or a key
	// hash (false). Used by the Blockfrost account transactions endpoint
	// to reconstruct the exact address type for one page of (payment
	// address, transaction) rows without decoding UTxO CBOR or scanning
	// the credential's full history: paymentKeys is expected to be the
	// small (<= page size) distinct set drawn from an already-paginated
	// GetAddressTransactionsByCredential page. A payment key with no
	// matching UTxO row is omitted from the result; callers should
	// default to key-hash for any omitted key.
	GetUtxoPaymentScriptByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		[][]byte, // paymentKeys
		types.Txn,
	) (map[string]bool, error)

	// GetScriptLockedSupply returns the sum of lovelace held in live
	// UTxOs whose payment credential is a script. This is the network's
	// script-locked supply (blockfrost /network supply.locked).
	GetScriptLockedSupply(types.Txn) (uint64, error)

	// GetUtxoBalanceByAddress returns the live-UTxO lovelace balance,
	// per-asset balances (ordered by policy id then name), and live UTxO
	// count for the given address, aggregated in SQL. Payment-credential
	// mode aggregates across address forms. Exact mode returns
	// models.ErrExactAddressRequiresCbor; exact summaries use the coordinated
	// Database query path instead.
	GetUtxoBalanceByAddress(
		lcommon.Address,
		models.UtxoAddressMatchMode,
		types.Txn,
	) (models.AddressBalance, error)

	// GetUtxosByAddressWithOrdering runs q against live UTxOs with ordering
	// metadata. Snapshot-imported UTxOs without a producing transaction use
	// AddedSlot and block index zero. Keyset ordering uses the unique tuple
	// (slot, block_index, output_idx, tx_id). See
	// models.UtxoWithOrderingQuery. q must be non-nil.
	GetUtxosByAddressWithOrdering(
		*models.UtxoWithOrderingQuery,
		types.Txn,
	) ([]models.UtxoWithOrdering, error)

	// CountUtxosByAddressWithOrdering returns the number of live UTxOs
	// matching q's coarse SQL predicate, without materializing rows. It
	// errors if q's address patterns require CBOR-based exact-address
	// filtering (see models.RequiresExactAddressFilter), since the coarse
	// predicate alone would over-count. See models.UtxoWithOrderingQuery.
	CountUtxosByAddressWithOrdering(
		*models.UtxoWithOrderingQuery,
		types.Txn,
	) (int, error)

	// GetUtxosByAddressAtSlot retrieves all UTxOs for a given address at a specific slot.
	GetUtxosByAddressAtSlot(
		models.UtxoAddressPattern,
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
}

// TransactionStore owns chain transactions: the transaction table, the
// per-transaction metadata it carries, and the address and metadata-label
// indexes derived from it.
//
// This is the chain-transaction domain, not TxnStore -- which creates the
// database transactions (database/types.Txn) that every method here takes
// as its final argument. The two names are close because the underlying
// concepts are, and mixing them up is the mistake this comment exists to
// prevent.
//
// CountTransactionsInSlotRange and GetBlockSlotRangeStats read the same
// tables but live on SlotRangeStore, which extracted them earlier for the
// API adapters.
type TransactionStore interface {
	// SumTransactionFeesInSlotRange sums the fee-pot contributions in the
	// inclusive slot range: declared fees of valid transactions plus
	// consumed collateral of phase-2-invalid transactions.
	SumTransactionFeesInSlotRange(
		uint64, // startSlot
		uint64, // endSlot
		types.Txn,
	) (uint64, error)

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

	// CountTransactionsByPaymentCred returns the total number of
	// transactions involving the provided payment credential across every
	// address that carries it, regardless of staking part. Used by the
	// Blockfrost payment-credential (addr_vkh/script) address lookups.
	CountTransactionsByPaymentCred(
		[]byte, // paymentKey
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

	// GetAddressTransactionsByCredential retrieves one page of (payment
	// address, transaction) association rows for a stake credential
	// tag/hash pair, ordered by (slot, tx_index, payment_key) and
	// optionally bounded by an inclusive from/to (slot, tx_index) range
	// (nil = unconstrained on that side). This is the direct SQL page: no
	// caller-side fan-out or filtering is needed, so cost is bounded by
	// limit/offset rather than by the credential's full transaction
	// history.
	GetAddressTransactionsByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		int, // limit
		int, // offset
		string, // order (asc|desc)
		*models.AddressTransactionPosition, // from
		*models.AddressTransactionPosition, // to
		types.Txn,
	) ([]models.AccountTransactionAssociationRow, error)

	// CountAddressTransactionsByCredential retrieves the total count of
	// (payment address, transaction) association rows for a stake
	// credential tag/hash pair within the same optional from/to range.
	CountAddressTransactionsByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		*models.AddressTransactionPosition, // from
		*models.AddressTransactionPosition, // to
		types.Txn,
	) (int, error)

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

	// SetTransaction stores a transaction with its metadata.
	SetTransaction(
		lcommon.Transaction,
		ocommon.Point,
		uint32, // idx
		map[int]uint64, // certDeposits: indexed by certificate position in tx.Certificates(); absent keys are treated as zero/no deposit
		bool, // skipWithdrawalWitness: elide the CIP-0163 account_withdrawal_witness insert (see BatchedTxIngestOpts.SkipWithdrawalWitnessWrite)
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
		bool, // skipWithdrawalWitness: see SetTransaction
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
}

// EpochStore owns the epoch table: the per-epoch records that map slots to
// epochs and eras, and the rollback delete over them.
//
// Unlike the other domain interfaces this one is defined by its table
// rather than by an implementation file. sqlstore's operational.go holds
// these alongside tip, block nonces, datums, scripts, protocol parameters,
// network state, and sync state, so taking that file wholesale would
// produce something that is not a domain at all. Those other groups are
// their own future extractions.
type EpochStore interface {
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
}

// StakeSnapshotStore owns the stake snapshots taken at epoch boundaries:
// per-pool snapshot rows, the epoch summaries computed from them, and the
// historical per-boundary stake used to reconstruct an earlier epoch's
// view.
//
// Live stake is not here. sqlstore's live_stake.go rebuilds
// reward_live_stake from the current utxo, account, and certificate tables
// to feed reward calculation; its subject is what the stake is now, not
// what a boundary recorded it as, and it migrates with the reward domain.
type StakeSnapshotStore interface {
	// GetStakeByPoolsAtSlot returns delegated stake for multiple pools at a
	// historical slot. It uses certificate history plus slot-aware UTxO
	// liveness so epoch-boundary stake snapshots do not read current live
	// stake for an older boundary. expiryEpoch and inactivityPeriod drive the
	// CIP-0163 reward-account inactivity gate: 0 disables it (result
	// byte-identical to pre-CIP); otherwise expiration is reconstructed from
	// witness history at slot and credentials expired before expiryEpoch are
	// excluded.
	GetStakeByPoolsAtSlot(
		[][]byte, // poolKeyHashes
		uint64, // slot
		uint64, // expiryEpoch (0 = gate off)
		uint64, // inactivityPeriod
		types.Txn,
	) (map[string]uint64, map[string]uint64, error)

	// GetEpochBoundaryStakeByPools is GetStakeByPoolsAtSlot with epoch-boundary
	// (SNAP) reward semantics: reward credits recorded at boundarySlot (which is
	// snapshotSlot+1) are retained unless they are marked
	// AccountRewardDelta.PostSnapshot. That reproduces what the authoritative
	// SNAP-point capture observes — the delayed reward update applied at the
	// boundary, and none of the POOLREAP/MIR/enactment credits that follow it.
	// Only the epoch-boundary mark-snapshot fallback may use this; use
	// GetStakeByPoolsAtSlot for a plain "stake at slot" query.
	GetEpochBoundaryStakeByPools(
		[][]byte, // poolKeyHashes
		uint64, // snapshotSlot
		uint64, // boundarySlot
		uint64, // expiryEpoch (0 = gate off)
		uint64, // inactivityPeriod
		types.Txn,
	) (map[string]uint64, map[string]uint64, error)

	// GetEpochBoundaryRewardStakeInputsForPools returns the positive
	// per-credential reward basis for the same epoch boundary as
	// GetEpochBoundaryStakeByPools, aggregated from the identical CTE. Pairing
	// the two keeps the reward basis and the leader-election pool totals in exact
	// agreement — same credential set, same slot-accurate values — regardless of
	// the CIP-0163 gate, instead of mixing a historical leader total with a live
	// reward aggregate.
	GetEpochBoundaryRewardStakeInputsForPools(
		[][]byte, // poolKeyHashes
		uint64, // snapshotSlot
		uint64, // boundarySlot
		uint64, // expiryEpoch (0 = gate off)
		uint64, // inactivityPeriod
		types.Txn,
	) ([]*models.RewardStakeInput, error)

	// GetPoolOwnerStakeAtSlot returns historical stake for the requested pool
	// owner key hashes, keyed by pool plus credential. An owner is included only
	// when that credential was delegated to the pool at the requested slot.
	// expiryEpoch drives the CIP-0163 inactivity gate (0 = gate off), and
	// inactivityPeriod reconstructs expiration from historical witnesses.
	GetPoolOwnerStakeAtSlot(
		[][]byte, // ownerKeyHashes
		uint64, // slot
		uint64, // expiryEpoch (0 = gate off)
		uint64, // inactivityPeriod
		types.Txn,
	) (map[string]uint64, error)

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

	// GetPoolStakeSnapshotsForPools retrieves the snapshot rows for just the
	// pools named, for a caller wanting a bounded subset rather than a whole
	// epoch. A pool the snapshot has no row for is absent from the result.
	// The read is chunked rather than issued once per pool named.
	GetPoolStakeSnapshotsForPools(
		uint64, // epoch
		string, // snapshotType
		[][]byte, // poolKeyHashes
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

	// DeleteEpochSummariesAfterEpoch deletes all epoch summaries after a given
	// epoch, for discarding boundaries that a rollback rewound. Rollback does
	// not currently call it: epoch numbering is slot-derived, so the boundary is
	// re-crossed on the selected chain and SaveEpochSummary upserts the row.
	//
	// There is deliberately no before-epoch counterpart. epoch_summary is one
	// small row per epoch and is retained for the life of the database, unlike
	// the per-pool snapshot and reward-input rows that cleanupOldSnapshots
	// prunes to the rotation window.
	DeleteEpochSummariesAfterEpoch(uint64, types.Txn) error
}

// CertificateStore owns on-chain certificates: the certs table and its
// per-certificate-type detail tables, move-instantaneous-rewards
// certificates, genesis delegations, and the rollback delete across them.
//
// The per-credential history readers here are certificate readers despite
// their Account names -- each joins certs to a stake_* certificate table.
// Their neighbours in sqlstore's account_history.go that read
// account_reward_delta or the account witness tables are account and
// reward state, not certificates, and stay on MetadataStore.
type CertificateStore interface {
	// GetMIRCertsInSlotRange returns the processed effects of all MIR
	// certificates whose added_slot is >= startSlot and < endSlot. Used to
	// apply the Shelley-era INSTANT rule at each epoch boundary.
	GetMIRCertsInSlotRange(
		startSlot, endSlot uint64,
		txn types.Txn,
	) ([]models.MIREffect, error)

	// GetStakeRegistrationsByCredential retrieves stake registration certificates
	// using the full credential identity: credential tag plus 28-byte hash.
	GetStakeRegistrationsByCredential(
		uint8, // credentialTag
		[]byte, // stakeKey
		types.Txn,
	) ([]lcommon.StakeRegistrationCertificate, error)

	// GetGenesisDelegationForSlot returns the latest genesis-key delegation
	// certificate for genesisHash before the supplied block slot.
	GetGenesisDelegationForSlot(
		[]byte, // genesisHash
		uint64, // blockSlot
		types.Txn,
	) (*models.GenesisDelegation, error)

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

	// DeleteCertificatesAfterSlot removes all certificate records added after
	// the given slot. This is used during chain rollbacks to undo certificate
	// state changes.
	DeleteCertificatesAfterSlot(uint64, types.Txn) error
}

// MetadataStore composes every capability for callers that predate the
// split. New components should depend on the smallest domain interface they
// consume instead; this composition exists so that adding one does not break
// the callers and backends that still take the whole surface.
//
// What remains declared here is what has not been extracted yet: accounts,
// pools, rewards and live stake, protocol parameters, block nonces, datums
// and scripts, assets, treasury/reserves and donations, Midnight indexer
// state, sync state, and backfill checkpoints. Each is a future domain on
// the same terms as the ones above -- drawn to an sqlstore implementation
// file where one exists, composed back in here, and with its callers moved
// to the narrow interface in the same change.
type MetadataStore interface {
	LifecycleStore
	SettingsStore
	TxnStore
	CertificateStore
	EpochStore
	GovernanceStore
	StakeSnapshotStore
	TransactionStore
	UtxoStore

	// Bulk import methods (ledger state restore from snapshot)

	// ImportAccount upserts an account (insert or update delegation
	// fields on conflict).
	ImportAccount(*models.Account, types.Txn) error
	// GetAccountImportRegistrationByCredential returns the virtual
	// registration captured by an imported account baseline.
	GetAccountImportRegistrationByCredential(
		uint8,
		[]byte,
		types.Txn,
	) (*models.AccountImportRegistration, error)

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

	// CreateAccount inserts an Account row directly. See CreateDrep
	// for the rationale; this is the simple-insert sibling of
	// ImportAccount.
	CreateAccount(types.Txn, *models.Account) error

	// RenewAccountExpirations sets expiration_epoch for every existing
	// account row matching one of refs (CIP-0163 delegator-inactivity
	// mechanism). A ref with no matching account row is ignored: an
	// account must already be registered to have an expiration. Callers
	// handling an ordinary witness pass currentEpoch + delegatorInactivity,
	// which moves expiration forward. Rollback recomputation may instead lower
	// expiration or reset it to zero to restore the value at the rollback point.
	RenewAccountExpirations(
		refs []models.StakeCredentialRef,
		expirationEpoch uint64,
		txn types.Txn,
	) error

	// AccountLastWitnessSlots returns, per requested credential, the greatest
	// witnessing added_slot <= maxSlot across the stake-witnessing certificate
	// tables and reward-withdrawal history. Withdrawal history includes
	// account_withdrawal_witness (which preserves zero-amount withdrawals) and
	// legacy account_reward_delta rows where withdrawal = TRUE — together with
	// the certificate tables, this is the CIP-0163 witness set. The result is
	// keyed by StakeCredentialRef.MapKey(); a credential with no witness <=
	// maxSlot is absent from the map. Used by the ledger's rollback expiration
	// recomputation to find each affected account's surviving witness slot.
	AccountLastWitnessSlots(
		refs []models.StakeCredentialRef,
		maxSlot uint64,
		txn types.Txn,
	) (map[string]uint64, error)

	// AccountsWitnessedAfterSlot returns the distinct reward-account
	// credentials with a stake-witnessing certificate OR a reward withdrawal
	// at added_slot > slot — the CIP-0163 rollback affected set. Withdrawals
	// come from account_withdrawal_witness (including zero-amount withdrawals)
	// and legacy account_reward_delta withdrawal rows. Callers must invoke it
	// before deleting rolled-back certificate, withdrawal-witness, and
	// reward-delta rows, since those are exactly the rows it inspects.
	AccountsWitnessedAfterSlot(
		slot uint64,
		txn types.Txn,
	) ([]models.StakeCredentialRef, error)

	// StampAllActiveAccountExpirations sets expiration_epoch = expirationEpoch
	// for every active account. Used once at CIP-0163 activation to give every
	// pre-existing account a full inactivity window from the activation epoch,
	// including accounts witnessed before activation. Returns the number of
	// rows stamped.
	StampAllActiveAccountExpirations(
		expirationEpoch uint64,
		txn types.Txn,
	) (int64, error)

	// AccountInactivityActivationMembership returns the requested credentials
	// included in the one-time activation stamp, keyed by
	// StakeCredentialRef.MapKey().
	AccountInactivityActivationMembership(
		[]models.StakeCredentialRef,
		types.Txn,
	) (map[string]struct{}, error)

	// ResetAccountExpirationActivation clears expiration for the exact durable
	// activation-membership set, deletes that set, and returns its credentials
	// so the ledger can reconstruct any pre-activation witness expiration. Used
	// when rollback crosses back before the activation boundary.
	ResetAccountExpirationActivation(
		types.Txn,
	) ([]models.StakeCredentialRef, error)

	// CreateMidnightAssetCreate inserts a cNIGHT UTxO creation row.
	CreateMidnightAssetCreate(types.Txn, *models.MidnightAssetCreate) error

	// CreateMidnightAssetSpend inserts a cNIGHT UTxO spend row.
	CreateMidnightAssetSpend(types.Txn, *models.MidnightAssetSpend) error

	// CreateMidnightRegistration inserts a mapping-validator registration row.
	CreateMidnightRegistration(types.Txn, *models.MidnightRegistration) error

	// CreateMidnightDeregistration inserts a mapping-validator deregistration row.
	CreateMidnightDeregistration(
		types.Txn,
		*models.MidnightDeregistration,
	) error

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
	DeleteMidnightAssetCreatesByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAssetCreate, error)

	// DeleteMidnightAssetSpendsByBlock removes all cNIGHT spend rows for the
	// given block number and returns them so the caller can restore the
	// in-memory tracked-UTxO set. Used during chain rollback.
	DeleteMidnightAssetSpendsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAssetSpend, error)

	// DeleteMidnightRegistrationsByBlock removes all registration rows for
	// the given block number and returns them so the caller can update the
	// in-memory tracked-UTxO set. Used during chain rollback.
	DeleteMidnightRegistrationsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightRegistration, error)

	// DeleteMidnightDeregistrationsByBlock removes all deregistration rows
	// for the given block number and returns them so the caller can restore
	// the in-memory tracked-UTxO set. Used during chain rollback.
	DeleteMidnightDeregistrationsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightDeregistration, error)

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

	// GetOffchainMetadataBatch retrieves cached off-chain documents for
	// many URLs of the given source type in a single query, rather than
	// one GetOffchainMetadata call per item. Used by callers that need
	// per-item off-chain metadata for a whole page of results (for
	// example, pool metadata for /pools/extended): the unique index on
	// (source_type, url, hash) covers source_type + url IN (...) as its
	// leading columns, so this is index-backed the same way
	// GetOffchainMetadata is. Because two documents can share a URL under
	// different hashes (metadata republished at the same URL with new
	// content), callers must still match each returned row against their
	// own (url, hash) pointer rather than assuming one row per URL.
	GetOffchainMetadataBatch(
		sourceType string,
		urls []string,
		txn types.Txn,
	) ([]models.OffchainMetadata, error)

	// Token registry methods

	// UpsertTokenRegistryEntries writes CIP-26 off-chain token registry
	// properties keyed by subject (hex policy ID followed by the
	// hex-encoded asset name) and returns the number of rows written.
	// Each entry replaces every property of an existing row for the same
	// subject, so a property the upstream registry has dropped stops
	// being served rather than surviving from an earlier sync. Written
	// only by the API-mode token registry sync.
	//
	// syncedAt stamps every written row with the timestamp of the
	// snapshot being applied, so PruneTokenRegistryEntriesBefore can
	// afterwards find rows that snapshot did not carry. Every batch of
	// one snapshot must pass the same value.
	UpsertTokenRegistryEntries(
		ctx context.Context,
		entries []models.TokenRegistryEntry,
		syncedAt time.Time,
		txn types.Txn,
	) (int, error)

	// PruneTokenRegistryEntriesBefore deletes registry rows last
	// confirmed by a snapshot older than cutoff and returns the number
	// removed. Callers pass the same timestamp they gave
	// UpsertTokenRegistryEntries for the snapshot just applied, so that
	// snapshot's rows survive and everything it did not carry is
	// removed. This is what retires a subject the upstream registry has
	// dropped. Run only after a snapshot has fully applied.
	PruneTokenRegistryEntriesBefore(
		ctx context.Context,
		cutoff time.Time,
		txn types.Txn,
	) (int, error)

	// GetTokenRegistryEntry returns the registry properties for a
	// subject, or nil when the registry has nothing for it. Backs the
	// `metadata` field of GET /assets/{asset}; an unknown subject is
	// absence rather than an error and yields a null field.
	GetTokenRegistryEntry(
		subject string,
		txn types.Txn,
	) (*models.TokenRegistryEntry, error)

	// GetRetiringPools returns pools whose latest retirement
	// certificate targets an epoch after currentEpoch and has not been
	// cancelled by a later registration certificate. Certificate
	// recency compares (added_slot, synthetic-import precedence,
	// block_index, cert_index). Results are ordered by retirement
	// epoch, then announcement position, matching Blockfrost.
	GetRetiringPools(
		currentEpoch uint64,
		txn types.Txn,
	) ([]models.PoolRetiringRow, error)

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

	// LatestPoolOpCertSequenceAfter returns the highest observed op-cert
	// sequence for a pool strictly after the given slot.
	LatestPoolOpCertSequenceAfter(
		lcommon.PoolKeyHash,
		uint64, // afterSlot
		types.Txn,
	) (uint64, bool, error)

	// LatestPoolOpCertSequenceAtOrBefore returns the highest op-cert sequence
	// observed for a pool no later than the supplied canonical-chain slot.
	// This is the chain-dependent counter view at a historical point; pools
	// with no issuer row by that point are absent rather than counter zero.
	LatestPoolOpCertSequenceAtOrBefore(
		lcommon.PoolKeyHash,
		uint64, // slot
		types.Txn,
	) (uint64, bool, error)

	// LatestPoolOpCertSequences returns the highest observed op-cert sequence
	// for every pool that has issued a block, keyed by pool key hash. Pools
	// that have never issued one are absent rather than reported as zero.
	//
	// The set is not restricted to currently registered pools: the chain's
	// accepted issue number for a cold key survives the pool leaving the
	// active set, and is still enforced against any block claiming that key.
	LatestPoolOpCertSequences(
		types.Txn,
	) (map[string]uint64, error)

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

	// GetActivePoolKeyHashesOrdered retrieves the key hashes of all
	// currently active pools (same active-pool semantics as
	// GetActivePoolKeyHashes), ordered oldest-first by each pool's
	// earliest on-chain registration certificate: added_slot ascending,
	// then block_index and cert_index ascending to disambiguate
	// certificates recorded in the same slot. This backs the Blockfrost
	// pool_list endpoint's documented "oldest first, newest last"
	// ordering. See poolorder.GetActivePoolKeyHashesOrdered for the full
	// rationale, including why "oldest" is keyed on first registration
	// rather than the most recent one.
	GetActivePoolKeyHashesOrdered(types.Txn) ([][]byte, error)

	// GetPoolCertificateHistory returns the transaction hashes of a pool's
	// registration and retirement certificates, in chronological order
	// (added_slot, block_index, cert_index ascending). Certificates with no
	// linked transaction — rows synthesized by the Mithril ledger-state
	// import, which carry certificate_id = 0 — are excluded since they have
	// no originating transaction to report.
	GetPoolCertificateHistory(
		lcommon.PoolKeyHash,
		types.Txn,
	) (registrationTxHashes [][]byte, retirementTxHashes [][]byte, err error)

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

	// GetLiveStakeInputsForPools returns every registered credential (including
	// zero-stake credentials) from the transactionally maintained live reward
	// aggregate for the requested pools. expiryEpoch applies the live
	// CIP-0163 account-expiration filter when nonzero.
	GetLiveStakeInputsForPools(
		[][]byte, // poolKeyHashes
		uint64, // expiryEpoch (0 = gate off)
		types.Txn,
	) ([]*models.RewardStakeInput, error)

	// RebuildRewardLiveStake rebuilds the live reward stake aggregate from
	// canonical account and live UTxO metadata. Node startup uses it as an
	// upgrade/repair backstop when RewardLiveStakeNeedsBackfill reports gaps.
	RebuildRewardLiveStake(uint64, types.Txn) error

	// RewardLiveStakeNeedsBackfill reports whether the reward_live_stake
	// aggregate needs a RebuildRewardLiveStake pass. It compares calculation
	// versions, credentials, stake values, registration, and delegation state
	// with canonical account and live-UTxO metadata.
	RewardLiveStakeNeedsBackfill(types.Txn) (bool, error)

	// StaleConsensusStakeSnapshotsExist reports whether persisted Mark/Set/Go
	// stake snapshots or authoritative Mark metadata use an older calculation
	// version. Such snapshots cannot safely be recreated from a pruned database.
	StaleConsensusStakeSnapshotsExist(types.Txn) (bool, error)

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
	// validated transaction withdrawal and records rollback state. txHash must
	// identify the withdrawing transaction; callers that pass nil cannot safely
	// apply more than one hashless withdrawal for the same credential.
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

	// AddPostSnapshotAccountRewardByCredential is AddAccountRewardByCredential
	// for a boundary credit that cardano-ledger applies after the epoch-boundary
	// stake snapshot (SNAP): POOLREAP deposit refunds, enacted treasury
	// withdrawals and proposal-deposit refunds. It is identical except that the
	// journal row is stamped AccountRewardDelta.PostSnapshot, which is what lets
	// the epoch-boundary stake reconstruction exclude these credits from a mark
	// snapshot while retaining the pre-SNAP credits — the delayed reward update
	// and MIR — recorded at the same boundary slot.
	AddPostSnapshotAccountRewardByCredential(
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

	// GetLatestBlockNonce returns the block_nonce row with the highest slot.
	// block_nonce is written in the same metadata transaction as a block's
	// UTxO/certificate effects and the ledger tip, so the maximum slot is the
	// authoritative high-water mark of durably applied ledger state. The bool
	// is false (with a zero row and nil error) when the table is empty.
	GetLatestBlockNonce(
		txn types.Txn,
	) (models.BlockNonce, bool, error)

	// GetDatum retrieves a datum by its hash, returning nil if not found.
	GetDatum(
		lcommon.Blake2b256,
		types.Txn,
	) (*models.Datum, error)

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

	// GetAccountWithdrawalHistoryByCredential retrieves withdrawal history
	// rows for a stake credential tag/hash pair.
	GetAccountWithdrawalHistoryByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		int, // limit
		int, // offset
		string, // order (asc|desc)
		types.Txn,
	) ([]models.AccountWithdrawalHistoryRow, error)

	// CountAccountWithdrawalHistoryByCredential retrieves the total count of
	// withdrawal history rows for a stake credential tag/hash pair.
	CountAccountWithdrawalHistoryByCredential(
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

	// SetGenesisStaking stores genesis pool registrations and stake
	// delegations from the shelley-genesis.json staking section.
	// pools maps pool key hash (hex) to its registration certificate.
	// stakeDelegations maps staking credential hash (hex) to pool key hash (hex).
	SetGenesisStaking(
		pools map[string]lcommon.PoolRegistrationCertificate,
		stakeDelegations map[string]string,
		keyDeposit uint64,
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

	// GetMidnightCandidates retrieves live committee-candidate UTxOs with
	// inline datum bytes from metadata rows, without materializing block CBOR.
	GetMidnightCandidates(ledger.Address, types.Txn) ([]models.Utxo, error)

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

	// SaveRewardSnapshot saves reward snapshot metadata for an epoch,
	// overwriting any existing row for the (epoch, snapshot_type) pair
	// (including its authoritative flag). Used by the authoritative
	// epoch-rollover capture, which must always win over a fallback row.
	SaveRewardSnapshot(
		*models.RewardSnapshot,
		types.Txn,
	) error

	// ClaimFallbackRewardSnapshot atomically reserves the (epoch, snapshot_type)
	// reward snapshot marker for a fallback (non-authoritative) capture,
	// returning false when an authoritative snapshot already occupies it so the
	// caller abandons the fallback rather than overwriting the authoritative
	// row. See rewardstate.ClaimFallbackSnapshot.
	ClaimFallbackRewardSnapshot(
		*models.RewardSnapshot,
		types.Txn,
	) (bool, error)

	// ClaimFallbackRewardSnapshotGuard serializes a fallback capture that cannot
	// persist reward inputs against the authoritative epoch-rollover capture.
	// It returns proceed=false when an authoritative row already exists. A
	// non-zero guard ID identifies a temporary row that the caller must delete
	// in the same transaction before commit.
	ClaimFallbackRewardSnapshotGuard(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) (bool, uint, error)

	// ReleaseFallbackRewardSnapshotGuard removes a temporary guard row returned
	// by ClaimFallbackRewardSnapshotGuard. It must run in the same transaction
	// that claimed the guard.
	ReleaseFallbackRewardSnapshotGuard(
		uint, // guardID
		types.Txn,
	) error

	// GetRewardSnapshot retrieves reward snapshot metadata for an epoch.
	GetRewardSnapshot(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) (*models.RewardSnapshot, error)

	// DeleteProvisionalRewardSnapshot deletes a non-authoritative reward
	// snapshot for an epoch and type. Authoritative boundary state is retained.
	DeleteProvisionalRewardSnapshot(uint64, string, types.Txn) error

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
	SaveRewardStakeInputs([]*models.RewardStakeInput, types.Txn) error

	// GetRewardStakeInputs retrieves all per-credential reward inputs for an epoch.
	GetRewardStakeInputs(uint64, types.Txn) ([]*models.RewardStakeInput, error)

	// DeleteRewardInputsForEpoch deletes reward-calculation input rows for an epoch.
	DeleteRewardInputsForEpoch(uint64, types.Txn) error

	// DeleteRewardOutputsForEpoch deletes reward-calculation output rows for an epoch.
	DeleteRewardOutputsForEpoch(uint64, types.Txn) error

	// SaveRewardPoolOutputs saves per-pool reward calculation outputs.
	SaveRewardPoolOutputs([]*models.RewardPoolOutput, types.Txn) error

	// GetRewardPoolOutputs retrieves per-pool reward calculation outputs.
	GetRewardPoolOutputs(uint64, types.Txn) ([]*models.RewardPoolOutput, error)

	// SaveRewardAccountOutputs saves per-account reward calculation outputs.
	SaveRewardAccountOutputs([]*models.RewardAccountOutput, types.Txn) error

	// GetRewardAccountOutputs retrieves per-account reward calculation outputs.
	GetRewardAccountOutputs(
		uint64,
		types.Txn,
	) ([]*models.RewardAccountOutput, error)

	// GetRewardAccountOutputsByCredential retrieves reward account output
	// rows for a stake credential tag/hash pair across every epoch that has
	// not yet been pruned, paginated and ordered by epoch. Used by the
	// Blockfrost account reward-history endpoint.
	//
	// Only credited rows (spendable = true and guarded = false) are returned.
	// Either excluded state records a reward that never reached the account:
	// deregistration routes non-spendable value to the unspendable total,
	// while CIP-0163 expiry leaves guarded value undistributed.
	GetRewardAccountOutputsByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		int, // limit
		int, // offset
		string, // order (asc|desc)
		types.Txn,
	) ([]*models.RewardAccountOutput, error)

	// CountRewardAccountOutputsByCredential retrieves the total count of
	// reward account output rows for a stake credential tag/hash pair.
	//
	// Counts only spendable, unguarded rows, matching
	// GetRewardAccountOutputsByCredential's filter. The two must agree, or
	// pagination advertises pages of rewards that were never paid.
	CountRewardAccountOutputsByCredential(
		uint8, // credentialTag
		[]byte, // stakingKey
		types.Txn,
	) (int, error)

	// DeleteRewardStateAfterSlot deletes reward-state rows captured from
	// rolled-back blocks.
	DeleteRewardStateAfterSlot(uint64, types.Txn) error

	// DeleteRewardStateBeforeEpoch deletes reward-state rows older than the
	// retained snapshot window. This is the CORE storage-mode pruning path:
	// it deletes both reward_stake_input and reward_account_output. See
	// rewardstate.DeleteStateBeforeEpoch for the full rationale.
	DeleteRewardStateBeforeEpoch(uint64, types.Txn) error

	// DeleteRewardStakeInputBeforeEpoch deletes only reward_stake_input rows
	// older than the retained snapshot window, leaving reward_account_output
	// intact. This is the API storage-mode pruning path, used so the
	// Blockfrost account reward-history endpoint can serve an account's full
	// reward history. See rewardstate.DeleteStakeInputBeforeEpoch.
	DeleteRewardStakeInputBeforeEpoch(uint64, types.Txn) error

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

	// State rollback methods

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
	InsertMidnightGovernanceDatum(
		types.Txn,
		*models.MidnightGovernanceDatum,
	) error
	DeleteMidnightGovernanceDatumsByBlock(types.Txn, uint64) error
	GetLatestMidnightGovernanceDatum(
		string,
		uint64,
		types.Txn,
	) (*models.MidnightGovernanceDatum, error)
	GetLatestMidnightAriadneParams(
		types.Txn,
	) (*models.MidnightAriadneParams, error)
	GetMidnightAriadneParamsByEpoch(
		uint64,
		types.Txn,
	) (*models.MidnightAriadneParams, error)
	GetMidnightAriadneParamsAtOrBeforeEpoch(
		uint64,
		types.Txn,
	) (*models.MidnightAriadneParams, error)
	UpsertMidnightAriadneParams(types.Txn, *models.MidnightAriadneParams) error
	DeleteMidnightAriadneParamsByEpoch(types.Txn, uint64) error
	CreateMidnightAriadneRollback(
		types.Txn,
		*models.MidnightAriadneRollback,
	) error
	FindMidnightAriadneRollbacksByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAriadneRollback, error)
	DeleteMidnightAriadneRollbacksByBlock(types.Txn, uint64) error
	DeleteMidnightAriadneRollbacksBeforeBlock(types.Txn, uint64) error
	UpsertMidnightEpochCandidates(
		types.Txn,
		*models.MidnightEpochCandidates,
	) error
	DeleteMidnightEpochCandidatesByBlock(types.Txn, uint64) error
	GetMidnightEpochCandidatesByEpoch(
		uint64,
		types.Txn,
	) (*models.MidnightEpochCandidates, error)
	InsertMidnightCommitteeCandidateRegistration(
		types.Txn,
		*models.MidnightCommitteeCandidateRegistration,
	) error
	DeleteMidnightCommitteeCandidateRegistrationsByBlock(
		types.Txn,
		uint64,
	) error
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

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

package ledgerdb

import (
	"context"
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

// UtxoStore resolves and maintains the live UTxO set consensus reads.
type UtxoStore interface {
	// UtxoRollbackChanges reports what a rollback to slot does to the live
	// UTxO set, read before the rollback runs: the live outputs created after
	// slot, which it removes, and the outputs created at or before slot but
	// spent after it, which it restores, with their CBOR.
	UtxoRollbackChanges(
		ctx context.Context,
		slot uint64,
		txn Txn,
	) ([]models.UtxoId, []models.Utxo, error)
	// ConsumedUtxoPruneFloor returns the highest slot the consumed-UTxO sweep
	// has hard-deleted spent rows at or below, or 0 when nothing has been
	// swept. Rolling back below this slot cannot restore the UTxOs consumed
	// above it.
	//
	// It fails closed: a read or parse failure is returned rather than reported
	// as "nothing swept", because callers use it to refuse a destructive
	// rollback and a floor that cannot be verified must not read as absent.
	ConsumedUtxoPruneFloor(txn Txn) (uint64, error)
	// CountUtxosConsumedUpTo counts, up to limit, the spent UTxO rows that
	// UtxosDeleteConsumed with the same slot would remove. It reads at most
	// limit rows, so a result equal to limit means at least that many.
	CountUtxosConsumedUpTo(
		ctx context.Context,
		slot uint64,
		limit int,
	) (int, error)
	// GetLiveUtxoLovelace returns the total lovelace of every live
	// (unspent) UTxO.
	GetLiveUtxoLovelace(txn Txn) (uint64, error)
	// IterateLiveUtxoRefs invokes fn once for each live UTxO row (DeletedSlot
	// == 0), like IterateLiveUtxos, but does not resolve u.Cbor -- it is left
	// as the store's raw stored value (a CborOffset reference, not the
	// referenced output CBOR). For a caller that needs every live UTxO's CBOR
	// and wants to resolve it itself (e.g. across a worker pool, rather than
	// serially via IterateLiveUtxos' inline loadCbor -- see
	// ledger.queryShelleyUtxoWhole). As with IterateLiveUtxos, the callback
	// receives a pointer to a row whose underlying buffer is reused between
	// callbacks -- copy out anything (e.g. u.TxId) you intend to retain past
	// the current call. When txn is nil a read transaction is opened
	// internally.
	IterateLiveUtxoRefs(
		ctx context.Context,
		txn Txn,
		fn func(*models.Utxo) error,
	) error
	// IterateLiveUtxos invokes fn once for each live UTxO row
	// (DeletedSlot == 0). The callback receives a pointer to a row whose
	// Cbor field has been populated from blob storage (or recovered from
	// the producing block) — copy out anything you intend to retain
	// because the underlying buffer is reused between callbacks.
	// Returning a non-nil error from fn aborts iteration and that error
	// is propagated up; CBOR-loading failures are also propagated.
	// When txn is nil a read transaction is opened internally.
	IterateLiveUtxos(
		ctx context.Context,
		txn Txn,
		fn func(*models.Utxo) error,
	) error
	// IterateUtxoRefsAsOf is IterateLiveUtxoRefs' historical counterpart: it
	// invokes fn once for each UTxO row live as of atSlot rather than live
	// right now, and likewise does not resolve u.Cbor (see
	// ledger.queryShelleyUtxoWhole, its only caller as of this writing, which
	// resolves CBOR itself across a worker pool). As with IterateLiveUtxoRefs,
	// the callback receives a pointer to a row whose underlying buffer is
	// reused between callbacks -- copy out anything you intend to retain past
	// the current call. When txn is nil a read transaction is opened
	// internally.
	//
	// Callers pinning atSlot should check checkUtxoRetentionWindow (or the
	// equivalent guarantee for their own use) first: a slot older than this
	// node's spent-UTxO retention floor cannot be answered correctly by this
	// query, since the rows it would need may already be pruned -- see
	// IterateUtxosAsOf's doc comment on the MetadataStore interface.
	IterateUtxoRefsAsOf(
		ctx context.Context,
		atSlot uint64,
		txn Txn,
		fn func(*models.Utxo) error,
	) error
	// IterateUtxoRefsAsOfPaged invokes fn once for each UTxO live as of atSlot,
	// like IterateUtxoRefsAsOf, but reads pageSize rows per short read
	// transaction instead of holding one for the whole walk. Over SQLite a
	// transaction held for a whole-UTxO walk -- minutes on a large chain --
	// keeps the WAL from restarting, and one walk after another keeps it
	// growing.
	//
	// The pages agree with one another without sharing a snapshot because of
	// how UTxO rows change. Blocks applied after atSlot add rows with a later
	// added_slot and spend rows at a later slot, and the as-of predicate
	// already excludes the first and includes the second. Spent-row pruning
	// removes only rows spent before the prune floor, which an acquired-point
	// pin holds at or below atSlot. Rows keep their id, so paging on it neither
	// repeats nor skips a row. The one change that alters the as-of set is a
	// rollback below atSlot, which noteUtxoRollback reports: this returns
	// ErrUtxoHistoryRolledBack and the caller decides whether to retry. Callers
	// must hold pruning at or below atSlot themselves.
	IterateUtxoRefsAsOfPaged(
		ctx context.Context,
		atSlot uint64,
		pageSize int,
		fn func(database.UtxoRef) error,
	) error
	// MarkUtxosDeletedAtSlot marks every live UTxO row matching one of
	// refs as deleted at atSlot. Refs that don't match any live row are
	// silently ignored; rollback un-deletion is handled by the existing
	// rollback path (SetUtxosNotDeletedAfterSlot). When txn is nil a
	// write transaction is opened, committed on success and rolled back
	// on error via Txn.Do.
	MarkUtxosDeletedAtSlot(
		ctx context.Context,
		txn Txn,
		refs []types.UtxoKey,
		atSlot uint64,
	) error
	// UtxoByRef returns the unspent output txId#outputIdx, or
	// database.ErrUtxoNotFound when it does not exist or is spent.
	UtxoByRef(
		ctx context.Context,
		txId []byte,
		outputIdx uint32,
		txn Txn,
	) (*models.Utxo, error)
	// UtxoByRefIncludingSpent returns a Utxo by reference,
	// including spent (consumed) UTxOs.
	UtxoByRefIncludingSpent(
		ctx context.Context,
		txId []byte,
		outputIdx uint32,
		txn Txn,
	) (*models.Utxo, error)
	// UtxoExists reports whether a live UTxO is recorded for the reference,
	// without materializing its CBOR.
	//
	// UtxoByRef resolves the output's bytes from the blob store and, on a miss,
	// reconstructs them by decoding the producing block. A caller that only
	// needs to know whether the output is still there pays for all of that, and
	// — worse — turns a CBOR that cannot be recovered into a hard error about a
	// UTxO that demonstrably exists. Replay recovery asks exactly that question
	// of every referenced input of a failing transaction (see
	// LedgerState.findReplayRecoveryCandidate), so it uses this instead.
	UtxoExists(
		ctx context.Context,
		txId []byte,
		outputIdx uint32,
		txn Txn,
	) (bool, error)
	// UtxosByAddress returns all UTxOs belonging to any of the given addresses.
	// maxResults is an optional bound on the number of candidate rows the
	// query may materialize; zero or negative means unbounded. Exceeding a
	// positive bound returns models.ErrTooManyUtxoResults.
	UtxosByAddress(
		ctx context.Context,
		addrs []ledger.Address,
		maxResults int,
		txn Txn,
	) ([]models.Utxo, error)
	// UtxosByAddressAsOf is UtxosByAddress as the outputs stood at atSlot (see
	// the metadata store's GetUtxosByAddressAsOf). Rows spent at or before
	// atSlot and later removed by consumed-UTxO cleanup are missing, so callers
	// must reject a point below their retention floor first.
	UtxosByAddressAsOf(
		ctx context.Context,
		addrs []ledger.Address,
		atSlot uint64,
		maxResults int,
		txn Txn,
	) ([]models.Utxo, error)
	// UtxosByRefs returns the live UTxOs matching the given references in a
	// single batch. Refs with no matching live UTxO are simply absent from the
	// result.
	UtxosByRefs(
		ctx context.Context,
		refs []models.UtxoId,
		txn Txn,
	) ([]models.Utxo, error)
	// UtxosByRefsAsOf returns the UTxOs matching refs as they stood at atSlot:
	// a ref is included when it was created at-or-before atSlot and is either
	// still live or was spent strictly after atSlot. As with UtxosByRefs, a
	// ref with no matching row is simply absent from the result -- but unlike
	// UtxosByRefs, that absence is ambiguous once atSlot is older than this
	// node's spent-UTxO retention floor: it could mean "genuinely never live
	// at atSlot" or "was live at atSlot but its spend record has since been
	// hard-deleted by the periodic stability-window cleanup"
	// (UtxosDeleteConsumed). Callers pinning a historical point (ledger.Query,
	// node-parity) must reject that case themselves before
	// calling this -- see ledger's checkUtxoRetentionWindow.
	UtxosByRefsAsOf(
		ctx context.Context,
		refs []models.UtxoId,
		atSlot uint64,
		txn Txn,
	) ([]models.Utxo, error)
	// UtxosByRefsBounded is UtxosByRefs with a cap on the CBOR it will load.
	// The CBOR size of an output is only known once it has been read, so it is
	// counted as each output is loaded and the call stops with
	// ErrUtxoBytesExceeded as soon as the running total passes maxBytes,
	// holding at most maxBytes plus the output that crossed it. The first
	// output always fits, so a limit smaller than one output still makes
	// progress. ctx is checked before each output is loaded. A maxBytes of zero
	// or less means no limit.
	UtxosByRefsBounded(
		ctx context.Context,
		refs []models.UtxoId,
		maxBytes int64,
		txn Txn,
	) ([]models.Utxo, error)
	// UtxosDeleteConsumed hard-deletes up to limit outputs consumed at or
	// before slot and returns how many it removed.
	UtxosDeleteConsumed(
		ctx context.Context,
		slot uint64,
		limit int,
		txn Txn,
	) (int, error)
	// UtxosDeleteSpentInBatch removes the rows of outputs that one trusted
	// batch both created and spent, inside the transaction that applied it, so
	// they are never committed. Every id must name a row the same transaction
	// has already marked spent. slot is the last slot the transaction applied;
	// the consumed-UTxO prune floor is raised to it, as UtxosDeleteConsumed
	// does, because a rollback below it could not restore the removed rows.
	UtxosDeleteSpentInBatch(ids []models.UtxoId, slot uint64, txn Txn) error
	// UtxosDropSpentInBatchBlobs is UtxosDeleteSpentInBatch for a batch whose
	// metadata rows a batch UTxO overlay already handled: it deletes the blob
	// offset keys of ids and raises the consumed-UTxO prune floor to slot the
	// same way, but deletes no metadata row.
	UtxosDropSpentInBatchBlobs(ids []models.UtxoId, slot uint64, txn Txn) error
}

// AccountStore holds stake accounts, registrations, delegations and deposits.
type AccountStore interface {
	// GetAccountsActiveAtSlot returns the subset of stakeCredentials that
	// were registered and not subsequently deregistered at or before slot,
	// keyed by StakeCredentialRef.MapKey().
	GetAccountsActiveAtSlot(
		stakeCredentials []models.StakeCredentialRef,
		slot uint64,
		txn Txn,
	) (map[string]struct{}, error)
	// AccountInactivityActivationMembership returns the requested credentials
	// that were included in the one-time CIP-0163 activation stamp.
	AccountInactivityActivationMembership(
		ctx context.Context,
		refs []models.StakeCredentialRef,
		txn Txn,
	) (map[string]struct{}, error)
	// AccountLastWitnessSlots returns, per requested credential, the greatest
	// CIP-0163 witnessing slot <= maxSlot across the stake-witnessing
	// certificate tables and the reward-withdrawal history, keyed by
	// StakeCredentialRef.MapKey(). A credential with no witness <= maxSlot is
	// absent from the map. When txn is nil a read transaction is opened for the
	// query; pass an existing txn to read within a wider unit of work.
	AccountLastWitnessSlots(
		ctx context.Context,
		refs []models.StakeCredentialRef,
		maxSlot uint64,
		txn Txn,
	) (map[string]uint64, error)
	// AccountsWitnessedAfterSlot returns the distinct reward-account
	// credentials witnessed (via a stake-witnessing certificate or a reward
	// withdrawal) at a slot greater than the given slot — the CIP-0163 rollback
	// affected set. It must be called before rolled-back certificate and
	// reward-delta rows are deleted. When txn is nil a read transaction is
	// opened for the query; pass an existing txn to read within a wider unit of
	// work.
	AccountsWitnessedAfterSlot(
		ctx context.Context,
		slot uint64,
		txn Txn,
	) ([]models.StakeCredentialRef, error)
	// GetAccountByCredential returns an account by staking credential tag and
	// key.
	GetAccountByCredential(
		ctx context.Context,
		credentialTag uint8,
		stakeKey []byte,
		includeInactive bool,
		txn Txn,
	) (*models.Account, error)
	// GetAccountImportRegistrationByCredential returns the virtual registration
	// stored with a snapshot-imported account baseline.
	GetAccountImportRegistrationByCredential(
		ctx context.Context,
		credentialTag uint8,
		stakeKey []byte,
		txn Txn,
	) (*models.AccountImportRegistration, error)
	// GetAccountRegistrationHistoryByCredential returns registration history
	// rows for a stake credential.
	GetAccountRegistrationHistoryByCredential(
		ctx context.Context,
		credentialTag uint8,
		stakeKey []byte,
		limit int,
		offset int,
		order string,
		txn Txn,
	) ([]models.AccountRegistrationHistoryRow, error)
	// GetAccountsByCredential returns accounts for the given staking
	// credentials in a single query, keyed by StakeCredentialRef.MapKey().
	GetAccountsByCredential(
		ctx context.Context,
		refs []models.StakeCredentialRef,
		includeInactive bool,
		txn Txn,
	) (map[string]*models.Account, error)
	// GetAccountsByCredentialAtSlot returns the given staking credentials'
	// accounts as they stood at slot, keyed by StakeCredentialRef.MapKey().
	// Only accounts registered at slot are returned.
	GetAccountsByCredentialAtSlot(
		ctx context.Context,
		refs []models.StakeCredentialRef,
		slot uint64,
		txn Txn,
	) (map[string]*models.Account, error)
	// GetActiveAccountCredentials returns the stake credentials (tag + key) of
	// every currently active account. Used by Mithril v2 catch-up
	// reconciliation to find accounts absent from a newer snapshot's live set.
	GetActiveAccountCredentials(txn Txn) ([]models.StakeCredentialRef, error)
	// GetGenesisDelegationForSlot returns the latest genesis-key delegation
	// certificate for genesisHash that has taken effect by blockSlot: one
	// whose certificate slot plus stabilityWindow is at or below it.
	GetGenesisDelegationForSlot(
		genesisHash []byte,
		blockSlot uint64,
		stabilityWindow uint64,
		txn Txn,
	) (*models.GenesisDelegation, error)
	// GetGenesisDelegationsInSlotRange returns the genesis-key delegation
	// certificates with a certificate slot from fromSlot through uptoSlot
	// inclusive, oldest first.
	GetGenesisDelegationsInSlotRange(
		fromSlot uint64,
		uptoSlot uint64,
		txn Txn,
	) ([]models.GenesisDelegation, error)
	// GetLatestAccountRegistrationAtOrBefore returns the newest registration
	// history row for a stake credential at or before slot, or nil when there
	// is none. The import baseline is not consulted.
	GetLatestAccountRegistrationAtOrBefore(
		ctx context.Context,
		credentialTag uint8,
		stakeKey []byte,
		slot uint64,
		txn Txn,
	) (*models.AccountRegistrationHistoryRow, error)
	// GetStakeCredentialsWithRegistrationEvents returns the credentials with
	// a registration or deregistration certificate in the inclusive slot
	// range.
	GetStakeCredentialsWithRegistrationEvents(
		fromSlot uint64,
		toSlot uint64,
		txn Txn,
	) ([]models.StakeCredentialRef, error)
	// GetStakeRegistrationsByCredential returns stake registration certificates
	// for the full stake credential identity.
	GetStakeRegistrationsByCredential(
		credentialTag uint8,
		stakingKey []byte,
		txn Txn,
	) ([]lcommon.StakeRegistrationCertificate, error)
	// RenewAccountExpirations sets the CIP-0163 expirationEpoch for the given
	// reward-account credentials. See the metadata store implementation for
	// semantics (refs with no matching account row are ignored). When txn is
	// nil a write transaction is opened, committed on success and rolled back
	// on error via Txn.Do; pass an existing write txn to participate in a
	// wider unit of work.
	RenewAccountExpirations(
		ctx context.Context,
		refs []models.StakeCredentialRef,
		expirationEpoch uint64,
		txn Txn,
	) error
	// SetGenesisStakingWithDeposits stores genesis staking with the key and
	// pool deposits that the reference ledger retains for later refunds.
	SetGenesisStakingWithDeposits(
		pools map[string]lcommon.PoolRegistrationCertificate,
		stakeDelegations map[string]string,
		keyDeposit uint64,
		poolDeposit uint64,
		blockHash []byte,
		txn Txn,
	) error
	// StampAllActiveAccountExpirations sets the CIP-0163 expirationEpoch for
	// every active account row. Used once at activation to give every
	// pre-existing account a full inactivity window from the activation epoch,
	// including accounts witnessed before activation. Returns the number of
	// rows stamped. When txn is nil a write transaction is opened, committed on
	// success and rolled back on error via Txn.Do; pass an existing write txn
	// to participate in a wider unit of work.
	StampAllActiveAccountExpirations(
		ctx context.Context,
		expirationEpoch uint64,
		txn Txn,
	) (int64, error)
}

// RewardStore holds reward calculation inputs, outputs, pots and credit rounds.
type RewardStore interface {
	// RefreshRewardLiveStakeRefs recomputes the live reward aggregate of
	// each credential in refs, recording slot as its update. With a live
	// UTxO source, a rollback calls it for the credentials of the outputs
	// the rollback changes, which the metadata store cannot find itself.
	RefreshRewardLiveStakeRefs(
		refs []models.StakeCredentialRef,
		slot uint64,
		txn Txn,
	) error
	// MergeImportedPoolBlockCounts combines the per-pool block counts
	// observed for epoch from epochStartSlot with those imported from a
	// ledger-state snapshot, returning the merged counts and epoch total. The
	// bool is false when the epoch is at or below the import anchor and the
	// snapshot carried no counts for it.
	MergeImportedPoolBlockCounts(
		epoch uint64,
		epochStartSlot uint64,
		counts map[string]uint64,
		totalBlocks uint64,
		txn Txn,
	) (map[string]uint64, uint64, bool, error)
	// AddAccountRewardByCredential credits the reward balance for a registered
	// account identified by stake credential tag and key. sourceHash uniquely
	// identifies the credit event (refunded proposal identity hash, reaped pool
	// key hash, or synthetic MIR event discriminator); it makes each
	// epoch-boundary credit a distinct rollback-aware journal row while letting
	// a crash-replayed boundary map onto the existing row and skip
	// idempotently. Pass nil when no per-event discriminator is available.
	AddAccountRewardByCredential(
		ctx context.Context,
		credentialTag uint8,
		stakeKey []byte,
		amount uint64,
		slot uint64,
		sourceHash []byte,
		txn Txn,
	) error
	// AddAccountRewardsByCredential applies a batch of reward credits with the
	// effect of one AddAccountRewardByCredential call per credit; see
	// metadata.MetadataStore.AddAccountRewardsByCredential.
	AddAccountRewardsByCredential(
		ctx context.Context,
		credits []models.AccountRewardCredit,
		txn Txn,
	) error
	// AddAppliedRewardCreditRound registers an applied reward round in the
	// caller's transaction.
	AddAppliedRewardCreditRound(round models.RewardCreditRound, txn Txn) error
	// AddPostSnapshotAccountRewardByCredential credits a reward account with an
	// epoch-boundary credit that cardano-ledger applies AFTER the boundary
	// stake snapshot (SNAP): POOLREAP deposit refunds, enacted treasury
	// withdrawals and governance proposal-deposit refunds. The journal row is
	// stamped AccountRewardDelta.PostSnapshot so the epoch-boundary stake
	// reconstruction can exclude it while still retaining the pre-SNAP credits,
	// which land at the same boundary slot. Use AddAccountRewardByCredential
	// for those — the delayed reward update and MIR — and for
	// transaction-driven credits.
	AddPostSnapshotAccountRewardByCredential(
		ctx context.Context,
		credentialTag uint8,
		stakeKey []byte,
		amount uint64,
		slot uint64,
		sourceHash []byte,
		txn Txn,
	) error
	// ClaimFallbackRewardSnapshot atomically reserves the (epoch,
	// snapshot_type) reward snapshot marker for a fallback (non-authoritative)
	// capture, returning false when an authoritative snapshot already occupies
	// it so the caller abandons the fallback rather than overwriting the
	// authoritative row. See rewardstate.ClaimFallbackSnapshot.
	ClaimFallbackRewardSnapshot(
		snapshot *models.RewardSnapshot,
		txn Txn,
	) (bool, error)
	// ClaimFallbackRewardSnapshotGuard serializes a fallback capture that
	// cannot persist reward inputs against the authoritative epoch-rollover
	// capture. It returns proceed=false when an authoritative row already
	// exists. A non-zero guard ID identifies a temporary row that the caller
	// must delete in the same transaction before commit.
	ClaimFallbackRewardSnapshotGuard(
		epoch uint64,
		snapshotType string,
		txn Txn,
	) (bool, uint, error)
	// ClaimPendingRewardCreditsForCredential marks every unfolded credit of
	// one stake credential in the credited rounds folded and returns them,
	// for the caller to write to the account in the same transaction.
	ClaimPendingRewardCreditsForCredential(
		credentialTag uint8,
		stakingKey []byte,
		txn Txn,
	) ([]*models.RewardAccountOutput, error)
	// ClaimUnfoldedRewardCredits marks up to limit unfolded credits of one
	// credited round folded and returns them, for the caller to write to
	// their accounts in the same transaction, or to acknowledge a certified
	// imported state that already covers those credits.
	ClaimUnfoldedRewardCredits(
		snapshotEpoch uint64,
		limit int,
		txn Txn,
	) ([]*models.RewardAccountOutput, error)
	// DeleteRewardInputsForEpoch deletes reward-calculation input rows for an
	// epoch.
	DeleteRewardInputsForEpoch(epoch uint64, txn Txn) error
	// DeleteRewardOutputsForEpoch deletes reward-calculation output rows for an
	// epoch.
	DeleteRewardOutputsForEpoch(epoch uint64, txn Txn) error
	// DeleteRewardStakeInputBeforeEpoch deletes only reward_stake_input rows
	// older than the retained snapshot window, leaving reward_account_output
	// intact. This is the Koios-parity pruning path, used so the observer
	// can read reward_account_output behind the tip. API storage mode prunes
	// neither table. See rewardstate.DeleteStakeInputBeforeEpoch.
	DeleteRewardStakeInputBeforeEpoch(
		ctx context.Context,
		epoch uint64,
		txn Txn,
	) error
	// DeleteRewardStateBeforeEpoch deletes reward-state rows older than the
	// retained snapshot window. This is the CORE storage-mode pruning path:
	// it deletes both reward_stake_input and reward_account_output. See
	// rewardstate.DeleteStateBeforeEpoch for the full rationale.
	DeleteRewardStateBeforeEpoch(
		ctx context.Context,
		epoch uint64,
		txn Txn,
	) error
	// GetEpochBoundaryRewardStakeInputsForPools returns the positive
	// per-credential reward basis for the same epoch boundary as
	// GetEpochBoundaryStakeByPools, aggregated from the identical CTE. Pairing
	// the two keeps the reward basis and the leader-election pool totals in
	// exact agreement — same credential set, same slot-accurate values —
	// regardless of the CIP-0163 gate, instead of mixing a historical leader
	// total with a live reward aggregate.
	GetEpochBoundaryRewardStakeInputsForPools(
		poolKeyHashes [][]byte,
		snapshotSlot uint64,
		boundarySlot uint64,
		incomingEraID *uint,
		expiryEpoch uint64,
		inactivityPeriod uint64,
		txn Txn,
	) ([]*models.RewardStakeInput, error)
	// GetMIRCertsInSlotRange returns the processed effects of all MIR
	// certificates whose added_slot is >= startSlot and < endSlot. Used to
	// apply the Shelley-era INSTANT rule at each epoch boundary.
	GetMIRCertsInSlotRange(
		ctx context.Context,
		startSlot uint64,
		endSlot uint64,
		txn Txn,
	) ([]models.MIREffect, error)
	// GetPendingRewardAccountOutputsForCredential returns unfolded outputs in
	// applied reward rounds for one credential.
	GetPendingRewardAccountOutputsForCredential(
		credentialTag uint8,
		stakingKey []byte,
		txn Txn,
	) ([]*models.RewardAccountOutput, error)
	// GetPendingRewardCreditRounds returns applied reward rounds retained until
	// rollback crosses their boundary. Their outputs may already be folded.
	GetPendingRewardCreditRounds(txn Txn) ([]models.RewardCreditRound, error)
	// GetPostSnapshotRewardCredits returns each stake credential's total of
	// the credits recorded at slot and marked AccountRewardDelta.PostSnapshot:
	// what a boundary at slot credited after its SNAP point.
	GetPostSnapshotRewardCredits(
		slot uint64,
		txn Txn,
	) ([]*models.AccountRewardDelta, error)
	// GetRewardAccountOutputs retrieves per-account reward calculation outputs.
	GetRewardAccountOutputs(
		epoch uint64,
		txn Txn,
	) ([]*models.RewardAccountOutput, error)
	// GetRewardAccountOutputsForEligibility returns one credential's unfolded
	// outputs in the given snapshot epochs, including outputs currently marked
	// nonspendable so the boundary can recheck registration changes.
	GetRewardAccountOutputsForEligibility(
		epochs []uint64,
		credentialTag uint8,
		stakingKey []byte,
		txn Txn,
	) ([]*models.RewardAccountOutput, error)
	// GetRewardAdaPots retrieves reward-related ADA pots for an epoch.
	GetRewardAdaPots(epoch uint64, txn Txn) (*models.RewardAdaPots, error)
	// GetRewardPoolInputs retrieves all per-pool reward inputs for an epoch.
	GetRewardPoolInputs(
		epoch uint64,
		txn Txn,
	) ([]*models.RewardPoolInput, error)
	// GetRewardPoolOutputs retrieves per-pool reward calculation outputs.
	GetRewardPoolOutputs(
		epoch uint64,
		txn Txn,
	) ([]*models.RewardPoolOutput, error)
	// GetRewardSeedFailure returns the durable imported reward-basis failure,
	// or an empty string when no failure was recorded.
	GetRewardSeedFailure(
		epoch uint64,
		snapshotType string,
		txn Txn,
	) (string, error)
	// GetRewardSnapshot retrieves reward snapshot metadata for an epoch.
	GetRewardSnapshot(
		epoch uint64,
		snapshotType string,
		txn Txn,
	) (*models.RewardSnapshot, error)
	// GetRewardStakeInputs retrieves all per-credential reward inputs for an
	// epoch.
	GetRewardStakeInputs(
		epoch uint64,
		txn Txn,
	) ([]*models.RewardStakeInput, error)
	// GetRewardStakeInputsInPoolKeyHashRange retrieves per-credential reward
	// inputs for an epoch whose pool_key_hash falls in the inclusive [lo, hi]
	// range, so a caller can process a contiguous batch of pools' delegators
	// at a time instead of loading every pool's stake inputs at once.
	GetRewardStakeInputsInPoolKeyHashRange(
		epoch uint64,
		poolKeyHashLo []byte,
		poolKeyHashHi []byte,
		txn Txn,
	) ([]*models.RewardStakeInput, error)
	// HasAppliedRewardCreditRound reports whether the snapshot epoch has been
	// applied, regardless of whether its output rows have been folded.
	HasAppliedRewardCreditRound(epoch uint64, txn Txn) (bool, error)
	// HasPendingRewardCreditRounds reports whether any applied round has
	// spendable, unguarded account outputs not yet folded into account
	// balances.
	HasPendingRewardCreditRounds(txn Txn) (bool, error)
	// HasUnfoldedRewardCreditsThroughEpoch reports whether any applied round
	// through snapshotEpoch has spendable, unguarded outputs not yet folded.
	HasUnfoldedRewardCreditsThroughEpoch(
		snapshotEpoch uint64,
		txn Txn,
	) (bool, error)
	// ReleaseFallbackRewardSnapshotGuard removes a temporary guard row returned
	// by ClaimFallbackRewardSnapshotGuard. It must run in the same transaction
	// that claimed the guard.
	ReleaseFallbackRewardSnapshotGuard(guardID uint, txn Txn) error
	// SaveRewardAccountOutputs saves per-account reward calculation outputs.
	SaveRewardAccountOutputs(
		outputs []*models.RewardAccountOutput,
		txn Txn,
	) error
	// SaveRewardAdaPots saves reward-related ADA pots for an epoch.
	SaveRewardAdaPots(pots *models.RewardAdaPots, txn Txn) error
	// SaveRewardPoolInputs saves per-pool reward inputs for an epoch.
	SaveRewardPoolInputs(inputs []*models.RewardPoolInput, txn Txn) error
	// SaveRewardPoolOutputs saves per-pool reward calculation outputs.
	SaveRewardPoolOutputs(outputs []*models.RewardPoolOutput, txn Txn) error
	// SaveRewardSnapshot saves reward snapshot metadata for an epoch,
	// overwriting any existing row for the (epoch, snapshot_type) pair
	// (including its authoritative flag). Used by the authoritative
	// epoch-rollover capture, which must always win over a fallback row.
	SaveRewardSnapshot(snapshot *models.RewardSnapshot, txn Txn) error
	// SaveRewardStakeInputs saves per-credential reward snapshot inputs.
	SaveRewardStakeInputs(inputs []*models.RewardStakeInput, txn Txn) error
	// TakeRewardEligibilityRecheck returns and clears the credentials whose
	// registration a rollback restored since the last call.
	TakeRewardEligibilityRecheck(txn Txn) ([]models.StakeCredentialRef, error)
}

// PoolStore holds stake pool registrations, retirements, VRF keys and
// operational-certificate counters.
type PoolStore interface {
	// ClearDelegationsToRetiredPool removes every account delegation pointing
	// at the given reaped pool, stamping added_slot with the epoch-boundary
	// slot so the clear is rollback-safe. The delegation half of POOLREAP; see
	// the sqlstore implementation for why the import baseline is left alone.
	ClearDelegationsToRetiredPool(
		poolKeyHash []byte,
		boundarySlot uint64,
		txn Txn,
	) error
	// CountPoolBlocksInSlotRange counts observed pool-issued blocks in the
	// inclusive slot range, grouped by pool key hash. The total return value
	// counts all observed pool blocks in the range, not only the requested
	// pools.
	CountPoolBlocksInSlotRange(
		poolKeyHashes []lcommon.PoolKeyHash,
		startSlot uint64,
		endSlot uint64,
		txn Txn,
	) (map[string]uint64, uint64, error)
	// DeletePoolStakeSnapshotsBeforeEpoch deletes all snapshots before a given
	// epoch.
	DeletePoolStakeSnapshotsBeforeEpoch(epoch uint64, txn Txn) error
	// DeletePoolStakeSnapshotsForEpoch deletes snapshots for a specific epoch
	// and type.
	DeletePoolStakeSnapshotsForEpoch(
		epoch uint64,
		snapshotType string,
		txn Txn,
	) error
	// GetActiveDelegatorCountsByPool counts the active accounts delegated to
	// each requested pool with one grouped query. Pools without a delegator
	// are absent from the map.
	GetActiveDelegatorCountsByPool(
		poolKeyHashes []lcommon.PoolKeyHash,
		txn Txn,
	) (map[string]uint64, error)
	// GetActiveDelegatorCredentialsByPool returns the stake credentials of the
	// active accounts delegated to each requested pool, in the ledger's
	// credential order (script credentials first, then by hash). Pools
	// without a delegator are absent from the map.
	GetActiveDelegatorCredentialsByPool(
		poolKeyHashes []lcommon.PoolKeyHash,
		txn Txn,
	) (map[string][]models.StakeCredentialRef, error)
	// GetActivePoolKeyHashes returns the key hashes of all currently active
	// (registered, non-retired) stake pools. This backs the GetStakePools
	// local-state-query.
	GetActivePoolKeyHashes(ctx context.Context, txn Txn) ([][]byte, error)
	// GetActivePoolKeyHashesAtSlot retrieves the key hashes of pools that were
	// active at the given slot. A pool was active at a slot if:
	// 1. It had a registration with added_slot <= slot
	// 2. Either:
	//    a. No retirement with added_slot <= slot, OR
	//    b. The most recent retirement was for an epoch that hadn't started yet, OR
	//    c. A registration occurred AFTER the most recent retirement (re-registration
	//       cancels a pending retirement)
	//
	// When determining order of events in the same slot, block_index
	// (transaction index within block) and cert_index (certificate index within
	// transaction) are used as tie-breakers since cert_index resets per
	// transaction. The full ordering is: added_slot DESC, block_index DESC,
	// cert_index DESC. This handles cases where registration and retirement
	// occur in different transactions within the same block.
	//
	// This is used for stake snapshot calculations at historical points.
	//
	// Returns types.ErrNoEpochData (wrapped) if epoch data has not been synced
	// for the requested slot. Callers should use errors.Is() to check.
	GetActivePoolKeyHashesAtSlot(slot uint64, txn Txn) ([][]byte, error)
	// GetActivePoolRelays returns all relays from currently active pools.
	// This is used for ledger peer discovery.
	GetActivePoolRelays(
		ctx context.Context,
		txn Txn,
	) ([]models.PoolRegistrationRelay, error)
	// GetDelegatedPoolKeyHashes returns every pool key hash the live reward
	// stake aggregate attributes stake to, including pools that are no longer
	// registered. cardano-ledger's ssTotalActiveStake sums registered
	// credentials holding a delegation without consulting the stake-pool set,
	// so the snapshot's sigma_a denominator must cover these pools too or every
	// reward on the node is under-credited by their share.
	GetDelegatedPoolKeyHashes(txn Txn) ([][]byte, error)
	// GetEpochBoundaryActivePoolKeyHashes excludes boundary retirements in
	// Dijkstra, while retaining the pre-boundary transaction certificate cut.
	GetEpochBoundaryActivePoolKeyHashes(
		slot uint64,
		boundarySlot uint64,
		txn Txn,
	) ([][]byte, error)
	// GetEpochBoundaryDelegatedPoolKeyHashes returns every pool key hash the
	// boundary reconstruction attributes stake to at the snapshot slot,
	// including pools that are no longer registered. It is the historical-path
	// counterpart of GetDelegatedPoolKeyHashes, and exists for the same reason:
	// the sigma_a denominator must be enumerated from delegations, not from the
	// stake-pool set.
	GetEpochBoundaryDelegatedPoolKeyHashes(
		snapshotSlot uint64,
		boundarySlot uint64,
		txn Txn,
	) ([][]byte, error)
	// GetEpochBoundaryStakeByPools is GetStakeByPoolsAtSlot with epoch-boundary
	// (SNAP) reward semantics: reward credits recorded at boundarySlot (which
	// is snapshotSlot+1) are retained unless they are marked
	// AccountRewardDelta.PostSnapshot. That reproduces what the authoritative
	// SNAP-point capture observes — the delayed reward update applied at the
	// boundary, and none of the POOLREAP/MIR/enactment credits that follow it.
	// Only the epoch-boundary mark-snapshot fallback may use this; use
	// GetStakeByPoolsAtSlot for a plain "stake at slot" query. incomingEraID
	// supplies the hard-fork result when the caller is inside the rollover
	// transaction before the incoming epoch row has been inserted; nil resolves
	// it from the epoch table.
	GetEpochBoundaryStakeByPools(
		poolKeyHashes [][]byte,
		snapshotSlot uint64,
		boundarySlot uint64,
		incomingEraID *uint,
		expiryEpoch uint64,
		inactivityPeriod uint64,
		txn Txn,
	) (map[string]uint64, map[string]uint64, error)
	// GetFuturePoolByLeiosKey retrieves the pool whose pending parameters claim
	// the given BLS public key in the current epoch.
	GetFuturePoolByLeiosKey(
		ctx context.Context,
		publicKey []byte,
		epochStartSlot uint64,
		txn Txn,
	) (*models.Pool, error)
	// GetFuturePoolByVrfKeyHash retrieves the pool whose pending parameters
	// claim the given VRF key hash during the current epoch.
	GetFuturePoolByVrfKeyHash(
		ctx context.Context,
		vrfKeyHash []byte,
		epochStartSlot uint64,
		txn Txn,
	) (*models.Pool, error)
	// GetPool returns a pool by its key hash
	GetPool(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		includeInactive bool,
		txn Txn,
	) (*models.Pool, error)
	// GetPoolBlockIssuersInSlotRange returns observed pool/op-cert issuer
	// rows in the inclusive slot range, ordered by slot and pool key hash.
	GetPoolBlockIssuersInSlotRange(
		startSlot uint64,
		endSlot uint64,
		txn Txn,
	) ([]models.PoolOpCertSequence, error)
	// GetPoolByLeiosKey retrieves the pool whose active parameters claim the
	// given BLS public key as of the current epoch's start slot.
	GetPoolByLeiosKey(
		ctx context.Context,
		publicKey []byte,
		epochStartSlot uint64,
		txn Txn,
	) (*models.Pool, error)
	// GetPoolByVrfKeyHash retrieves the pool that currently claims the given
	// VRF key hash, as of the given epoch's start slot. Returns nil if no
	// active pool claims it. See metadata.MetadataStore's GetPoolByVrfKeyHash
	// for the epoch-boundary deferral rule this respects.
	GetPoolByVrfKeyHash(
		ctx context.Context,
		vrfKeyHash []byte,
		epochStartSlot uint64,
		txn Txn,
	) (*models.Pool, error)
	// GetPoolDepositsHeld returns, for each requested pool with a
	// registration, the deposit its latest registration retains: the
	// deposit_held column, or deposit_amount for a row written before that
	// column existed. A re-registration of a still-registered pool carries
	// the earlier deposit forward, so this is not the latest certificate's
	// own charge. Pools without a registration, or whose deposit is unknown
	// (both columns NULL), are absent from the map.
	GetPoolDepositsHeld(
		poolKeyHashes []lcommon.PoolKeyHash,
		txn Txn,
	) (map[string]uint64, error)
	// GetPoolDepositsHeldAtSlot is GetPoolDepositsHeld over the
	// registrations added at or before the slot.
	GetPoolDepositsHeldAtSlot(
		poolKeyHashes []lcommon.PoolKeyHash,
		slot uint64,
		txn Txn,
	) (map[string]uint64, error)
	// GetPoolEarliestVrfKeyHashAtSlot returns the VRF key hash from the
	// pool's earliest registration at or before the given slot, which is what
	// cardano-ledger's psStakePools holds for a pool that first registered
	// inside the captured epoch: the POOL rule inserts a first registration
	// directly and defers only a re-registration through
	// psFutureStakePoolParams. Contrast GetPoolVrfKeyHashAtSlot, which
	// returns the latest such registration.
	GetPoolEarliestVrfKeyHashAtSlot(
		poolKeyHash []byte,
		slot uint64,
		txn Txn,
	) ([]byte, bool, error)
	// GetPoolRegistrations returns a list of pool registration certificates
	GetPoolRegistrations(
		poolKeyHash lcommon.PoolKeyHash,
		txn Txn,
	) ([]lcommon.PoolRegistrationCertificate, error)
	// GetPoolRegistrationsAtSlot retrieves the latest registration for each
	// requested pool at or before the supplied slot. Same-slot ordering must
	// use block_index and cert_index so reward inputs reflect the historical
	// epoch boundary, not the current denormalized pool row.
	GetPoolRegistrationsAtSlot(
		poolKeyHashes []lcommon.PoolKeyHash,
		slot uint64,
		txn Txn,
	) ([]models.PoolRegistration, error)
	// GetPoolRegistrationsEffectiveForEpoch retrieves the registration
	// selected for each pool by the supplied epoch window. It considers the
	// latest registration or retirement before epochStartSlot, then uses the
	// earliest registration in [epochStartSlot, snapshotSlot] for pools without
	// a pre-epoch registration selected, including pools whose latest prior
	// event is a retirement. Callers whose SNAP follows POOLREAP
	// set epochStartSlot to the incoming boundary so registrations from the
	// just-ended epoch are included in the pre-epoch selection.
	GetPoolRegistrationsEffectiveForEpoch(
		poolKeyHashes []lcommon.PoolKeyHash,
		epochStartSlot uint64,
		endedEpoch uint64,
		snapshotSlot uint64,
		txn Txn,
	) ([]models.PoolRegistration, error)
	// GetPoolVrfKeyHashAtSlot returns the VRF key hash the pool had
	// registered as of a slot, using the same latest-certificate-wins
	// ordering as GetActivePoolKeyHashesAtSlot. The bool reports whether any
	// registration exists at or before that slot.
	GetPoolVrfKeyHashAtSlot(
		poolKeyHash []byte,
		slot uint64,
		txn Txn,
	) ([]byte, bool, error)
	// GetPoolVrfState reads the fields needed to resolve a pool's current VRF
	// key. Only PoolKeyHash, VrfKeyHash and the latest Registration's
	// VrfKeyHash are guaranteed. Inactive pools are included; a pool with no
	// registrations stays distinct from one whose latest registration has an
	// empty VRF key hash. Providers without the optional projection use GetPool
	// with associations.
	GetPoolVrfState(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		txn Txn,
	) (*models.Pool, error)
	// GetPools returns pools by key hash.
	GetPools(
		ctx context.Context,
		pkhs []lcommon.PoolKeyHash,
		txn Txn,
	) ([]models.Pool, error)
	// GetPoolsRetiringAtEpoch returns the pools whose effective retirement
	// takes effect at the given epoch as of the boundary slot, with the reward
	// account and deposit needed to refund their POOLREAP deposit.
	GetPoolsRetiringAtEpoch(
		ctx context.Context,
		epoch uint64,
		boundarySlot uint64,
		txn Txn,
	) ([]models.PoolRetirementRefund, error)
	// GetRetiringPools returns pools whose latest retirement
	// certificate targets an epoch after currentEpoch and has not been
	// cancelled by a later registration certificate. Certificate
	// recency compares (added_slot, synthetic-import precedence,
	// block_index, cert_index). Results are ordered by retirement
	// epoch, then announcement position, matching Blockfrost.
	GetRetiringPools(
		currentEpoch uint64,
		txn Txn,
	) ([]models.PoolRetiringRow, error)
	// GetRetiringPoolsAtSlot is GetRetiringPools as the chain stood at the
	// slot: certificates added after it neither announce a retirement nor
	// cancel one.
	GetRetiringPoolsAtSlot(
		currentEpoch uint64,
		slot uint64,
		txn Txn,
	) ([]models.PoolRetiringRow, error)
	// LatestPoolOpCertSequence returns the highest observed op-cert sequence
	// for a pool.
	LatestPoolOpCertSequence(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		txn Txn,
	) (uint64, bool, error)
	// LatestPoolOpCertSequenceAfter returns the highest observed op-cert
	// sequence for a pool strictly after afterSlot.
	LatestPoolOpCertSequenceAfter(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		afterSlot uint64,
		txn Txn,
	) (uint64, bool, error)
	// LatestPoolOpCertSequenceAtOrBefore returns the highest observed op-cert
	// sequence for a pool at or before slot. It provides a historical
	// chain-dependent view without changing or restoring the live database tip.
	LatestPoolOpCertSequenceAtOrBefore(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		slot uint64,
		txn Txn,
	) (uint64, bool, error)
	// LatestPoolOpCertSequenceInRange returns the highest observed op-cert
	// sequence for a pool between the inclusive slot bounds.
	LatestPoolOpCertSequenceInRange(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		startSlot uint64,
		endSlot uint64,
		txn Txn,
	) (uint64, bool, error)
	// LatestPoolOpCertSequences returns the highest observed op-cert sequence
	// for every pool that has issued a block, keyed by pool key hash. This
	// backs the GetChainDepState local-state-query, whose counters cover every
	// cold key the chain has accepted a certificate for rather than only the
	// active pools.
	LatestPoolOpCertSequences(
		ctx context.Context,
		txn Txn,
	) (map[string]uint64, error)
	// LatestPoolOpCertSequencesAtOrBefore is LatestPoolOpCertSequences as the
	// counters stood at slot.
	LatestPoolOpCertSequencesAtOrBefore(
		ctx context.Context,
		slot uint64,
		txn Txn,
	) (map[string]uint64, error)
	// PoolOpCertSequencesExistAtSlot reports whether any pool has an op-cert
	// sequence row at exactly slot. Ledger validation reads it at the Mithril
	// trust boundary to tell an imported certified counter map from one that
	// was never imported.
	PoolOpCertSequencesExistAtSlot(
		ctx context.Context,
		slot uint64,
		txn Txn,
	) (bool, error)
	// UpdatePoolOpCertSequence records an observed op-cert sequence for a pool
	// and updates the pool's denormalized maximum. The first counter written
	// for a (pool, slot) wins; a different counter at that slot returns an
	// opcert counter conflict error and rolls back the write transaction.
	UpdatePoolOpCertSequence(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		sequence uint64,
		slot uint64,
		txn Txn,
	) error
	// UpdatePoolOpCertSequenceBatched queues an observed op-cert sequence in
	// acc. FlushBatch writes every queued observation and advances each pool's
	// denormalized maximum inside txn.
	UpdatePoolOpCertSequenceBatched(
		ctx context.Context,
		pkh lcommon.PoolKeyHash,
		sequence uint64,
		slot uint64,
		acc database.BatchAccumulator,
		txn Txn,
	) error
}

// SnapshotStore holds per-epoch pool stake snapshots and the live stake inputs
// they are built from.
type SnapshotStore interface {
	// GetLiveStakeInputsForPools returns every registered credential (including
	// zero-stake credentials) from the transactionally maintained live reward
	// aggregate for the requested pools. expiryEpoch applies the live
	// CIP-0163 account-expiration filter when nonzero.
	GetLiveStakeInputsForPools(
		poolKeyHashes [][]byte,
		expiryEpoch uint64,
		txn Txn,
	) ([]*models.RewardStakeInput, error)
	// GetPointerStakeInputsForPools returns the per-credential stake held at a
	// pointer address for pools in poolKeyHashes, resolved and delegated as of
	// slot. It is additive: the caller adds it to what
	// GetLiveStakeInputsForPools returned, because reward_live_stake never
	// carries pointer-derived UTxO stake -- attribution depends on certificate
	// history at slot, not on anything the live aggregate's incremental
	// maintenance can express. boundarySlot is the epoch-boundary era gate
	// (0 = no boundary; see GetEpochBoundaryStakeByPools), and expiryEpoch
	// drives the same live CIP-0163 gate GetLiveStakeInputsForPools applies.
	GetPointerStakeInputsForPools(
		poolKeyHashes [][]byte,
		slot uint64,
		boundarySlot uint64,
		expiryEpoch uint64,
		txn Txn,
	) ([]*models.RewardStakeInput, error)
	// GetPoolStakeSnapshot retrieves a specific pool's stake snapshot for an
	// epoch.
	GetPoolStakeSnapshot(
		epoch uint64,
		snapshotType string,
		poolKeyHash []byte,
		txn Txn,
	) (*models.PoolStakeSnapshot, error)
	// GetPoolStakeSnapshotsByEpoch returns all pool stake snapshots of
	// snapshotType for the given epoch.
	GetPoolStakeSnapshotsByEpoch(
		epoch uint64,
		snapshotType string,
		txn Txn,
	) ([]*models.PoolStakeSnapshot, error)
	// GetPoolStakeSnapshotsByEpochBounded retrieves at most maxRows pool
	// stake snapshots for an epoch.
	GetPoolStakeSnapshotsByEpochBounded(
		epoch uint64,
		snapshotType string,
		maxRows int,
		txn Txn,
	) ([]*models.PoolStakeSnapshot, error)
	// GetPoolStakeSnapshotsForPools retrieves the snapshot rows for just the
	// pools named, for a caller wanting a bounded subset rather than a whole
	// epoch. A pool the snapshot has no row for is absent from the result.
	// The read is chunked rather than issued once per pool named.
	GetPoolStakeSnapshotsForPools(
		epoch uint64,
		snapshotType string,
		poolKeyHashes [][]byte,
		txn Txn,
	) ([]*models.PoolStakeSnapshot, error)
	// GetStakeByPools returns delegated stake for multiple pools in a single
	// query: pool key hash (as a string) to total stake, and to delegator
	// count. A pool with no delegation maps to zero.
	GetStakeByPools(
		ctx context.Context,
		poolKeyHashes [][]byte,
		txn Txn,
	) (map[string]uint64, map[string]uint64, error)
	// GetTotalActiveStake returns the sum of all pool stakes for an epoch.
	GetTotalActiveStake(
		epoch uint64,
		snapshotType string,
		txn Txn,
	) (uint64, error)
	// SavePoolStakeSnapshots saves multiple pool stake snapshots in batch.
	SavePoolStakeSnapshots(snapshots []*models.PoolStakeSnapshot, txn Txn) error
}

// GovernanceStore holds proposals, votes, DReps, the constitutional committee
// and the constitution.
type GovernanceStore interface {
	// BumpDormantDRepExpiries extends active DRep expiries at an empty Conway
	// governance boundary. Replaying the same boundary slot is idempotent.
	BumpDormantDRepExpiries(
		ctx context.Context,
		slot uint64,
		txn Txn,
	) (int, error)
	// ClearCommitteeQuorum records at the given slot that no quorum is
	// in effect (e.g. after a NoConfidence action is enacted). A later
	// GetCommitteeQuorum will return nil until a subsequent
	// SetCommitteeQuorum writes a new threshold.
	ClearCommitteeQuorum(ctx context.Context, slot uint64, txn Txn) error
	// ClearDanglingDRepDelegations applies the cardano-ledger Conway HARDFORK
	// STS rule at protocol major version 10 (Plomin): it clears
	// credential-backed account delegations to inactive DReps and rebuilds the
	// reverse delegator set from active account state. Both changes are
	// rollbackable to atSlot. Pseudo-DRep delegations (AlwaysAbstain,
	// AlwaysNoConfidence) are preserved. Returns the number of account
	// delegations cleared.
	//
	// See cardano-ledger Conway/Rules/HardFork.hs (updateDRepDelegations).
	ClearDanglingDRepDelegations(
		ctx context.Context,
		atSlot uint64,
		txn Txn,
	) (int, error)
	// ClearGovernanceProposalRatification moves a proposal back to the active,
	// pending state at transitionSlot. Governance epoch processing uses it when
	// a legacy ratified row fails a deterministic enactment precondition.
	ClearGovernanceProposalRatification(
		ctx context.Context,
		txHash []byte,
		actionIndex uint32,
		transitionSlot uint64,
		txn Txn,
	) error
	// DeleteGovernanceVotesForDrep soft-deletes a DRep's votes on proposals
	// active in the current epoch at its deregistration slot so rollback can
	// restore them.
	DeleteGovernanceVotesForDrep(
		ctx context.Context,
		credentialTag uint8,
		credential []byte,
		epoch uint64,
		deletedSlot uint64,
		txn Txn,
	) (int, error)
	// GetActiveCommitteeMembers returns all active committee members
	GetActiveCommitteeMembers(
		ctx context.Context,
		txn Txn,
	) ([]*models.AuthCommitteeHot, error)
	// GetActiveDreps returns all active DReps
	GetActiveDreps(ctx context.Context, txn Txn) ([]*models.Drep, error)
	// GetActiveGovernanceProposals returns all governance proposals that are
	// still in the active pool (not expired, not enacted, not soft-deleted).
	GetActiveGovernanceProposals(
		ctx context.Context,
		epoch uint64,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetCommitteeAuthorizedCount returns the number of seated, non-resigned
	// committee members that hold a current hot-key authorization. Members
	// without a hot key are not counted and term expiry is not applied.
	GetCommitteeAuthorizedCount(ctx context.Context, txn Txn) (int, error)
	// GetCommitteeHotAuthorizationsSince returns, for each cold credential, its
	// latest hot-key authorization when that authorization was recorded at or
	// after minSlot. Seated and unseated cold credentials are both included.
	GetCommitteeHotAuthorizationsSince(
		ctx context.Context,
		minSlot uint64,
		txn Txn,
	) ([]*models.AuthCommitteeHot, error)
	// GetCommitteeMember returns a committee member by cold key
	GetCommitteeMember(
		ctx context.Context,
		coldCredentialTag uint8,
		coldKey []byte,
		termStartSlot uint64,
		txn Txn,
	) (*models.AuthCommitteeHot, error)
	// GetCommitteeMembers returns all active (non-deleted) governance-enacted
	// committee members.
	GetCommitteeMembers(
		ctx context.Context,
		txn Txn,
	) ([]*models.CommitteeMember, error)
	// GetCommitteeMembersIncludeDeleted returns all committee members
	// including soft-deleted rows. Used to detect whether a committee
	// was ever seated — after a NoConfidence action, GetCommitteeMembers
	// returns no rows, but the committee was seated previously.
	GetCommitteeMembersIncludeDeleted(
		ctx context.Context,
		txn Txn,
	) ([]*models.CommitteeMember, error)
	// GetCommitteeQuorum returns the latest enacted committee quorum.
	GetCommitteeQuorum(ctx context.Context, txn Txn) (*big.Rat, error)
	// GetConstitution returns the current constitution
	GetConstitution(txn Txn) (*models.Constitution, error)
	// GetDRepDelegators returns the stake credentials currently delegating
	// their voting power to the given DRep, in canonical (tag, hash) order.
	// This is the `delegators` member of the GetDRepState ledger query result.
	// credentialTag distinguishes key (0) from script (1) DRep credentials
	// sharing the same hash.
	GetDRepDelegators(
		ctx context.Context,
		credentialTag uint8,
		drepCredential []byte,
		txn Txn,
	) ([]models.StakeCredentialRef, error)
	// GetDRepDelegatorsAtSlot returns the stake credentials delegating to each
	// of dreps (every DRep when empty) at slot, keyed by the DRep's
	// StakeCredentialRef.MapKey().
	GetDRepDelegatorsAtSlot(
		ctx context.Context,
		dreps []models.StakeCredentialRef,
		slot uint64,
		txn Txn,
	) (map[string][]models.StakeCredentialRef, error)
	// GetDRepVotingPower calculates the voting power for a DRep by summing
	// the current stake of all delegated accounts, approximated from live
	// UTxO balance plus reward-account balance. credentialTag distinguishes
	// key (0) from script (1) DRep credentials sharing the same 28-byte hash.
	// expiryEpoch is the CIP-0163 reward-account inactivity gate: 0 = off
	// (byte-identical to the pre-CIP query), >0 = exclude accounts whose
	// expiration_epoch is nonzero and less than expiryEpoch.
	GetDRepVotingPower(
		ctx context.Context,
		credentialTag uint8,
		drepCredential []byte,
		expiryEpoch uint64,
		txn Txn,
	) (uint64, error)
	// GetDRepVotingPowerBatch is the batch form of GetDRepVotingPower; see
	// the metadata-store interface for the contract. expiryEpoch is the
	// CIP-0163 gate; see GetDRepVotingPower.
	GetDRepVotingPowerBatch(
		ctx context.Context,
		drepCredentials []models.StakeCredentialRef,
		expiryEpoch uint64,
		txn Txn,
	) (map[string]uint64, error)
	// GetDRepVotingPowerByType returns voting power grouped by DRep
	// delegation type. expiryEpoch is the CIP-0163 gate; see
	// GetDRepVotingPower.
	GetDRepVotingPowerByType(
		ctx context.Context,
		drepTypes []uint64,
		expiryEpoch uint64,
		txn Txn,
	) (map[uint64]uint64, error)
	// GetDormantDRepEpochs returns the current count of consecutive dormant
	// epochs, used to extend DRep expiry.
	GetDormantDRepEpochs(ctx context.Context, txn Txn) (uint64, error)
	// GetDrepByCredential returns a drep by the full credential identity (tag +
	// hash).
	GetDrepByCredential(
		ctx context.Context,
		credentialTag uint8,
		cred []byte,
		includeInactive bool,
		txn Txn,
	) (*models.Drep, error)
	// GetDrepLastRegistrationDeposit returns the deposit amount recorded
	// against the most recent registration certificate for the DRep
	// credential, or nil when no recorded deposit exists.
	GetDrepLastRegistrationDeposit(
		ctx context.Context,
		credentialTag uint8,
		credential []byte,
		txn Txn,
	) (*uint64, error)
	// GetDrepLastRegistrationDeposits returns the most recent registration
	// deposit of every active DRep, keyed by models.DrepDepositKey, so a caller
	// listing all active DReps does not need one query per DRep. Credentials
	// with no registration_drep row are absent from the map.
	GetDrepLastRegistrationDeposits(
		ctx context.Context,
		txn Txn,
	) (map[string]uint64, error)
	// GetDrepRegistrationDepositsAtSlot returns the deposit recorded against
	// the latest registration at or before slot of each of refs (every DRep
	// when empty), keyed by models.DrepDepositKey.
	GetDrepRegistrationDepositsAtSlot(
		ctx context.Context,
		refs []models.StakeCredentialRef,
		slot uint64,
		txn Txn,
	) (map[string]uint64, error)
	// GetDrepsAtSlot returns the given DReps (every DRep when refs is empty)
	// that were registered at slot, as they stood there.
	GetDrepsAtSlot(
		ctx context.Context,
		refs []models.StakeCredentialRef,
		slot uint64,
		txn Txn,
	) ([]*models.Drep, error)
	// GetDroppedGovernanceProposalsAt returns proposals dropped (deposit
	// returned) at the exact epoch-boundary slot. Used by epoch replay to
	// restore deposit-return effects.
	GetDroppedGovernanceProposalsAt(
		ctx context.Context,
		epoch uint64,
		slot uint64,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetEnactedGovernanceProposalsAt returns proposals enacted at the exact
	// epoch-boundary slot. Used by epoch replay to restore enactment effects.
	GetEnactedGovernanceProposalsAt(
		ctx context.Context,
		epoch uint64,
		slot uint64,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetExpiredAwaitingDropGovernanceProposals returns proposals whose
	// expired_epoch is strictly below the given epoch and whose deposit has not
	// yet been returned. Used at epoch start, before marking any new proposals
	// expired, to return the deposit and finalize proposals expired as of a
	// prior boundary.
	GetExpiredAwaitingDropGovernanceProposals(
		ctx context.Context,
		epoch uint64,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetExpiredDReps returns all active DReps whose expiry epoch is at
	// or before the given epoch.
	GetExpiredDReps(
		ctx context.Context,
		epoch uint64,
		txn Txn,
	) ([]*models.Drep, error)
	// GetExpiredGovernanceProposalsAt returns proposals expired at the exact
	// epoch-boundary slot. Used by epoch replay to restore deposit-return
	// effects.
	GetExpiredGovernanceProposalsAt(
		ctx context.Context,
		epoch uint64,
		slot uint64,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetExpiringGovernanceProposals returns unratified proposals whose
	// expires_epoch is strictly less than the given epoch and that have not yet
	// been enacted, expired, or soft-deleted.
	GetExpiringGovernanceProposals(
		ctx context.Context,
		epoch uint64,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetGovernanceProposal returns a governance proposal by transaction hash
	// and action index
	GetGovernanceProposal(
		ctx context.Context,
		txHash []byte,
		actionIndex uint32,
		txn Txn,
	) (*models.GovernanceProposal, error)
	// GetGovernanceProposalSet returns the Conway proposals set: every proposal
	// not yet enacted, dropped, or soft-deleted.
	GetGovernanceProposalSet(
		ctx context.Context,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetGovernanceProposalSetAtSlot returns the Conway proposals set as it
	// stood at slot.
	GetGovernanceProposalSetAtSlot(
		ctx context.Context,
		slot uint64,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetGovernanceVotes returns all votes for a governance proposal
	GetGovernanceVotes(
		ctx context.Context,
		proposalID uint,
		txn Txn,
	) ([]*models.GovernanceVote, error)
	// GetGovernanceVotesAtSlot returns a governance proposal's votes as they
	// stood at slot.
	GetGovernanceVotesAtSlot(
		ctx context.Context,
		proposalID uint,
		slot uint64,
		txn Txn,
	) ([]*models.GovernanceVote, error)
	// GetLastEnactedGovernanceProposal returns the most recently enacted
	// proposal whose action_type is in actionTypes, or nil if none exist.
	// Callers group per-purpose action types (CIP-1694 chain roots) in
	// the slice; the single-type case passes a one-element slice.
	GetLastEnactedGovernanceProposal(
		ctx context.Context,
		actionTypes []uint8,
		txn Txn,
	) (*models.GovernanceProposal, error)
	// GetRatifiedGovernanceProposals returns proposals ratified but not yet
	// enacted, ordered by (ratified_epoch, ratified_slot, id). Used at epoch
	// start for enactment.
	GetRatifiedGovernanceProposals(
		ctx context.Context,
		txn Txn,
	) ([]*models.GovernanceProposal, error)
	// GetResignedCommitteeMembers returns cold credentials with a resignation
	// record in each credential's selected membership term.
	GetResignedCommitteeMembers(
		ctx context.Context,
		coldCredentials []models.CommitteeCredential,
		txn Txn,
	) (map[string]bool, error)
	// InsertDrepIfAbsent inserts a minimal DRep row when no record exists
	// for the given credential. Existing rows are left untouched so real
	// registration metadata (added_slot, anchor_url, anchor_hash, active)
	// is never overwritten by the vote-replay recovery path.
	InsertDrepIfAbsent(
		ctx context.Context,
		credentialTag uint8,
		cred []byte,
		slot uint64,
		url string,
		hash []byte,
		active bool,
		txn Txn,
	) error
	// IsCommitteeMemberResigned checks if a committee member has resigned
	IsCommitteeMemberResigned(
		ctx context.Context,
		coldCredentialTag uint8,
		coldKey []byte,
		termStartSlot uint64,
		txn Txn,
	) (bool, error)
	// RecordDRepActivityEpoch sets a DRep's last activity epoch without
	// touching its expiry. Historical replay below a Mithril anchor uses it:
	// the snapshot's DRepState expiry already reflects the dormant-epoch rules
	// replay does not run, so recomputing expiry from a historical vote or
	// certificate would replace a correct value with a stale one.
	RecordDRepActivityEpoch(
		ctx context.Context,
		credentialTag uint8,
		drepCredential []byte,
		activityEpoch uint64,
		txn Txn,
	) error
	// ResetDormantDRepEpochs resets the dormant-epoch count as of slot.
	ResetDormantDRepEpochs(ctx context.Context, slot uint64, txn Txn) error
	// ResolvePoolRewardAccountAutoVotes classifies each PoolStakeSnapshot
	// with the CIP-1694 reward-account DRep-delegation outcome and writes
	// the result onto snapshot.RewardAccountAutoVote in place. Callers
	// invoke this immediately before persisting the snapshots so the
	// auto-vote signal is frozen with the snapshot rather than re-derived
	// from live state at tally time.
	//
	// Resolution proceeds in two batched lookups:
	//  1. Pool rows yield each pool's reward-account stake credential.
	//  2. Account rows (active and inactive) yield the DRep delegation type
	//     for each credential.
	//
	// RewardAccountAutoVoteResolved is set true only when the outcome is
	// determined from real data, at exactly three terminal conditions:
	//   - The pool row exists and carries no reward account → confirmed None.
	//   - The pool row exists, its reward-account row is present and ACTIVE →
	//     classified by DRep delegation (Abstain / NoConfidence / None).
	//   - The pool row exists, its reward-account row is present but INACTIVE
	//     (deregistered) → confirmed None, since CIP-1694 treats unregistered
	//     reward accounts as implicit no.
	//
	// Resolved is left false (so the tally falls back to implicit no without
	// freezing a value) when:
	//   - The pool row is absent from the DB (cannot determine the reward
	//     account at all), or
	//   - The pool's reward-account credential has no row in the account table
	//     at all. An absent row is ambiguous: it may mean the account was never
	//     registered, OR that account data has not yet been imported (e.g. a
	//     Mithril restore that imported pools from the snapshot fallback before
	//     cert-state accounts were loaded). Persisting Resolved=true here would
	//     conflate "account input unavailable" with "confirmed none".
	ResolvePoolRewardAccountAutoVotes(
		ctx context.Context,
		snapshots []*models.PoolStakeSnapshot,
		txn Txn,
	) error
	// SetCommitteeMembers upserts governance-enacted committee members. Used
	// by UpdateCommittee action enactment and snapshot import.
	SetCommitteeMembers(
		ctx context.Context,
		members []*models.CommitteeMember,
		txn Txn,
	) error
	// SetCommitteeQuorum stores the quorum threshold enacted with a committee.
	SetCommitteeQuorum(
		ctx context.Context,
		quorum *big.Rat,
		slot uint64,
		txn Txn,
	) error
	// SetConstitution saves the constitution
	SetConstitution(constitution *models.Constitution, txn Txn) error
	// SetGenesisGovernance stores initial DReps and delegations from the
	// Conway genesis bootstrap section. This is metadata-only.
	SetGenesisGovernance(
		initialDReps conway.ConwayGenesisInitialDReps,
		delegs conway.ConwayGenesisDelegs,
		blockHash []byte,
		txn Txn,
	) error
	// SetGovernanceProposal creates or updates a governance proposal
	SetGovernanceProposal(
		ctx context.Context,
		proposal *models.GovernanceProposal,
		txn Txn,
	) error
	// SetGovernanceVote records a vote on a governance proposal
	SetGovernanceVote(
		ctx context.Context,
		vote *models.GovernanceVote,
		txn Txn,
	) error
	// SoftDeleteAllCommitteeMembers marks all active committee members as
	// removed. Used by NoConfidence action enactment.
	SoftDeleteAllCommitteeMembers(
		ctx context.Context,
		slot uint64,
		txn Txn,
	) error
	// SoftDeleteCommitteeMembers marks the given cold credential hashes as
	// removed. Used by UpdateCommittee action enactment to remove members.
	SoftDeleteCommitteeMembers(
		ctx context.Context,
		coldCredentials []models.CommitteeCredential,
		slot uint64,
		txn Txn,
	) error
	// UpdateDRepActivity updates the DRep's last activity epoch and
	// recalculates the expiry epoch.
	UpdateDRepActivity(
		ctx context.Context,
		credentialTag uint8,
		drepCredential []byte,
		activityEpoch uint64,
		inactivityPeriod uint64,
		slot uint64,
		txn Txn,
	) error
}

// EpochStore holds epochs, protocol parameters, network state, block nonces and
// genesis state.
type EpochStore interface {
	// GetPParamsRows returns the stored protocol-parameter rows for epoch and
	// era eraId, undecoded.
	GetPParamsRows(epoch uint64, eraId uint, txn Txn) ([]models.PParams, error)
	// AddNetworkDonation records a block's total Conway treasury
	// donation for the given slot and epoch. Idempotent per slot.
	AddNetworkDonation(slot uint64, epoch uint64, amount uint64, txn Txn) error
	// ComputeAndApplyPParamUpdates computes the new protocol parameters by
	// applying the pending update to enact for the given epoch, and persists
	// the result for that epoch. The epoch parameter is the epoch where the
	// updates take effect (currentEpoch + 1 during epoch rollover); per the
	// Shelley update system the enacted update is the one at least quorum
	// genesis keys proposed, with identical values, for epoch-1 (see
	// selectClassicPParamUpdate). The quorum parameter comes from
	// shelley-genesis.json updateQuorum. The epoch-1 record must exist when a
	// proposal for it carries a protocol version update. Although the interface
	// is passed by value, era-specific update functions may mutate its
	// underlying concrete protocol-parameter pointer in place. Callers that
	// need the original value preserved must pass an independently owned copy;
	// the returned value is the authoritative updated parameter set.
	//
	// hasPlutusV2CostModelFunc reports whether the enacted update itself (not
	// the merged result) explicitly specifies a PlutusV2 cost model (map key
	// 1). This is the pre-Conway equivalent of governance.EnactmentResult's
	// PlutusV2CostModelWritten: on a network that forks into Babbage before
	// receiving a real PlutusV2 cost model, that model can arrive through this
	// classic Shelley-style update system (as it did on real mainnet, well
	// before CIP-1694 governance existed), and the caller needs the same
	// real-write provenance signal here that governance.EnactProposal provides
	// for the Conway/Dijkstra path -- comparing the merged result's value
	// before and after is unsound for the same reason it is there:
	// HardForkBabbage's synthetic default is the real, canonical mainnet value,
	// so a real update writing that exact value would otherwise look unchanged.
	// May be nil (no signal available for this era, e.g. Byron), in which case
	// the returned bool is always false.
	ComputeAndApplyPParamUpdates(
		ctx context.Context,
		slot uint64,
		epoch uint64,
		era uint,
		quorum int,
		currentPParams lcommon.ProtocolParameters,
		decodeFunc func([]byte) (any, error),
		updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
		hasPlutusV2CostModelFunc func(any) bool,
		txn Txn,
	) (lcommon.ProtocolParameters, bool, error)
	// DeleteBlockNoncesAfterPoint removes nonces that cannot belong to the
	// active chain after rolling back to point. It keeps the exact point row
	// and removes competing rows at the same slot.
	DeleteBlockNoncesAfterPoint(point common.Point, txn Txn) error
	// DeleteBlockNoncesBeforeSlotWithoutCheckpoints removes non-checkpoint
	// block_nonces older than the given slot number
	DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		slotNumber uint64,
		txn Txn,
	) error
	// ForecastPParamUpdates computes the protocol parameters that the epoch
	// rollover will enact for the given epoch by applying the pending proposed
	// protocol-parameter update already collected in ledger state, WITHOUT
	// persisting anything. It selects the update with
	// selectClassicPParamUpdate, exactly as ComputeAndApplyPParamUpdates does,
	// but performs no writes, so it is safe to call from header verification
	// and concurrently.
	//
	// It does not mutate currentPParams: era update functions mutate their
	// concrete pointer in place (see PParamsUpdateShelley), so before applying
	// an update it clones currentPParams via cloneFunc and mutates the clone.
	// The clone happens only when an update will actually be enacted, so the
	// common no-op forecast pays no clone cost and returns the original
	// currentPParams pointer. When no pending update meets quorum for the
	// epoch — no proposals, quorum not met, or epoch is 0 — it returns
	// currentPParams unchanged, matching the "nothing enacted" forecast.
	ForecastPParamUpdates(
		epoch uint64,
		quorum int,
		currentPParams lcommon.ProtocolParameters,
		decodeFunc func([]byte) (any, error),
		updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
		cloneFunc func(lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error),
		txn Txn,
	) (lcommon.ProtocolParameters, error)
	// GetBlockNonce fetches the block nonce for a given chain point
	GetBlockNonce(point common.Point, txn Txn) ([]byte, error)
	// GetBlockNoncesInSlotRange fetches all block nonces in [startSlot,
	// endSlot).
	GetBlockNoncesInSlotRange(
		startSlot uint64,
		endSlot uint64,
		txn Txn,
	) ([]models.BlockNonce, error)
	// GetEpoch returns the persisted epoch epochId.
	GetEpoch(epochId uint64, txn Txn) (*models.Epoch, error)
	// GetEpochBySlot returns the persisted epoch that contains slot.
	GetEpochBySlot(slot uint64, txn Txn) (*models.Epoch, error)
	// GetEpochSummary retrieves the summary for a specific epoch.
	GetEpochSummary(epoch uint64, txn Txn) (*models.EpochSummary, error)
	// GetEpochs returns every persisted epoch.
	GetEpochs(txn Txn) ([]models.Epoch, error)
	// GetEpochsByEra returns the persisted epochs of era eraId.
	GetEpochsByEra(eraId uint, txn Txn) ([]models.Epoch, error)
	// GetLatestBlockNonce returns the highest-slot block_nonce row, the
	// authoritative high-water mark of durably applied ledger state. The bool
	// is false when no rows exist.
	GetLatestBlockNonce(txn Txn) (models.BlockNonce, bool, error)
	// GetNetworkState retrieves the most recent network state.
	GetNetworkState(txn Txn) (*models.NetworkState, error)
	// GetNetworkStateAsOfSlot retrieves the most recent network state
	// recorded at or before the given slot, for a historical
	// GetStakeDistribution answer rather than
	// GetNetworkState's always-latest row.
	GetNetworkStateAsOfSlot(slot uint64, txn Txn) (*models.NetworkState, error)
	// GetPParams resolves the protocol-parameters row at epoch <= the
	// supplied epoch whose era_id matches eraId, then decodes it with
	// decodeFunc. The era filter is required because at era boundaries
	// the rollover path writes both an old-era row (post-pparams-update)
	// and a new-era row (transitionToEra) at the same epoch — without
	// the filter, the latest insert wins regardless of shape and the
	// caller's era-specific decoder rejects the CBOR on element count.
	GetPParams(
		epoch uint64,
		eraId uint,
		decodeFunc func([]byte) (lcommon.ProtocolParameters, error),
		txn Txn,
	) (lcommon.ProtocolParameters, error)
	// HasAnyGenesisCbor checks whether any genesis CBOR data exists at the
	// given slot, regardless of hash. Only ID-zero synthetic records qualify;
	// genuine Byron slot-zero blocks do not identify the configured genesis.
	HasAnyGenesisCbor(slot uint64) (bool, error)
	// HasGenesisCbor checks whether genesis CBOR data exists at the expected
	// blob key for the given slot and hash. This is used to validate that
	// existing chain data matches the current genesis configuration.
	HasGenesisCbor(slot uint64, hash []byte) (bool, error)
	// ListPParamsForEra returns every stored protocol-parameter row for an era
	// in insertion order. Callers that need the era's initial parameters use
	// the first row rather than substituting a later parameter update.
	ListPParamsForEra(eraID uint, txn Txn) ([]models.PParams, error)
	// ReconcileAlonzoPParamsUnitAfterRecovery repairs a conservative v20
	// classification only when commit-timestamp recovery removed every
	// persisted Alonzo protocol-parameter row. The migration must initially
	// treat any such row as legacy because releases before gouroboros v0.205.7
	// stored a lossy per-byte value. Once recovery has rolled all of those rows
	// back, however, there is no incompatible value left to protect and a
	// genesis replay can safely persist the corrected per-word representation.
	//
	// Missing and unknown markers are never inferred here. They remain hard
	// failures in checkAlonzoPParamsUnit rather than being silently blessed.
	ReconcileAlonzoPParamsUnitAfterRecovery(ctx context.Context) error
	// SaveEpochSummary saves an epoch summary.
	SaveEpochSummary(summary *models.EpochSummary, txn Txn) error
	// SetBlockNonce records the nonce of the block blockHash at slotNumber.
	// isCheckpoint marks the first stored nonce of an epoch, which
	// DeleteBlockNoncesBeforeSlotWithoutCheckpoints keeps.
	SetBlockNonce(
		blockHash []byte,
		slotNumber uint64,
		nonce []byte,
		isCheckpoint bool,
		txn Txn,
	) error
	// SetBlockNonces stores block nonces, using a batch-capable metadata
	// backend when available.
	SetBlockNonces(nonces []models.BlockNonce, txn Txn) error
	// SetEpoch records the epoch that starts at slot, with its nonces, era and
	// length.
	SetEpoch(
		slot uint64,
		epoch uint64,
		nonce []byte,
		evolvingNonce []byte,
		candidateNonce []byte,
		lastEpochBlockNonce []byte,
		era uint,
		slotLength uint,
		lengthInSlots uint,
		txn Txn,
	) error
	// SetGenesisCbor stores synthetic genesis CBOR data without creating a
	// block index entry. This allows the CBOR to be retrieved for offset-based
	// UTxO extraction while preventing the chain iterator from trying to decode
	// it as a real block (which would fail since genesis CBOR is just
	// concatenated UTxO data, not a valid block structure).
	SetGenesisCbor(slot uint64, hash []byte, cborData []byte, txn Txn) error
	// SetGenesisTransaction stores a genesis transaction with its UTxO outputs.
	// Genesis transactions have no inputs, witnesses, or fees - just outputs.
	// The offsets map contains pre-computed byte offsets into the synthetic
	// genesis block.
	SetGenesisTransaction(
		ctx context.Context,
		txHash []byte,
		blockHash []byte,
		outputs []lcommon.Utxo,
		offsets map[database.UtxoRef]database.CborOffset,
		txn Txn,
	) error
	// SetNetworkState stores the treasury and reserves balances.
	SetNetworkState(
		treasury uint64,
		reserves uint64,
		slot uint64,
		txn Txn,
	) error
	// SetPParams records the CBOR-encoded protocol parameters effective from
	// epoch.
	SetPParams(
		params []byte,
		slot uint64,
		epoch uint64,
		era uint,
		txn Txn,
	) error
	// SumNetworkDonationsForEpoch returns the total donation
	// contributed by blocks in the given epoch.
	SumNetworkDonationsForEpoch(epoch uint64, txn Txn) (uint64, error)
}

// ChainStore holds the chain tip, sync state, the block index and deferred
// header markers.
type ChainStore interface {
	// BlockAtOrAfterIndex returns the first block whose chain index is greater
	// than or equal to blockIndex. It seeks the ordered block-index keys so
	// sparse imported chains do not require one lookup and transaction per
	// missing index.
	BlockAtOrAfterIndex(blockIndex uint64, txn Txn) (models.Block, error)
	// FirstBlockAtOrAfterSlot returns the lowest-slot ranking block whose slot
	// is at or above slot, or models.ErrBlockNotFound when there is none.
	// Genesis and other synthetic blobs are skipped. It reads in txn, so the
	// answer matches the snapshot the rest of the transaction sees.
	FirstBlockAtOrAfterSlot(slot uint64, txn Txn) (models.Block, error)
	// BlockByIndex returns the block with chain index blockIndex.
	BlockByIndex(blockIndex uint64, txn Txn) (models.Block, error)
	// BlockCreate stores block.
	BlockCreate(block models.Block, txn Txn) error
	// BlockPointByIndex returns the point encoded in the current block-index
	// entry without loading the referenced block CBOR or metadata. It is
	// intended for primary-chain membership checks that need only the canonical
	// slot and hash at an internal block index.
	BlockPointByIndex(blockIndex uint64, txn Txn) (common.Point, error)
	// DeleteDeferredHeaderMarker removes the marker for key. Deleting an absent
	// marker is not an error.
	DeleteDeferredHeaderMarker(key string) error
	// DeleteSyncState removes a sync state key.
	DeleteSyncState(key string, txn Txn) error
	// EnsureBlockInventory creates the retained-block inventory when it is
	// absent. It is safe to call during commit-timestamp recovery and is a
	// no-op after the inventory has been initialized.
	EnsureBlockInventory() error
	// GetDeferredHeaderMarkerValue returns a copy of a marker's opaque payload.
	// found is false when no marker exists for key.
	GetDeferredHeaderMarkerValue(key string) ([]byte, bool, error)
	// GetSyncState retrieves a sync state value by key.
	// Returns empty string if the key does not exist.
	GetSyncState(key string, txn Txn) (string, error)
	// GetSyncStateBounded retrieves a sync state value by key without reading a
	// value larger than maxBytes; see metadata.ErrSyncStateTooLarge.
	// Returns empty string if the key does not exist.
	GetSyncStateBounded(key string, maxBytes int, txn Txn) (string, error)
	// GetTip returns the current tip as represented by the protocol
	GetTip(txn Txn) (chainsync.Tip, error)
	// ListDeferredHeaderMarkerValues returns every persisted marker and payload
	// in one blob-store read transaction.
	ListDeferredHeaderMarkerValues() ([]database.DeferredHeaderMarker, error)
	// ListSyncStateKeysByPrefix returns every sync_state key beginning with
	// prefix.
	ListSyncStateKeysByPrefix(prefix string, txn Txn) ([]string, error)
	// MithrilTrustBoundarySlotStrict is MithrilTrustBoundarySlot, but returns
	// the underlying read error instead of swallowing it as "no boundary
	// recorded" — for a caller that must fail closed (refuse the operation)
	// rather than fail open when the boundary can't be verified. A malformed
	// stored value is also propagated as an error here (unlike
	// MithrilTrustBoundarySlot, which still treats it as absent): a corrupted
	// persisted boundary must not be indistinguishable from "no snapshot was
	// ever imported" for a caller enforcing a safety check, or the check is
	// defeated exactly when it matters most.
	//
	// That includes a recorded empty value. GetSyncState reports an absent key
	// as the empty string, so an empty return alone cannot tell "no snapshot
	// was ever imported" from "a boundary row exists and holds nothing"; only
	// the second is malformed, and the two are separated here by asking the
	// sync_state keyspace whether the row exists at all. The extra query runs
	// only on that path.
	MithrilTrustBoundarySlotStrict(txn Txn) (uint64, error)
	// OldestRetainedSlot returns the slot of the oldest block whose body is
	// still stored, as last computed by RefreshOldestRetainedSlot. ok is false
	// before the first refresh, when history expiry has never run, when the
	// bound could not be found, and after the block at or below the bound was
	// expired or deleted, until the next refresh. It reads only memory.
	OldestRetainedSlot() (uint64, bool)
	// SetDeferredHeaderMarkerWithValue records an opaque marker payload with
	// the same durability and isolation as SetDeferredHeaderMarker. The
	// one-byte payload used by SetDeferredHeaderMarker remains the legacy
	// unattributed encoding.
	SetDeferredHeaderMarkerWithValue(key string, value []byte) error
	// SetMidEpochEraSwitch records a mid-epoch era switch so a rollback past
	// its block restores the switched epoch's era.
	SetMidEpochEraSwitch(sw database.MidEpochEraSwitch, txn Txn) error
	// SetSyncState stores or updates a sync state value.
	SetSyncState(key string, value string, txn Txn) error
	// SetTip saves the current tip
	SetTip(tip chainsync.Tip, txn Txn) error
	// SumTransactionFeesInSlotRange sums the fee-pot contributions in the
	// inclusive slot range: declared fees of valid transactions plus
	// consumed collateral of phase-2-invalid transactions.
	SumTransactionFeesInSlotRange(
		startSlot uint64,
		endSlot uint64,
		txn Txn,
	) (uint64, error)
	// TruncateAfterSlot reverts all metadata rows and blob-referenced UTxO/
	// transaction CBOR added strictly after point.Slot: certificates, account
	// reward deltas, account/pool/DRep delegation state, protocol parameters,
	// governance proposals/votes, constitutions, committee state, epochs,
	// reward state, block nonces, network state/donations, and UTxOs/
	// transactions. UTxOs spent after point.Slot are restored as unspent. It
	// then sets the tip to point and returns the resulting tip and block nonce.
	//
	// mithrilFloor floors the UTxO/transaction deletion slot at the Mithril
	// ledger boundary, if any (pass 0 if there is none). UTxOs produced by
	// gap blocks during Mithril bootstrap are written via
	// SetGapBlockTransaction without advancing the ledger tip, so their
	// added_slot values can be well above the persisted tip; deleting below
	// the Mithril boundary would bulk-delete every gap-block-produced UTxO,
	// leaving the chain unable to validate the first post-gap block that
	// consumes one of them. The Mithril snapshot is the trust anchor and is
	// never rewound past, so the authoritative deletion slot is point.Slot or
	// mithrilFloor, whichever is later.
	//
	// This is the shared metadata+blob truncation sweep used by both live
	// ledger rollback (ledger.LedgerState.rollback, bounded by the security
	// parameter) and offline/live database truncation (database/lifecycle,
	// which may go far deeper for CIP-0135 disaster recovery). It performs no
	// in-memory cache updates of its own — callers that hold additional
	// in-memory state (epoch cache, era, protocol parameters, chain tip)
	// must reload it themselves from the database after this returns
	// successfully.
	//
	// If txn is nil, a new read-write transaction is opened and committed
	// internally; otherwise the caller is responsible for committing txn.
	TruncateAfterSlot(
		ctx context.Context,
		point common.Point,
		mithrilFloor uint64,
		txn Txn,
	) (chainsync.Tip, []byte, error)
}

// Applier writes a block's transactions. Today it writes ledger state and API
// history together.
type Applier interface {
	// BeginDestructiveTransitionContext prevents coordinated read and lifecycle
	// snapshots from opening while a destructive update spans multiple physical
	// transactions. ctx can cancel a wait before the transition starts.
	BeginDestructiveTransitionContext(ctx context.Context) (func(), error)
	// BlockBatchTransactionContext starts a serialized coordinated block
	// transaction. ctx can cancel commit-barrier or inventory admission before
	// either backend transaction opens.
	BlockBatchTransactionContext(ctx context.Context) (Txn, error)
	// BlockBatchTxnContext starts a serialized blob-only block transaction. ctx
	// can cancel inventory admission before the backend transaction opens.
	BlockBatchTxnContext(ctx context.Context) (Txn, error)
	// EnableBatchUtxoOverlay attaches a batch UTxO overlay to acc for the rest
	// of txn when the metadata plugin supports one (see
	// metadata.BatchUtxoOverlay). It reports false, and changes nothing, when
	// it does not.
	EnableBatchUtxoOverlay(
		acc database.BatchAccumulator,
		txn Txn,
		inputs []models.UtxoId,
		deleteOnSpend []models.UtxoId,
		producers [][]byte,
	) (bool, error)
	// FlushBatch writes accumulated metadata rows for the active metadata
	// plugin.
	FlushBatch(acc database.BatchAccumulator, txn Txn) error
	// FlushBatchStakeDeltas applies pending live-stake changes while leaving
	// the accumulator's other batched rows queued for FlushBatch.
	FlushBatchStakeDeltas(acc database.BatchAccumulator, txn Txn) error
	// NewBatchAccumulator creates an accumulator for the configured metadata
	// plugin.
	NewBatchAccumulator() database.BatchAccumulator
	// NewValidatedBlockBatchAccumulator creates an API-detail accumulator whose
	// consensus state remains immediate. Unsupported providers return nil.
	NewValidatedBlockBatchAccumulator() database.BatchAccumulator
	// PersistTransactionOffsets writes the transaction offset and produced-UTxO
	// offset blob entries of txs from offsets, for a caller that then applies
	// the same transactions with BatchedTxIngestOpts.OffsetsPersisted.
	//
	// A dense Leios endorser block stages thousands of entries, more than one
	// Badger write transaction accepts, so the entries are committed in
	// blob-only transactions of their own, each before it fills (the same
	// budget-sized batching as the block inventory index rebuild). A store that
	// reports no per-transaction budget has none to exhaust and receives the
	// entries in txn's own blob transaction.
	//
	// Batches commit ahead of txn and are not undone by its rollback. That is
	// safe because every entry is keyed by transaction hash, metadata is the
	// source of truth for which transactions exist, and a re-apply of the same
	// transactions rewrites identical values. The offsets must point at a blob
	// already committed. Reads of the written keys through txn use a fresh
	// snapshot.
	//
	// afterBatch, when non-nil, runs after each batch commits; an error from it
	// stops the write. It is the seam for a crash between batches.
	PersistTransactionOffsets(
		txn Txn,
		txs []lcommon.Transaction,
		offsets *database.BlockIngestionResult,
		afterBatch func() error,
	) error
	// SetTransactionBatchedWithOpts is the option-aware form of
	// SetTransactionBatched. See BatchedTxIngestOpts for the available toggles.
	SetTransactionBatchedWithOpts(
		ctx context.Context,
		tx lcommon.Transaction,
		point common.Point,
		idx uint32,
		updateEpoch uint64,
		pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
		certDeposits map[int]uint64,
		offsets *database.BlockIngestionResult,
		acc database.BatchAccumulator,
		txn Txn,
		opts database.BatchedTxIngestOpts,
	) error
	// SetTransactionWithOpts is SetTransaction with control over UTxO ingest
	// behavior via opts. Certified Leios endorser-block closure application
	// passes SkipConsumedInputRecovery after validating the complete ordered
	// closure, so a transaction's effects are applied without the consumed-utxo
	// recovery/repair pass: produced outputs and input spends are written, but
	// a consumed input that is absent from the store is left as a no-op instead
	// of triggering blob recovery. This matches the reference ledger's
	// endorser-closure apply, which folds the closure's transactions onto the
	// ledger state without storage recovery.
	SetTransactionWithOpts(
		ctx context.Context,
		tx lcommon.Transaction,
		point common.Point,
		idx uint32,
		updateEpoch uint64,
		pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
		certDeposits map[int]uint64,
		offsets *database.BlockIngestionResult,
		txn Txn,
		opts database.BatchedTxIngestOpts,
	) error
}

// TxHistory is the set of consensus paths that still read transaction history.
// Each one is a dependency to remove before the LedgerDB can stand alone.
type TxHistory interface {
	// ResolveTxCbor returns the body CBOR of the stored transaction txHash.
	// It is the body only, not a complete standalone transaction.
	ResolveTxCbor(txn Txn, txHash []byte) ([]byte, error)
	// GetTransactionByHash returns the stored transaction with hash, or nil
	// when none is stored.
	GetTransactionByHash(
		ctx context.Context,
		hash []byte,
		txn Txn,
	) (*models.Transaction, error)
	// GetTransactionsByHashes returns transactions for the provided hashes.
	GetTransactionsByHashes(
		ctx context.Context,
		hashes [][]byte,
		txn Txn,
	) ([]models.Transaction, error)
	// ResolveUtxoCborWithRecovery resolves a UTxO's CBOR via the tiered cache,
	// falling back to reconstructing it from the producing block (the same
	// recovery loadCbor performs for UtxoByRef and IterateLiveUtxos) when the
	// blob has gone missing but is reconstructable. Returns
	// ErrUtxoCborUnavailable when recovery itself confirms the CBOR cannot be
	// reconstructed.
	//
	// Prefer this over CborCache().ResolveUtxoCbor for a caller that has only a
	// bare ref (not a *models.Utxo already loaded via GetUtxo) and must not
	// silently treat a recoverable missing blob as absent -- e.g.
	// ledger.queryShelleyUtxoWhole, which answers GetUTxOWhole and would
	// otherwise return an incomplete reply for a row IterateLiveUtxos'
	// loadCbor-based path would have recovered.
	//
	// txn may be blob-only (Metadata() == nil): the hot path above only ever
	// reaches the blob store (TieredCborCache.ResolveUtxoCbor), and a caller
	// resolving many refs concurrently (queryShelleyUtxoWhole's worker pool)
	// wants to avoid holding a metadata connection from the shared read pool
	// for the whole resolve phase when it is needed only on the rare recovery
	// branch below. Only recoverUtxoCbor's metadata-based fallback
	// (utxoRecoveryBlockForTx, once the blob-based lookup misses) actually
	// needs one, so a metadata-capable transaction is opened here, on demand,
	// just for that branch. This is an
	// explicit, self-contained guarantee: sqlstore.Store.GetTransactionByHash
	// (what that fallback ultimately calls) separately tolerates a nil
	// types.Txn by borrowing its own ad-hoc pooled connection for just that one
	// query, so passing txn.Metadata() straight through would also work today
	// -- but relying on that would make this function's own resource behavior
	// depend on an incidental property of a different package's nil-handling
	// rather than on something stated and tested here.
	ResolveUtxoCborWithRecovery(
		ctx context.Context,
		txId []byte,
		outputIdx uint32,
		txn Txn,
	) ([]byte, error)
}

// LeiosStore holds Leios endorser blocks. It belongs to the LeiosDB role and
// moves there with it.
type LeiosStore interface {
	// GetLeiosEBManifest retrieves the raw Leios endorser-block manifest CBOR
	// for the exact (slot, hash) occurrence named. Returns ErrBlobKeyNotFound
	// when no manifest has been stored for that occurrence -- including when a
	// manifest exists for the same hash under a different slot, since the
	// manifest is content-addressed and that is a distinct occurrence.
	//
	// On a miss it also tries the legacy key (hash only), so data persisted by
	// a node running before the key format changed does not become silently
	// unreachable after an upgrade: that format could only ever hold one
	// occurrence per hash, and its value carries that occurrence's slot as an
	// 8-byte big-endian prefix, which must match the requested slot before the
	// legacy record is trusted.
	GetLeiosEBManifest(hash []byte, slot uint64) ([]byte, error)
	// GetLeiosEBTxs retrieves the raw transaction bodies for the exact (slot,
	// hash) occurrence named. Returns ErrBlobKeyNotFound when no txs have been
	// stored for that occurrence. The returned slice holds the
	// block_transaction CBOR values exactly as SetLeiosEBTxs stored them.
	//
	// On a miss it also tries the legacy key (hash only, see
	// GetLeiosEBManifest), gated on the legacy "em" record's embedded slot
	// matching: the legacy format paired one "em" and one "et" record per hash
	// (only one occurrence was ever trackable), so once that pairing is
	// confirmed to be this occurrence, its "et" value is safe to use too.
	GetLeiosEBTxs(hash []byte, slot uint64) ([]cbor.RawMessage, error)
	// MaxLeiosEBSlot returns the highest slot represented by a persisted Leios
	// endorser-block manifest. Current records encode the slot in the key;
	// legacy records ("em"+hash with no slot) encode it in the first eight
	// bytes of the value.
	//
	// COST. The full prefix scan is inherent, not an oversight: the current key
	// layout is "em"+hash+slot, so keys sort by hash and no bounded reverse
	// seek can find the maximum slot. It runs at startup to restore the
	// watermark and hourly when GC computes its cutoff. On Badger it is
	// key-only -- iterator options leave PrefetchValues false and only the
	// legacy branch copies a value -- while S3 and GCS perform a paginated
	// listing over the prefix. Ordering keys by slot or maintaining the maximum
	// as its own record would make the scan bounded.
	MaxLeiosEBSlot() (uint64, error)
	// MaxLeiosEBSlotContext is MaxLeiosEBSlot with cancellation checks between
	// records. Blob iterator operations themselves are controlled by the
	// selected provider, but a canceled caller will not continue scanning later
	// records.
	MaxLeiosEBSlotContext(ctx context.Context) (uint64, error)
	// NewLeiosClosureBatchAccumulator creates an API-detail accumulator when
	// the metadata provider supports ordered closure application; nil selects
	// the existing immediate path.
	NewLeiosClosureBatchAccumulator() database.BatchAccumulator
	// PrepareLeiosClosureBatch prepares the next ordered closure window.
	// Providers without window support retain the complete-closure batching
	// behavior.
	PrepareLeiosClosureBatch(
		acc database.BatchAccumulator,
		txn Txn,
		transactions []lcommon.Transaction,
	) (int, error)
	// PruneLeiosEBBeforeSlot removes persisted Leios manifests and transaction
	// lists strictly older than beforeSlot. The caller chooses the retention
	// frontier; this method does not infer one from chain state.
	//
	// It scans the shared "e" prefix once because the manifest and transaction
	// prefixes are adjacent. Deletes commit in blob-only transactions
	// containing at most one key. Cloud stores spool prior object values for
	// compensation, so this avoids aggregating many large values into one local
	// spool. Repeating the same call is safe after a partial cloud commit. The
	// returned count includes only keys from fully committed transactions; on
	// error it is a lower bound.
	PruneLeiosEBBeforeSlot(ctx context.Context, beforeSlot uint64) (int, error)
	// SetLeiosEB persists an endorser block's manifest and, when txsRaw is
	// non-nil, its transaction bodies in a SINGLE blob-store transaction (one
	// commit), merging what SetLeiosEBManifest + SetLeiosEBTxs do in two, for
	// the exact (slot, hash) occurrence identified by slot and hash. The stored
	// values are byte-identical to those setters, so GetLeiosEBManifest /
	// GetLeiosEBTxs and the reload path are unchanged. Pass txsRaw==nil to
	// write only the manifest (an incomplete endorser block); pass the complete
	// tx set otherwise. Note the nil contract differs from SetLeiosEBTxs:
	// SetLeiosEBTxs(nil) writes an empty tx list under the "et" key, whereas
	// SetLeiosEB(..., nil) omits the "et" key entirely (manifest-only), so the
	// two must not be treated as interchangeable nil handlers. Used by the
	// asynchronous EB-persistence writer so historical-serving storage costs
	// one commit per endorser block off the leios-fetch hot path.
	SetLeiosEB(
		slot uint64,
		hash []byte,
		manifestRaw []byte,
		txsRaw []cbor.RawMessage,
	) error
	// SetLeiosEBCertified records that a ranking block certified the endorser
	// block occurrence (slot, hash). The marker is idempotent and, given the
	// block-apply transaction, commits with the certifying ranking block and is
	// discarded with it when a later check rejects the block. A nil txn commits
	// the marker in its own blob transaction.
	SetLeiosEBCertified(slot uint64, hash []byte, txn Txn) error
}

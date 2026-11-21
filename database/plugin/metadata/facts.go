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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// FactTable describes a table of ledger facts: rows the ledger derives at
// epoch boundaries and from governance and reward rules, which block replay
// alone cannot reproduce. A split IndexerDB receives them from the ledger's
// store by natural key, so a store's own row ids never cross between stores.
type FactTable struct {
	// Name is the SQL table name.
	Name string
	// SlotColumns lists the columns that date a row. A row is in a slot range
	// when any of them lies inside it.
	SlotColumns []string
	// Key lists the columns identifying a row across stores. They are never
	// row ids, and no key column is nullable.
	Key []string
	// Patch, when not empty, restricts the copy to those columns of a row the
	// indexer already holds under Key; the remaining columns are written from
	// blocks. A row the indexer does not hold is skipped. Empty means the
	// table is copied whole: an existing row with the same Key is replaced.
	Patch []string
	// AsOf lists groups of columns the ledger fills at a slot of their own.
	// A row read for a range ending at through returns a group's columns
	// null when that slot lies past through, so the copy shows the row as it
	// stood then and not as the ledger holds it now. Columns not named here
	// are as of the ledger's tip: nothing records when they changed.
	AsOf []AsOfColumns
	// Paged marks a table with a surrogate id column, used only as the read
	// cursor and never returned. A table without one is read in a single
	// call.
	Paged bool
	// FilterColumn and FilterValue, when FilterColumn is set, restrict reads
	// to rows whose column equals the value.
	FilterColumn string
	FilterValue  any
	// Companion, when set, marks a table whose rows hang off a row of
	// another table by its row id. Key then names columns of that parent:
	// reads return them joined in, and writes find the parent by them and
	// skip a row whose parent the store does not hold.
	Companion *Companion
}

// Companion describes how a FactTable references its parent table.
type Companion struct {
	// Parent is the parent table's name.
	Parent string
	// ParentID is this table's column holding the parent's row id. It is
	// also the table's primary key and the read cursor.
	ParentID string
	// Columns are the table's own columns, those it carries besides
	// ParentID.
	Columns []string
}

// AfterBlocks reports whether the table is copied after the segment's blocks
// rather than before: it patches, or hangs off, rows that block replay writes.
func (t FactTable) AfterBlocks() bool {
	return len(t.Patch) > 0 || t.Companion != nil
}

// AsOfColumns names columns that become non-null at the slot held in
// SlotColumn.
type AsOfColumns struct {
	SlotColumn string
	Columns    []string
}

// FactRows is a batch of fact rows: Columns names the values of each row.
type FactRows struct {
	Columns []string
	Rows    [][]any
	// LastID is the largest row id a Paged read returned, set even when the
	// read reached the end of the range and the returned cursor is zero.
	LastID int64
}

// FactRowReader is the read side of FactRowStore. A source other than a
// metadata store implements it to export fact rows: the ledger, once the
// live UTxO set holds them instead.
type FactRowReader interface {
	// ReadFactRows is FactRowStore.ReadFactRows.
	ReadFactRows(
		ctx context.Context,
		table FactTable,
		from, through uint64,
		cursor int64,
		limit int,
		txn types.Txn,
	) (rows FactRows, next int64, err error)
}

// PendingStakeInputsReader is the optional capability of a FactRowReader
// whose ledger records the snapshots it has not finished writing stake inputs
// for somewhere other than the metadata store's sync state.
type PendingStakeInputsReader interface {
	// RewardStakeInputsPendingState returns the pending record as the ledger
	// stores it, or the empty string when none exists.
	RewardStakeInputsPendingState() (string, error)
}

// RewardPrecomputeCursorReader is the optional capability of a FactRowReader
// whose ledger keeps the reward precompute cursors somewhere other than the
// metadata store's sync state.
type RewardPrecomputeCursorReader interface {
	// RewardPrecomputeCursors returns each retained cursor record as the
	// ledger stores it, by snapshot epoch.
	RewardPrecomputeCursors() (map[uint64]string, error)
}

// ErrFactCursorLost is returned by a fact read whose cursor names an export
// the reader no longer holds, as after a restart between pages. The caller
// starts the table again from the beginning.
var ErrFactCursorLost = errors.New("fact read cursor names no export in progress")

// CommitSyncer is an optional capability of a metadata store whose commits
// are not durable when they return. SyncCommits returns only once every commit
// that returned before it began survives a crash of the host. A store without
// it commits durably.
type CommitSyncer interface {
	SyncCommits(ctx context.Context) error
}

// FactRowStore copies fact rows by natural key. It is an optional capability
// of a metadata store: the split indexer requires it on both of its stores,
// which hold the same schema.
type FactRowStore interface {
	// ReadFactRows returns rows of table with a slot column in [from,
	// through], at most limit of them for a Paged table, after cursor (zero
	// to start). The returned cursor continues the read; it is zero when the
	// table has no more rows in range.
	ReadFactRows(
		ctx context.Context,
		table FactTable,
		from, through uint64,
		cursor int64,
		limit int,
		txn types.Txn,
	) (rows FactRows, next int64, err error)
	// WriteFactRows stores rows as the table's spec directs: replacing the
	// row under each Key, or patching the Patch columns of an existing one.
	WriteFactRows(
		table FactTable,
		rows FactRows,
		txn types.Txn,
	) error
	// DeleteFactRowsAfter removes rows of table dated after slot, for a
	// rollback. A Patch table is not touched: its rows are rolled back with
	// the block-derived state they sit on.
	DeleteFactRowsAfter(
		table FactTable,
		slot uint64,
		txn types.Txn,
	) error
}

// ImportedAccount is an account a snapshot import established at its anchor:
// its import baseline and the reward balance the ledger store held there.
type ImportedAccount struct {
	StakingKey    []byte
	Pool          []byte
	Drep          []byte
	DepositAmount *types.Uint64
	Reward        types.Uint64
	DrepType      uint64
	AddedSlot     uint64
	CredentialTag uint8
	Active        bool
}

// RewardJournalEntry is one account reward change journaled by the ledger.
type RewardJournalEntry struct {
	StakingKey    []byte
	Amount        uint64
	CredentialTag uint8
	// Withdrawal is true for a debit and false for a credit.
	Withdrawal bool
}

// OpCertCounter is the operational certificate counter a snapshot import
// established for a pool at its anchor.
type OpCertCounter struct {
	PoolKeyHash []byte
	Sequence    uint64
}

// UtxoSetDigest summarizes a live UTxO set: how many outputs it holds and a
// hash over them in (transaction hash, output index) order.
type UtxoSetDigest struct {
	Digest [32]byte
	Count  uint64
}

// UtxoOutputResolver returns the stored CBOR of an output. A digest that
// takes one covers what the metadata rows do not hold: assets, inline datums
// and reference scripts.
type UtxoOutputResolver func(txHash []byte, outputIdx uint32) ([]byte, error)

// AnchorStateStore is the optional capability a split IndexerDB uses to take
// over the ledger state a snapshot import established, and to check its own
// history against it.
type AnchorStateStore interface {
	// ReadImportedAccounts returns accounts whose import baseline was
	// established at or after anchor, ordered by account id, at most limit
	// of them after cursor (zero to start). Reward is the balance now. The
	// returned cursor continues the read and is zero at the end.
	ReadImportedAccounts(
		ctx context.Context,
		anchor uint64,
		cursor int64,
		limit int,
		txn types.Txn,
	) ([]ImportedAccount, int64, error)
	// ReadRewardJournalAfter returns the reward journal entries dated after
	// slot, ordered by id, at most limit after cursor (zero to start).
	ReadRewardJournalAfter(
		ctx context.Context,
		slot uint64,
		cursor int64,
		limit int,
		txn types.Txn,
	) ([]RewardJournalEntry, int64, error)
	// WriteImportedAccounts records the import baseline of each account and
	// sets the balance of an account the store already holds. An account it
	// does not hold is left absent.
	WriteImportedAccounts(accounts []ImportedAccount, txn types.Txn) error
	// LiveUtxoDigest digests the outputs created at or before atSlot and not
	// spent at or before it, streaming them in pages.
	LiveUtxoDigest(
		ctx context.Context,
		atSlot uint64,
		txn types.Txn,
	) (UtxoSetDigest, error)
	// LiveUtxoDigestWithOutputs is LiveUtxoDigest with each output's stored
	// CBOR, from resolve, folded into the digest.
	LiveUtxoDigestWithOutputs(
		ctx context.Context,
		atSlot uint64,
		resolve UtxoOutputResolver,
		txn types.Txn,
	) (UtxoSetDigest, error)
	// AppliedTransactionCount returns the number of applied transaction
	// rows.
	AppliedTransactionCount(ctx context.Context, txn types.Txn) (uint64, error)
	// ReadOpCertCountersAt returns the op-cert sequence recorded at exactly
	// slot for each pool that has one, ordered by pool.
	ReadOpCertCountersAt(
		ctx context.Context,
		slot uint64,
		txn types.Txn,
	) ([]OpCertCounter, error)
}

// AnchorOpCertSource is the optional capability of a FactRowReader that
// serves the op-cert counters a snapshot import established, for a ledger
// that no longer keeps them in its metadata store.
type AnchorOpCertSource interface {
	// AnchorOpCertCounters returns the counters recorded at the anchor slot.
	AnchorOpCertCounters(
		ctx context.Context,
		anchor uint64,
	) ([]OpCertCounter, error)
}

// FactTables lists the ledger-fact tables a split IndexerDB receives. Rollback
// of the ones whose block-derived counterpart the rollback sweep already
// removes is covered there as well; deleting again is harmless.
var FactTables = []FactTable{
	{
		Name:        "epoch",
		SlotColumns: []string{"start_slot"},
		Key:         []string{"epoch_id"},
		Paged:       true,
	},
	{
		Name:        "epoch_summary",
		SlotColumns: []string{"boundary_slot"},
		Key:         []string{"epoch"},
		Paged:       true,
	},
	{
		Name: "pool_stake_snapshot", SlotColumns: []string{"captured_slot"},
		Key:   []string{"epoch", "snapshot_type", "pool_key_hash"},
		Paged: true,
	},
	{
		Name:        "reward_ada_pots",
		SlotColumns: []string{"captured_slot"},
		Key:         []string{"epoch"},
		Paged:       true,
	},
	{
		Name: "reward_snapshot", SlotColumns: []string{"captured_slot"},
		Key: []string{"epoch", "snapshot_type"}, Paged: true,
	},
	{
		Name: "reward_pool_input", SlotColumns: []string{"captured_slot"},
		Key: []string{"epoch", "pool_key_hash"}, Paged: true,
	},
	{
		Name: "reward_stake_input", SlotColumns: []string{"captured_slot"},
		Key: []string{
			"epoch",
			"pool_key_hash",
			"credential_tag",
			"staking_key",
		},
		Paged: true,
	},
	{
		Name: "reward_pool_output", SlotColumns: []string{"captured_slot"},
		Key: []string{"epoch", "pool_key_hash"}, Paged: true,
	},
	{
		Name: "reward_account_output", SlotColumns: []string{"captured_slot"},
		Key: []string{
			"epoch", "pool_key_hash", "credential_tag", "staking_key",
			"reward_type",
		},
		Paged: true,
	},
	{
		Name: "reward_seed_failure", SlotColumns: []string{"captured_slot"},
		Key: []string{"epoch", "snapshot_type"},
	},
	{
		Name: "imported_pool_block_count", SlotColumns: []string{"captured_slot"},
		Key: []string{"epoch", "pool_key_hash"},
	},
	{
		Name: "imported_epoch_block_total", SlotColumns: []string{"captured_slot"},
		Key: []string{"epoch"},
	},
	{
		Name:        "reward_credit_round",
		SlotColumns: []string{"boundary_slot"},
		Key:         []string{"snapshot_epoch"},
	},
	{
		Name:        "reward_likelihood_state",
		SlotColumns: []string{"boundary_slot"},
		Key:         []string{"epoch"},
		Paged:       true,
	},
	{
		Name:        "reward_pool_likelihood",
		SlotColumns: []string{"boundary_slot"},
		Key:         []string{"epoch", "pool_key_hash"},
		Paged:       true,
	},
	{
		Name:        "network_state",
		SlotColumns: []string{"slot"},
		Key:         []string{"slot"},
		Paged:       true,
	},
	{
		Name:        "network_donation",
		SlotColumns: []string{"slot"},
		Key:         []string{"slot"},
		Paged:       true,
	},
	{
		Name: "pparams", SlotColumns: []string{"added_slot"},
		Key: []string{"epoch", "era_id"}, Paged: true,
	},
	{
		Name: "drep_expiry_history", SlotColumns: []string{"added_slot"},
		Key:   []string{"credential_tag", "credential", "added_slot"},
		Paged: true,
	},
	{
		// A dormant expiry bump and a dormancy reset can fall in one slot, and
		// the counter before each differs.
		Name: "drep_dormancy_history", SlotColumns: []string{"added_slot"},
		Key: []string{"added_slot", "previous_dormant_epochs"}, Paged: true,
	},
	{
		Name:        "drep_expiry_epoch_event",
		SlotColumns: []string{"added_slot"},
		Key:         []string{"added_slot"},
	},
	{
		Name: "account_drep_clear", SlotColumns: []string{"added_slot"},
		Key: []string{"credential_tag", "staking_key", "added_slot"},
	},
	{
		Name: "governance_proposal",
		SlotColumns: []string{
			"ratified_slot", "enacted_slot", "expired_slot", "deleted_slot",
		},
		Key: []string{"tx_hash", "action_index"},
		Patch: []string{
			"ratified_epoch", "ratified_slot", "enacted_epoch", "enacted_slot",
			"expired_epoch", "expired_slot", "deleted_slot",
		},
		AsOf: []AsOfColumns{
			{
				SlotColumn: "ratified_slot",
				Columns:    []string{"ratified_epoch", "ratified_slot"},
			},
			{
				SlotColumn: "enacted_slot",
				Columns:    []string{"enacted_epoch", "enacted_slot"},
			},
			{
				SlotColumn: "expired_slot",
				Columns:    []string{"expired_epoch", "expired_slot"},
			},
			{SlotColumn: "deleted_slot", Columns: []string{"deleted_slot"}},
		},
		Paged: true,
	},
	{
		// The ledger drops an expired proposal one epoch after marking it
		// expired, and records when in this companion row.
		Name:        "governance_proposal_drop",
		SlotColumns: []string{"dropped_slot"},
		Key:         []string{"tx_hash", "action_index"},
		Companion: &Companion{
			Parent:   "governance_proposal",
			ParentID: "proposal_id",
			Columns:  []string{"dropped_epoch", "dropped_slot"},
		},
		Paged: true,
	},
	{
		Name: "account", SlotColumns: []string{"added_slot"},
		Key: []string{"credential_tag", "staking_key"},
		Patch: []string{
			"pool", "drep", "drep_type", "active", "expiration_epoch",
			"added_slot",
		},
		Paged: true,
	},
	{
		Name: "drep", SlotColumns: []string{"added_slot"},
		Key: []string{"credential_tag", "credential"},
		Patch: []string{
			"active", "added_slot",
		},
		Paged: true,
	},
}

// DrepExpiryAsOfStore is the optional capability that sets each DRep's
// activity and expiry epochs from its expiry history as of a slot. The
// ledger renews them in place without moving the row's own slot, so the row
// cannot be copied as of an earlier slot; the history, which is, can.
type DrepExpiryAsOfStore interface {
	// ApplyDrepExpiryAsOf sets last_activity_epoch and expiry_epoch of every
	// DRep that has expiry history at or before slot to its latest entry
	// there. A DRep without history keeps what its row holds.
	ApplyDrepExpiryAsOf(slot uint64, txn types.Txn) error
}

// AccountRewardCreditTable is the reward-credit journal. It is not copied as
// rows: each credit is applied through AddAccountRewardsByCredential, which
// also moves the account balance and is idempotent per credit.
var AccountRewardCreditTable = FactTable{
	Name: "account_reward_delta", SlotColumns: []string{"added_slot"},
	Key: []string{
		"withdrawal",
		"tx_hash",
		"credential_tag",
		"staking_key",
		"added_slot",
	},
	Paged:        true,
	FilterColumn: "withdrawal", FilterValue: false,
}

// RewardFoldStore is the optional capability a split IndexerDB uses to keep
// its reward outputs consistent with the reward credits it copies. The
// ledger folds a credited round lazily, journaling each credit under the
// round's boundary slot only when its credential is next touched, so a
// credit can appear after the copy has passed that slot.
type RewardFoldStore interface {
	// UnfoldedRewardCreditRounds returns the credited rounds that still hold
	// an unfolded spendable, unguarded output, ordered by epoch.
	UnfoldedRewardCreditRounds(
		ctx context.Context,
		txn types.Txn,
	) ([]models.RewardCreditRound, error)
	// FoldRewardOutputsForCredits marks as folded each unfolded output whose
	// stake-reward credit is in credits, so a balance read no longer counts
	// it as pending once the credit is part of the account.
	FoldRewardOutputsForCredits(
		credits []models.AccountRewardCredit,
		txn types.Txn,
	) error
}

// ErrAnchorDigestUnavailable is returned by AnchorStateSource.AnchorUtxoDigest
// when the ledger no longer holds the UTxO set as it was at the anchor.
var ErrAnchorDigestUnavailable = errors.New(
	"the ledger no longer holds the UTxO set at the anchor",
)

// AnchorStateSource is the optional capability of a FactRowReader that serves
// the state a snapshot import established, for a ledger that drained it from
// its metadata store into a live UTxO set. The accounts and reward journal
// come from the set's records and the UTxO set at the anchor from its
// journal; a ledger that kept them in the metadata store serves them from
// there.
type AnchorStateSource interface {
	// AnchorStateReady returns nil when the imported accounts and the reward
	// journal after the anchor can be served, and the reason they cannot
	// otherwise.
	AnchorStateReady(ctx context.Context) error
	// AnchorImportedAccounts is AnchorStateStore.ReadImportedAccounts with an
	// opaque cursor: nil to start, nil returned at the end.
	AnchorImportedAccounts(
		ctx context.Context,
		anchor uint64,
		cursor []byte,
		limit int,
	) ([]ImportedAccount, []byte, error)
	// EachAnchorRewardJournal calls fn with each reward journal entry dated
	// after slot.
	EachAnchorRewardJournal(
		ctx context.Context,
		slot uint64,
		fn func(RewardJournalEntry) error,
	) error
	// AnchorUtxoDigest digests the UTxO set at the anchor, with each output's
	// CBOR, and wraps ErrAnchorDigestUnavailable when the set cannot rebuild
	// the anchor.
	AnchorUtxoDigest(ctx context.Context, anchor uint64) (UtxoSetDigest, error)
	// AnchorUtxoInMetadata reports whether the ledger's metadata store, not a
	// live UTxO set, holds the UTxO set a digest is taken of. It fails when
	// that cannot be told, rather than answer for a set it could not read.
	AnchorUtxoInMetadata(anchor uint64) (bool, error)
}

// AnchorStateViewer is an optional capability of an AnchorStateSource whose
// state moves while the writer reads it. AnchorStateView returns a source
// whose every read sees the ledger at one point, and the function that ends
// it. A balance is the account's balance now less the journal after the
// anchor, so the two have to be read at the same point: a credit or
// withdrawal landing between them is in the balance and not in the journal.
type AnchorStateViewer interface {
	AnchorStateView(ctx context.Context) (AnchorStateSource, func(), error)
}

// UtxoDigester accumulates a UtxoSetDigest over outputs fed in (transaction
// hash, output index) order. Every store digests through it, so the digests
// of two stores that hold the same outputs are equal.
type UtxoDigester struct {
	h     hash.Hash
	count uint64
}

// NewUtxoDigester returns an empty digester.
func NewUtxoDigester() *UtxoDigester {
	return &UtxoDigester{h: sha256.New()}
}

// Add folds in one output: its transaction hash, index, lovelace as a decimal
// string, payment and staking credentials and datum hash, and, when withCbor,
// its stored CBOR.
func (d *UtxoDigester) Add(
	txHash []byte,
	index uint32,
	amount string,
	payment, staking, datum []byte,
	withCbor bool,
	cbor []byte,
) {
	var buf [8]byte
	write := func(field []byte) {
		binary.BigEndian.PutUint64(buf[:], uint64(len(field)))
		d.h.Write(buf[:])
		d.h.Write(field)
	}
	for _, field := range [][]byte{
		txHash, []byte(amount), payment, staking, datum,
	} {
		write(field)
	}
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	d.h.Write(buf[:])
	if withCbor {
		write(cbor)
	}
	d.count++
}

// Sum returns the digest of the outputs added so far.
func (d *UtxoDigester) Sum() UtxoSetDigest {
	var out UtxoSetDigest
	out.Count = d.count
	copy(out.Digest[:], d.h.Sum(nil))
	return out
}

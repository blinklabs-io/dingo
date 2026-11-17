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

// Package indexerdb is the IndexerDB: chain history and the address and asset
// projections that serve the APIs. Validation never consults it. It follows
// the ledger on its own schedule and records its own tip, committed in the
// same transaction as the rows that tip covers.
package indexerdb

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TipSyncKey is the sync-state key holding the IndexerDB tip. It lives in the
// IndexerDB's own metadata store, so it is written by the same SQL
// transaction as the rows it covers.
const TipSyncKey = "indexer_tip"

// Tip is the last chain position whose rows the IndexerDB holds.
type Tip struct {
	Hash        []byte
	Slot        uint64
	BlockNumber uint64
}

// IsZero reports whether no block has been indexed.
func (t Tip) IsZero() bool {
	return len(t.Hash) == 0 && t.Slot == 0 && t.BlockNumber == 0
}

// encode renders a tip as "slot:blockNumber:hash".
func (t Tip) encode() string {
	return strconv.FormatUint(t.Slot, 10) + ":" +
		strconv.FormatUint(t.BlockNumber, 10) + ":" +
		hex.EncodeToString(t.Hash)
}

func decodeTip(s string) (Tip, error) {
	if s == "" {
		return Tip{}, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return Tip{}, fmt.Errorf("malformed indexer tip %q", s)
	}
	// Slots and block numbers are int64 columns in every SQL backend and
	// in the API responses built from the tip, so a value above that range is
	// malformed rather than a real position.
	slot, err := strconv.ParseUint(parts[0], 10, 63)
	if err != nil {
		return Tip{}, fmt.Errorf("malformed indexer tip slot: %w", err)
	}
	number, err := strconv.ParseUint(parts[1], 10, 63)
	if err != nil {
		return Tip{}, fmt.Errorf("malformed indexer tip block number: %w", err)
	}
	hash, err := hex.DecodeString(parts[2])
	if err != nil {
		return Tip{}, fmt.Errorf("malformed indexer tip hash: %w", err)
	}
	return Tip{Slot: slot, BlockNumber: number, Hash: hash}, nil
}

// AnchorSyncKey is the sync-state key holding the Mithril anchor the
// IndexerDB's history must reach before it serves. It is absent for a chain
// that starts at genesis.
const AnchorSyncKey = "indexer_anchor"

// BootstrapSyncKey is the sync-state key holding the phase of the replay that
// builds the IndexerDB up to the anchor.
const BootstrapSyncKey = "indexer_bootstrap"

// ReplayTransactionsSyncKey is the sync-state key holding how many
// transactions the replay has indexed, for the row-count check at the anchor.
const ReplayTransactionsSyncKey = "indexer_replay_txs"

// BootstrapPhase is where the replay to a Mithril anchor stands.
type BootstrapPhase string

const (
	// PhaseNone is no bootstrap: a chain from genesis, or one adopted from a
	// store that already held its history.
	PhaseNone BootstrapPhase = ""
	// PhaseReplay replays blocks from the chain's origin to the anchor with
	// the deferred indexes dropped.
	PhaseReplay BootstrapPhase = "replay"
	// PhaseIndex builds the dropped indexes, then verifies the rows at the
	// anchor and applies the imported account state.
	PhaseIndex BootstrapPhase = "index"
	// PhaseDone marks the anchor reached, built and verified.
	PhaseDone BootstrapPhase = "done"
)

// StateStore is the key-value surface a Store keeps its tip in.
// *database.Database implements it.
type StateStore interface {
	GetSyncState(key string, txn *database.Txn) (string, error)
	SetSyncState(key, value string, txn *database.Txn) error
	DeleteSyncState(key string, txn *database.Txn) error
	ListSyncStateKeysByPrefix(
		prefix string,
		txn *database.Txn,
	) ([]string, error)
}

// checkpointPrefix prefixes the sync-state keys of the tip checkpoints. The
// block number is zero-padded so keys sort in chain order.
const checkpointPrefix = "indexer_cp:"

// CheckpointRetainBlocks bounds how far below the tip checkpoints are kept.
// It exceeds any rollback the ledger accepts.
const CheckpointRetainBlocks uint64 = 20_000

func checkpointKey(t Tip) string {
	return checkpointPrefix + fmt.Sprintf("%020d", t.BlockNumber)
}

// AddCheckpoint records tip as a point the IndexerDB can roll back to and
// drops checkpoints more than CheckpointRetainBlocks below it. Call it in the
// transaction that sets the tip. Checkpoints exist because the block bodies
// of an abandoned fork are gone by the time the writer notices the switch, so
// its tip cannot be walked back through parent links; the newest checkpoint
// still on the chain is a safe, if slightly early, rollback target.
func (s *Store) AddCheckpoint(tip Tip, txn *database.Txn) error {
	if txn == nil {
		return errors.New(
			"indexerdb: AddCheckpoint needs the writer's transaction",
		)
	}
	if err := s.state.SetSyncState(checkpointKey(tip), tip.encode(), txn); err != nil {
		return err
	}
	// The oldest checkpoint is kept beside them so that a batch which prunes
	// nothing does not list every checkpoint: a listing per batch grows with
	// the retention window.
	oldest, known, err := s.oldestCheckpoint(txn)
	if err != nil {
		return err
	}
	if known && tip.BlockNumber < oldest {
		oldest = tip.BlockNumber
	}
	if known && oldest+CheckpointRetainBlocks >= tip.BlockNumber {
		return s.setOldestCheckpoint(oldest, txn)
	}
	keys, err := s.state.ListSyncStateKeysByPrefix(checkpointPrefix, txn)
	if err != nil {
		return err
	}
	remaining := tip.BlockNumber
	for _, k := range keys {
		var n uint64
		if _, err := fmt.Sscanf(strings.TrimPrefix(k, checkpointPrefix), "%d", &n); err != nil {
			continue
		}
		if n+CheckpointRetainBlocks < tip.BlockNumber {
			if err := s.state.DeleteSyncState(k, txn); err != nil {
				return err
			}
			continue
		}
		remaining = min(remaining, n)
	}
	return s.setOldestCheckpoint(remaining, txn)
}

// checkpointOldestKey holds the block number of the oldest checkpoint. Its
// prefix differs from checkpointPrefix, so a checkpoint listing never returns
// it.
const checkpointOldestKey = "indexer_cp_oldest"

func (s *Store) oldestCheckpoint(
	txn *database.Txn,
) (uint64, bool, error) {
	v, err := s.state.GetSyncState(checkpointOldestKey, txn)
	if err != nil || v == "" {
		return 0, false, err
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		// A malformed marker is recomputed from a listing.
		return 0, false, nil //nolint:nilerr
	}
	return n, true, nil
}

func (s *Store) setOldestCheckpoint(n uint64, txn *database.Txn) error {
	return s.state.SetSyncState(
		checkpointOldestKey, strconv.FormatUint(n, 10), txn,
	)
}

// Checkpoints returns the recorded checkpoints, newest first.
func (s *Store) Checkpoints(txn *database.Txn) ([]Tip, error) {
	keys, err := s.state.ListSyncStateKeysByPrefix(checkpointPrefix, txn)
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	out := make([]Tip, 0, len(keys))
	for _, k := range keys {
		v, err := s.state.GetSyncState(k, txn)
		if err != nil {
			return nil, err
		}
		tip, err := decodeTip(v)
		if err != nil {
			return nil, err
		}
		out = append(out, tip)
	}
	return out, nil
}

// DropCheckpointsAbove removes checkpoints whose block number exceeds
// blockNumber, for a rollback to that block.
func (s *Store) DropCheckpointsAbove(
	blockNumber uint64,
	txn *database.Txn,
) error {
	keys, err := s.state.ListSyncStateKeysByPrefix(checkpointPrefix, txn)
	if err != nil {
		return err
	}
	// The next AddCheckpoint recomputes the oldest checkpoint.
	if err := s.state.DeleteSyncState(checkpointOldestKey, txn); err != nil {
		return err
	}
	for _, k := range keys {
		var n uint64
		if _, err := fmt.Sscanf(strings.TrimPrefix(k, checkpointPrefix), "%d", &n); err != nil {
			continue
		}
		if n > blockNumber {
			if err := s.state.DeleteSyncState(k, txn); err != nil {
				return err
			}
		}
	}
	return nil
}

// Anchor returns the recorded Mithril anchor, or the zero Tip when the chain
// has none.
func (s *Store) Anchor(txn *database.Txn) (Tip, error) {
	v, err := s.state.GetSyncState(AnchorSyncKey, txn)
	if err != nil {
		return Tip{}, err
	}
	return decodeTip(v)
}

// SetAnchor records the anchor. Call it in the transaction that starts the
// replay.
func (s *Store) SetAnchor(anchor Tip, txn *database.Txn) error {
	if txn == nil {
		return errors.New("indexerdb: SetAnchor needs the writer's transaction")
	}
	return s.state.SetSyncState(AnchorSyncKey, anchor.encode(), txn)
}

// TrustBoundarySyncKey is the sync-state key that dates the Mithril trust
// boundary in every store that serves block counts. Counts raise their start
// past it, so the op-cert counters a snapshot import records at the anchor are
// never counted as blocks.
const TrustBoundarySyncKey = "mithril_ledger_slot"

// SetTrustBoundary records the anchor slot as the trust boundary, inside the
// writer's transaction.
func (s *Store) SetTrustBoundary(slot uint64, txn *database.Txn) error {
	if txn == nil {
		return errors.New(
			"indexerdb: SetTrustBoundary needs the writer's transaction",
		)
	}
	return s.state.SetSyncState(
		TrustBoundarySyncKey, strconv.FormatUint(slot, 10), txn,
	)
}

// AnchorDigestSyncKey is the sync-state key holding the digest of the ledger's
// live UTxO set at the anchor, captured when the anchor was recorded.
const AnchorDigestSyncKey = "indexer_anchor_digest"

// AnchorDigest returns the recorded digest of the ledger's live UTxO set at
// the anchor. found is false when none was recorded.
func (s *Store) AnchorDigest(
	txn *database.Txn,
) (count uint64, digest [32]byte, found bool, err error) {
	v, err := s.state.GetSyncState(AnchorDigestSyncKey, txn)
	if err != nil {
		return 0, digest, false, err
	}
	if v == "" {
		return 0, digest, false, nil
	}
	countText, digestText, ok := strings.Cut(v, ":")
	if ok {
		raw, decodeErr := hex.DecodeString(digestText)
		count, err = strconv.ParseUint(countText, 10, 64)
		if decodeErr == nil && err == nil && len(raw) == len(digest) {
			copy(digest[:], raw)
			return count, digest, true, nil
		}
	}
	return 0, digest, false, fmt.Errorf(
		"malformed indexer anchor digest %q", v,
	)
}

// SetAnchorDigest records the digest of the ledger's live UTxO set at the
// anchor. Call it in the transaction that records the anchor.
func (s *Store) SetAnchorDigest(
	count uint64,
	digest [32]byte,
	txn *database.Txn,
) error {
	if txn == nil {
		return errors.New(
			"indexerdb: SetAnchorDigest needs the writer's transaction",
		)
	}
	return s.state.SetSyncState(
		AnchorDigestSyncKey,
		strconv.FormatUint(count, 10)+":"+hex.EncodeToString(digest[:]),
		txn,
	)
}

// Bootstrap returns the phase of the replay to the anchor.
func (s *Store) Bootstrap(txn *database.Txn) (BootstrapPhase, error) {
	v, err := s.state.GetSyncState(BootstrapSyncKey, txn)
	if err != nil {
		return PhaseNone, err
	}
	switch p := BootstrapPhase(v); p {
	case PhaseNone, PhaseReplay, PhaseIndex, PhaseDone:
		return p, nil
	}
	return PhaseNone, fmt.Errorf("malformed indexer bootstrap phase %q", v)
}

// SetBootstrap records the phase. Call it in the transaction of the work that
// completes the previous phase, so a crash resumes in the phase that still
// has work to do.
func (s *Store) SetBootstrap(phase BootstrapPhase, txn *database.Txn) error {
	if txn == nil {
		return errors.New(
			"indexerdb: SetBootstrap needs the writer's transaction",
		)
	}
	return s.state.SetSyncState(BootstrapSyncKey, string(phase), txn)
}

// ReplayTransactions returns how many transactions the replay has indexed.
func (s *Store) ReplayTransactions(txn *database.Txn) (uint64, error) {
	v, err := s.state.GetSyncState(ReplayTransactionsSyncKey, txn)
	if err != nil || v == "" {
		return 0, err
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed indexer replay transactions: %w", err)
	}
	return n, nil
}

// AddReplayTransactions adds n to the replay's transaction count inside txn.
func (s *Store) AddReplayTransactions(n uint64, txn *database.Txn) error {
	if txn == nil {
		return errors.New(
			"indexerdb: AddReplayTransactions needs the writer's transaction",
		)
	}
	cur, err := s.ReplayTransactions(txn)
	if err != nil {
		return err
	}
	return s.state.SetSyncState(
		ReplayTransactionsSyncKey, strconv.FormatUint(cur+n, 10), txn,
	)
}

// Kind classifies what an API endpoint reads, for readiness.
type Kind int

const (
	// KindCurrent endpoints read current state (balances, UTxOs, pool
	// state) and need the IndexerDB close to the ledger tip.
	KindCurrent Kind = iota
	// KindHistory endpoints read immutable history and need only the
	// IndexerDB to have passed the Mithril anchor.
	KindHistory
)

// Store is the IndexerDB's handle: its database, its tip, and readiness.
type Store struct {
	db    *database.Database
	state StateStore
	// maxReadyLagBlocks bounds how far behind the ledger the IndexerDB may be
	// while KindCurrent endpoints are ready.
	maxReadyLagBlocks uint64
}

// NewStore wraps db, which must be the IndexerDB's own database. State is
// where the tip is kept; pass db itself outside tests.
func NewStore(
	db *database.Database,
	state StateStore,
	maxReadyLagBlocks uint64,
) (*Store, error) {
	if state == nil {
		return nil, errors.New("indexerdb: state store is required")
	}
	return &Store{
		db:                db,
		state:             state,
		maxReadyLagBlocks: maxReadyLagBlocks,
	}, nil
}

// Database returns the IndexerDB's database.
func (s *Store) Database() *database.Database { return s.db }

// Tip returns the recorded tip, or the zero Tip before any block is indexed.
// Pass the writer's transaction to read inside it.
func (s *Store) Tip(txn *database.Txn) (Tip, error) {
	v, err := s.state.GetSyncState(TipSyncKey, txn)
	if err != nil {
		return Tip{}, err
	}
	return decodeTip(v)
}

// SetTip records tip. Call it inside the transaction that writes the rows tip
// covers: a tip committed separately would let a crash leave rows the tip
// does not account for, or a tip ahead of its rows.
func (s *Store) SetTip(tip Tip, txn *database.Txn) error {
	if txn == nil {
		return errors.New("indexerdb: SetTip needs the writer's transaction")
	}
	if err := s.state.SetSyncState(TipSyncKey, tip.encode(), txn); err != nil {
		return err
	}
	// The metadata store's own tip row is what its SQL readers (the active
	// pool set, live stake, the chain tip a block list ends at) consult. In
	// an IndexerDB the writer is its only writer; in the ledger's database
	// that row is the ledger's tip and is not ours to move.
	if s.db != nil && s.db.IsIndexer() {
		return s.db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(tip.Slot, tip.Hash),
				BlockNumber: tip.BlockNumber,
			},
			txn,
		)
	}
	return nil
}

// Ready reports whether endpoints of kind may answer. ledgerBlockNumber is the
// ledger's durable tip block number; anchorBlockNumber is the Mithril anchor's
// block number, or zero when the node did not bootstrap from Mithril.
func (s *Store) Ready(
	kind Kind,
	ledgerBlockNumber, anchorBlockNumber uint64,
) (bool, error) {
	tip, err := s.Tip(nil)
	if err != nil {
		return false, err
	}
	return readyFor(
		kind, tip.BlockNumber, ledgerBlockNumber,
		anchorBlockNumber, s.maxReadyLagBlocks,
	), nil
}

// ReadyServing is Ready for the recorded anchor: not ready until the replay to
// the anchor has finished and the tip has passed it.
func (s *Store) ReadyServing(
	kind Kind,
	ledgerBlockNumber uint64,
) (bool, error) {
	phase, err := s.Bootstrap(nil)
	if err != nil {
		return false, err
	}
	if phase == PhaseReplay || phase == PhaseIndex {
		return false, nil
	}
	anchor, err := s.Anchor(nil)
	if err != nil {
		return false, err
	}
	return s.Ready(kind, ledgerBlockNumber, anchor.BlockNumber)
}

func readyFor(
	kind Kind,
	indexed, ledger, anchor, maxLag uint64,
) bool {
	if indexed < anchor {
		return false
	}
	if kind == KindHistory {
		return true
	}
	return ledger <= indexed || ledger-indexed <= maxLag
}

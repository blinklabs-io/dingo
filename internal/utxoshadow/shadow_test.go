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

package utxoshadow

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type stubTxn struct{}

func (stubTxn) Commit() error           { return nil }
func (stubTxn) Rollback() error         { return nil }
func (stubTxn) Release()                {}
func (stubTxn) AfterCommit(func())      {}
func (stubTxn) OnFinish(func())         {}
func (stubTxn) SavePoint(string) error  { return nil }
func (stubTxn) RollbackTo(string) error { return nil }
func (stubTxn) IsReadWrite() bool       { return false }
func (stubTxn) IsCommitted() bool       { return false }

// stubLedgerDB serves only what the shadow reads: a tip, a live set and
// as-of lookups. Any other method panics through the nil embedded interface.
type stubLedgerDB struct {
	ledgerdb.LedgerDB
	tip  ocommon.Point
	live map[badgerutxo.Ref][]byte
	// asOf records the slot of every as-of lookup.
	asOf []uint64
}

func (s *stubLedgerDB) Begin(context.Context, bool) ledgerdb.Txn {
	return stubTxn{}
}

func (s *stubLedgerDB) GetTip(ledgerdb.Txn) (chainsync.Tip, error) {
	return chainsync.Tip{Point: s.tip}, nil
}

func (s *stubLedgerDB) IterateLiveUtxos(
	_ context.Context,
	_ ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	for r, c := range s.live {
		if err := fn(&models.Utxo{
			TxId: r[:32], OutputIdx: refIndex(r), Cbor: c,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *stubLedgerDB) UtxosByRefsAsOf(
	_ context.Context,
	refs []models.UtxoId,
	atSlot uint64,
	_ ledgerdb.Txn,
) ([]models.Utxo, error) {
	s.asOf = append(s.asOf, atSlot)
	var out []models.Utxo
	for _, r := range refs {
		if c, ok := s.live[badgerutxo.NewRef(r.Hash, r.Idx)]; ok {
			out = append(
				out,
				models.Utxo{TxId: r.Hash, OutputIdx: r.Idx, Cbor: c},
			)
		}
	}
	return out, nil
}

func pt(slot uint64) ocommon.Point {
	return ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{byte(slot)}, 32)}
}

func id(b byte, idx uint32) models.UtxoId {
	return models.UtxoId{Hash: bytes.Repeat([]byte{b}, 32), Idx: idx}
}

func ref(b byte, idx uint32) badgerutxo.Ref {
	return badgerutxo.NewRef(bytes.Repeat([]byte{b}, 32), idx)
}

func output(b byte, idx uint32) ledger.UtxoOutput {
	return ledger.UtxoOutput{Ref: id(b, idx), Cbor: []byte{b, byte(idx)}}
}

func newTestShadow(t *testing.T, ldb *stubLedgerDB) *Shadow {
	t.Helper()
	s, err := Start(Config{
		Dir:         t.TempDir(),
		LedgerDB:    ldb,
		EventBus:    event.NewEventBus(nil, nil),
		SampleEvery: 1,
		SampleSize:  8,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Stop() })
	return s
}

func send(s *Shadow, ue ledger.UtxoEvent) {
	s.handle(event.Event{
		Timestamp: time.Now(),
		Type:      ledger.UtxoEventType,
		Data:      ue,
	})
}

func txChange(
	slot uint64,
	added []ledger.UtxoOutput,
	removed ...models.UtxoId,
) ledger.UtxoEvent {
	return ledger.UtxoEvent{
		Point:   pt(slot),
		Cause:   ledger.UtxoChangeTransaction,
		Added:   added,
		Removed: removed,
	}
}

func requireLive(t *testing.T, s *Shadow, live bool, refs ...badgerutxo.Ref) {
	t.Helper()
	for _, r := range refs {
		_, ok, err := s.store.Get(r)
		require.NoError(t, err)
		require.Equal(t, live, ok, refString(r))
	}
}

func TestShadowBootstrapsSkipsCoveredChangesAndFollows(t *testing.T) {
	t.Parallel()
	ldb := &stubLedgerDB{
		tip:  pt(20),
		live: map[badgerutxo.Ref][]byte{ref(1, 0): {1, 0}},
	}
	s := newTestShadow(t, ldb)

	// Covered by the bootstrap at slot 20: skipped.
	send(s, txChange(10, []ledger.UtxoOutput{output(9, 0)}))
	requireLive(t, s, false, ref(9, 0))
	require.Zero(t, testutil.ToFloat64(s.m.applied))

	send(s, txChange(30, []ledger.UtxoOutput{output(2, 0)}, id(1, 0)))
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, true, ref(2, 0))
	require.InDelta(t, 1, testutil.ToFloat64(s.m.applied), 0)
	require.InDelta(t, 1, testutil.ToFloat64(s.m.bootstraps), 0)
	require.Zero(t, testutil.ToFloat64(s.m.missing))
}

func TestShadowCountsMissingAndDuplicateOutputs(t *testing.T) {
	t.Parallel()
	ldb := &stubLedgerDB{
		tip:  pt(1),
		live: map[badgerutxo.Ref][]byte{ref(1, 0): {1, 0}},
	}
	s := newTestShadow(t, ldb)
	send(s, txChange(2, []ledger.UtxoOutput{output(1, 0)}, id(7, 0)))
	require.InDelta(t, 1, testutil.ToFloat64(s.m.missing), 0)
	require.InDelta(t, 1, testutil.ToFloat64(s.m.duplicate), 0)
}

// A rollback below the bootstrap point still applies, because its event
// carries what it restores, and the changes re-applied after it are new.
func TestShadowAppliesRollbackBelowTheBootstrapPoint(t *testing.T) {
	t.Parallel()
	ldb := &stubLedgerDB{
		tip:  pt(30),
		live: map[badgerutxo.Ref][]byte{ref(2, 0): {2, 0}},
	}
	s := newTestShadow(t, ldb)
	send(s, txChange(31, []ledger.UtxoOutput{output(3, 0)}))

	send(s, ledger.UtxoEvent{
		Point:   pt(10),
		Cause:   ledger.UtxoChangeRollback,
		Added:   []ledger.UtxoOutput{output(1, 0)},
		Removed: []models.UtxoId{id(2, 0), id(3, 0)},
	})
	requireLive(t, s, true, ref(1, 0))
	requireLive(t, s, false, ref(2, 0), ref(3, 0))
	require.InDelta(t, 1, testutil.ToFloat64(s.m.undone), 0)

	// Slot 20 is below the original bootstrap point but after the rollback.
	send(s, txChange(20, []ledger.UtxoOutput{output(4, 0)}, id(1, 0)))
	requireLive(t, s, true, ref(4, 0))
	requireLive(t, s, false, ref(1, 0))
	require.Zero(t, testutil.ToFloat64(s.m.missing))
	require.InDelta(t, 1, testutil.ToFloat64(s.m.bootstraps), 0)
}

func TestShadowBootstrapsAgainAfterAnOutOfOrderChange(t *testing.T) {
	t.Parallel()
	ldb := &stubLedgerDB{tip: pt(1)}
	s := newTestShadow(t, ldb)
	send(s, txChange(10, nil))
	send(s, txChange(5, nil))
	require.False(t, s.synced)

	ldb.tip = pt(10)
	send(s, txChange(11, nil))
	require.True(t, s.synced)
	require.InDelta(t, 2, testutil.ToFloat64(s.m.bootstraps), 0)
}

func TestShadowComparesWhenABlockCompletes(t *testing.T) {
	t.Parallel()
	ldb := &stubLedgerDB{
		tip:  pt(1),
		live: map[badgerutxo.Ref][]byte{ref(1, 0): {1, 0}},
	}
	s := newTestShadow(t, ldb)
	send(s, txChange(2, nil))
	require.Zero(t, testutil.ToFloat64(s.m.sampled))
	// The next block's first change completes the block at slot 2.
	send(s, txChange(3, nil))
	require.Positive(t, testutil.ToFloat64(s.m.sampled))
	require.Zero(t, testutil.ToFloat64(s.m.sampleMismatch))
	require.Equal(t, []uint64{2}, ldb.asOf,
		"the comparison reads the store as of the completed block")
}

func TestCompareFindsMissingAndDifferentOutputs(t *testing.T) {
	t.Parallel()
	a, b := ref(0xaa, 0), ref(0xbb, 1)
	ldb := &stubLedgerDB{
		tip:  pt(1),
		live: map[badgerutxo.Ref][]byte{a: {0x01}, b: {0x02}},
	}
	s := newTestShadow(t, ldb)
	require.NoError(t, s.Bootstrap())
	s.compare(1)
	require.InDelta(t, 2, testutil.ToFloat64(s.m.sampled), 0)
	require.Zero(t, testutil.ToFloat64(s.m.sampleMismatch))

	ldb.live[a] = []byte{0x09}
	delete(ldb.live, b)
	s.compare(1)
	require.InDelta(t, 2, testutil.ToFloat64(s.m.sampleMismatch), 0)

	rep, err := s.VerifyAll(context.Background())
	require.NoError(t, err)
	require.False(t, rep.Clean())
	require.Len(t, rep.CborDiffers, 1)
	require.Len(t, rep.ExtraInShadow, 1)
}

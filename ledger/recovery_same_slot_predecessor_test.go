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

package ledger

import (
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Byron epoch-boundary blocks take the first slot of their epoch, and the
// epoch's first regular block takes the same slot whenever one is minted
// there. Mainnet does it at every Byron boundary -- the genesis EBB
// 89d9b5a5 at slot 0 is the direct parent of f0f7892b, also at slot 0 -- so a
// chain holding two distinct blocks at one slot is ordinary history rather
// than a fork.
func sameSlotBoundaryBlocks(t testing.TB) []models.Block {
	t.Helper()
	specs := []struct {
		slot uint64
		seed string
	}{
		{slot: 1, seed: "ancestor-1"},
		{slot: 2, seed: "ancestor-2"},
		{slot: 3, seed: "epoch-boundary"},
		{slot: 3, seed: "first-block-of-epoch"},
		{slot: 4, seed: "successor"},
	}
	blocks := make([]models.Block, 0, len(specs))
	for i, spec := range specs {
		hash := sha256.Sum256([]byte(spec.seed))
		block := models.Block{
			ID:     uint64(i + 1), //nolint:gosec
			Slot:   spec.slot,
			Hash:   hash[:],
			Number: uint64(i + 1), //nolint:gosec
			Type:   1,
			Cbor:   []byte{0x80},
		}
		if i > 0 {
			block.PrevHash = append([]byte(nil), blocks[i-1].Hash...)
		}
		blocks = append(blocks, block)
	}
	return blocks
}

type sameSlotRecoveryFixture struct {
	ls           *LedgerState
	cm           *chain.ChainManager
	blocks       []models.Block
	resyncEvents <-chan event.Event
}

func newSameSlotRecoveryFixture(t *testing.T) *sameSlotRecoveryFixture {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := sameSlotBoundaryBlocks(t)
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	// The ledger has applied the epoch-boundary block. The block that failed
	// validation is its direct successor, which shares its slot.
	boundary := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(boundary),
		BlockNumber: boundary.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = ledgerTip
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip())
	require.Equal(t, blocks[4].Slot, cm.PrimaryChain().Tip().Point.Slot,
		"the rejected block and its successor start on the chain")

	return &sameSlotRecoveryFixture{
		ls:           ls,
		cm:           cm,
		blocks:       blocks,
		resyncEvents: resyncEvents,
	}
}

func TestHeaderValidationRecoveryDeclinesPastFailureAtSameSlot(
	t *testing.T,
) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	laterBlock := f.blocks[3]
	laterTip := ochainsync.Tip{
		Point:       makeTestPoint(laterBlock),
		BlockNumber: laterBlock.Number,
	}
	require.NoError(t, f.ls.db.SetTip(laterTip, nil))
	f.ls.currentTip = laterTip
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(boundary),
			Cause:      errors.New("failing EBB precedes the applied block"),
		},
	)

	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"a later same-slot tip must not be treated as preceding the failed EBB")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point)
	select {
	case <-f.resyncEvents:
		t.Fatal("declined recovery must not publish a resync")
	default:
	}
}

// The ledger tip is a valid rewind target whenever it is a different block
// from the one that failed and the chain orders it first. At a Byron epoch
// boundary that pair shares a slot, so a slot-only precedence test reports
// "no rewind target precedes it" for a target that does, declines a recovery
// that would have dropped the rejected block, and leaves the pipeline reading
// the same persisted block until the stuck detector fires.
func TestHeaderValidationRecoveryRewindsToSameSlotPredecessor(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	failing := f.blocks[3]
	require.Equal(t, boundary.Slot, failing.Slot)
	require.NotEqual(t, boundary.Hash, failing.Hash)

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(failing),
			Cause:      errors.New("VRF leader value exceeds threshold"),
		},
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered,
		"the applied epoch-boundary block precedes the rejected block and "+
			"is a rewind target")
	require.Equal(t, makeTestPoint(boundary),
		f.cm.PrimaryChain().Tip().Point,
		"the chain must be rewound onto the boundary block, not left "+
			"holding the rejected block at the same slot")

	select {
	case evt := <-f.resyncEvents:
		data, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		require.Equal(
			t,
			event.ChainsyncResyncReasonHeaderValidationRecovery,
			data.Reason,
		)
		require.Equal(t, makeTestPoint(boundary), data.Point)
	default:
		t.Fatal("recovery must publish a resync so chainsync re-delivers")
	}
}

// The same slot with the same hash is the ledger tip itself, which is what
// the guard exists to refuse: nothing would be dropped, so reporting a
// recovery would hide the failure from the stuck detector.
func TestHeaderValidationRecoveryDeclinesAtSameSlotSameHash(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(f.blocks[2]),
			Cause:      errors.New("rejected"),
		},
	)
	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"the ledger tip cannot be a rewind target for itself")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point,
		"declining must not disturb the chain")
	select {
	case <-f.resyncEvents:
		t.Fatal("a declined recovery must not publish a resync")
	default:
	}
}

// The deterministic transaction-validation recovery carries the identical
// precedence test and the identical Byron boundary exposure.
func TestDeterministicTxRecoveryRewindsToSameSlotPredecessor(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	failing := f.blocks[3]

	recovered, recoverErr := f.ls.recoverFromDeterministicTxValidationError(
		&txValidationError{
			BlockPoint: makeTestPoint(failing),
			TxHash:     testHashBytes("same-slot-duplicate-input"),
			Cause:      errors.New("duplicate input"),
		},
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered,
		"the applied epoch-boundary block precedes the rejected block and "+
			"is a rewind target")
	require.Equal(t, makeTestPoint(boundary),
		f.cm.PrimaryChain().Tip().Point,
		"the chain must be rewound onto the boundary block")
}

// Same-slot, same-hash still declines on the transaction path.
func TestDeterministicTxRecoveryDeclinesAtSameSlotSameHash(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.recoverFromDeterministicTxValidationError(
		&txValidationError{
			BlockPoint: makeTestPoint(f.blocks[2]),
			TxHash:     testHashBytes("same-slot-same-hash"),
			Cause:      errors.New("duplicate input"),
		},
	)
	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"the ledger tip cannot be a rewind target for itself")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point,
		"declining must not disturb the chain")
}

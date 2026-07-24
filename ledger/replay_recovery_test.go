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
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	pdata "github.com/blinklabs-io/plutigo/data"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type replayRecoveryInput struct {
	txId  []byte
	index uint32
}

func (m *replayRecoveryInput) Id() lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(m.txId)
}

func (m *replayRecoveryInput) Index() uint32 {
	return m.index
}

func (m *replayRecoveryInput) String() string {
	return fmt.Sprintf(
		"%s#%d",
		lcommon.NewBlake2b256(m.txId).String(),
		m.index,
	)
}

func (m *replayRecoveryInput) Utxorpc() (*cardano.TxInput, error) {
	return nil, nil
}

func (m *replayRecoveryInput) ToPlutusData() pdata.PlutusData {
	return nil
}

func TestTryRecoverFromTxValidationErrorRollsBackToEarliestProducerParent(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("recovery-parent", 100, 1, nil)
	producerOneBlock := testRawBlock(
		"producer-one",
		120,
		2,
		parentBlock.Hash,
	)
	producerTwoBlock := testRawBlock(
		"producer-two",
		140,
		3,
		producerOneBlock.Hash,
	)
	currentBlock := testRawBlock(
		"recovery-current",
		160,
		4,
		producerTwoBlock.Hash,
	)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{
				parentBlock,
				producerOneBlock,
				producerTwoBlock,
				currentBlock,
			},
		),
	)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	parentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(parentBlock.Slot, parentBlock.Hash),
		BlockNumber: parentBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}

	require.NoError(
		t,
		db.SetBlockNonce(
			parentTip.Point.Hash,
			parentTip.Point.Slot,
			[]byte("nonce-parent"),
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")
	ls.publishSnapshotsLocked()

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)
	require.NoError(
		t,
		store.DB().Create(&models.Transaction{
			Hash:       testHashBytes("producer-tx-1"),
			BlockHash:  producerOneBlock.Hash,
			Slot:       producerOneBlock.Slot,
			Type:       1,
			Valid:      true,
			BlockIndex: 0,
		}).Error,
	)
	require.NoError(
		t,
		store.DB().Create(&models.Transaction{
			Hash:       testHashBytes("producer-tx-2"),
			BlockHash:  producerTwoBlock.Hash,
			Slot:       producerTwoBlock.Slot,
			Type:       1,
			Valid:      true,
			BlockIndex: 0,
		}).Error,
	)

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("failing-tx"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  testHashBytes("producer-tx-2"),
					index: 0,
				},
				&replayRecoveryInput{
					txId:  testHashBytes("producer-tx-1"),
					index: 1,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)

	assert.Equal(t, parentTip, ls.currentTip)
	assert.Equal(t, currentTip, ls.chain.Tip())
	dbTip, err := ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, parentTip, dbTip)
}

func TestTryRecoverFromTxValidationErrorRejectsReplayBelowMithrilBoundary(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("mithril-recovery-parent", 100, 1, nil)
	producerBlock := testRawBlock(
		"mithril-recovery-producer",
		120,
		2,
		parentBlock.Hash,
	)
	boundaryBlock := testRawBlock(
		"mithril-recovery-boundary",
		200,
		3,
		producerBlock.Hash,
	)
	failingBlock := testRawBlock(
		"mithril-recovery-failing",
		220,
		4,
		boundaryBlock.Hash,
	)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{
				parentBlock,
				producerBlock,
				boundaryBlock,
				failingBlock,
			},
		),
	)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	activeConnId := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 3000,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.ParseIP("192.0.2.10"),
			Port: 3001,
		},
	}
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		EventBus:          bus,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			return &activeConnId
		},
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	shadowConnId := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 3000,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.ParseIP("192.0.2.11"),
			Port: 3001,
		},
	}

	boundaryTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(boundaryBlock.Slot, boundaryBlock.Hash),
		BlockNumber: boundaryBlock.BlockNumber,
	}
	require.NoError(t, db.SetTip(boundaryTip, nil))
	ls.currentTip = boundaryTip
	ls.currentTipBlockNonce = []byte("nonce-boundary")
	ls.mithrilLedgerSlot = boundaryTip.Point.Slot
	ls.publishSnapshotsLocked()
	timer := time.NewTimer(time.Hour)
	t.Cleanup(func() { timer.Stop() })
	ls.activeBlockfetchConnId = activeConnId
	ls.selectedBlockfetchConnId = activeConnId
	ls.shadowBlockfetchConnId = shadowConnId
	ls.shadowBlockReceivedHashes = map[string]struct{}{"stale": {}}
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chainsyncBlockfetchTimeoutTimer = timer
	ls.pendingBlockfetchEvents = []BlockfetchEvent{{Point: boundaryTip.Point}}
	ls.firstBlockReceived = true

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)
	producerTxHash := testHashBytes("mithril-recovery-producer-tx")
	require.NoError(
		t,
		store.DB().Create(&models.Transaction{
			Hash:       producerTxHash,
			BlockHash:  producerBlock.Hash,
			Slot:       producerBlock.Slot,
			Type:       1,
			Valid:      true,
			BlockIndex: 0,
		}).Error,
	)

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: ocommon.NewPoint(
				failingBlock.Slot,
				failingBlock.Hash,
			),
			TxHash: testHashBytes("mithril-recovery-failing-tx"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  producerTxHash,
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)

	assert.Equal(t, boundaryTip, ls.currentTip)
	assert.Equal(t, boundaryTip, ls.chain.Tip())
	dbTip, err := db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, boundaryTip, dbTip)
	_, err = database.BlockByPoint(
		db,
		ocommon.NewPoint(failingBlock.Slot, failingBlock.Hash),
	)
	assert.ErrorIs(t, err, models.ErrBlockNotFound)

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected Mithril boundary resync after replay recovery rejection",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		resync.Reason,
	)
	assert.Equal(t, activeConnId.String(), resync.ConnectionId.String())
	assert.Equal(t, boundaryTip.Point, resync.Point)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.activeBlockfetchConnId)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.selectedBlockfetchConnId)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.shadowBlockfetchConnId)
	assert.Nil(t, ls.shadowBlockReceivedHashes)
	assert.Nil(t, ls.chainsyncBlockfetchReadyChan)
	assert.Nil(t, ls.chainsyncBlockfetchTimeoutTimer)
	assert.Empty(t, ls.pendingBlockfetchEvents)
	assert.False(t, ls.firstBlockReceived)
}

func TestTryRecoverFromTxValidationErrorAtTipRewindsPrimaryChain(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("at-tip-parent", 100, 1, nil)
	producerBlock := testRawBlock(
		"at-tip-producer",
		120,
		2,
		parentBlock.Hash,
	)
	ledgerTipBlock := testRawBlock(
		"at-tip-ledger-tip",
		140,
		3,
		producerBlock.Hash,
	)
	failingBlock := testRawBlock(
		"at-tip-failing",
		160,
		4,
		ledgerTipBlock.Hash,
	)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{
				parentBlock,
				producerBlock,
				ledgerTipBlock,
				failingBlock,
			},
		),
	)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	ledgerTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ledgerTipBlock.Slot, ledgerTipBlock.Hash),
		BlockNumber: ledgerTipBlock.BlockNumber,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))
	ls.currentTip = ledgerTip
	ls.currentTipBlockNonce = []byte("nonce-ledger-tip")
	ls.validationEnabled = true
	ls.reachedTip.Store(true)
	ls.publishSnapshotsLocked()

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)
	require.NoError(
		t,
		store.DB().Create(&models.Transaction{
			Hash:       testHashBytes("producer-tx-live"),
			BlockHash:  producerBlock.Hash,
			Slot:       producerBlock.Slot,
			Type:       1,
			Valid:      true,
			BlockIndex: 0,
		}).Error,
	)

	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(
			failingBlock.Slot,
			failingBlock.Hash,
		),
		TxHash: testHashBytes("failing-live-tx"),
		Inputs: []lcommon.TransactionInput{
			&replayRecoveryInput{
				txId:  testHashBytes("producer-tx-live"),
				index: 0,
			},
		},
		Cause: errors.New(
			"conway utxo validation rule 38: delegation state mismatch",
		),
	}
	// First attempt: rewind primary chain to authoritative ledger tip
	// and request chainsync resync. This is the simple case — peers
	// may switch to a different fork that is compatible with our
	// ledger.
	recovered, err := ls.tryRecoverFromTxValidationError(validationErr)
	require.NoError(t, err)
	require.True(t, recovered)

	assert.Equal(t, ledgerTip, ls.currentTip)
	assert.Equal(t, ledgerTip, ls.chain.Tip())
	dbTip, err := ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, ledgerTip, dbTip)

	_, err = database.BlockByPoint(
		db,
		ocommon.NewPoint(failingBlock.Slot, failingBlock.Hash),
	)
	assert.ErrorIs(t, err, models.ErrBlockNotFound)

	// Second and third attempts: same failing (slot, block, tx).
	// The pipeline should keep recovering with progressively deeper
	// rewinds rather than halting on the first repeat — peers may
	// keep replaying the same losing fork and we need to expose a
	// wider candidate set for chainselection.
	for i := 2; i <= maxAtTipRecoveryAttempts; i++ {
		recovered, err = ls.tryRecoverFromTxValidationError(validationErr)
		require.NoError(t, err, "attempt %d should still recover", i)
		require.True(t, recovered, "attempt %d should still recover", i)
	}
	require.NotNil(t, ls.lastAtTipRecovery)
	assert.Equal(
		t,
		maxAtTipRecoveryAttempts,
		ls.lastAtTipRecovery.Attempts,
	)

	// One more attempt past the cap keeps recovering at the deepest
	// scheduled rewind. A persistent bad candidate chain is a peer/fork
	// selection problem; the node should keep trying fresh ChainSync
	// connections instead of halting the ledger pipeline.
	recovered, err = ls.tryRecoverFromTxValidationError(validationErr)
	require.NoError(t, err)
	require.True(t, recovered)
	require.NotNil(t, ls.lastAtTipRecovery)
	assert.Equal(
		t,
		maxAtTipRecoveryAttempts,
		ls.lastAtTipRecovery.Attempts,
	)
}

// newAtTipDescentLedger builds a LedgerState sitting at a fixed tip with
// at-tip validation enabled, for exercising the non-convergence guard in
// recoverAtTipFromTxValidationError. The failing blocks fed in these tests are
// synthetic and never added to the chain — recovery only ever rewinds to the
// ledger tip, which is all the guard tests assert on.
func newAtTipDescentLedger(t *testing.T) (*LedgerState, ochainsync.Tip) {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("descent-parent", 100, 1, nil)
	ledgerTipBlock := testRawBlock(
		"descent-ledger-tip",
		120,
		2,
		parentBlock.Hash,
	)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{parentBlock, ledgerTipBlock},
		),
	)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	ledgerTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			ledgerTipBlock.Slot,
			ledgerTipBlock.Hash,
		),
		BlockNumber: ledgerTipBlock.BlockNumber,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))
	ls.currentTip = ledgerTip
	ls.currentTipBlockNonce = []byte("nonce-descent-tip")
	ls.validationEnabled = true
	ls.reachedTip.Store(true)
	ls.publishSnapshotsLocked()
	return ls, ledgerTip
}

func atTipDescentFailure(slot uint64, tag string) *txValidationError {
	return &txValidationError{
		BlockPoint: ocommon.NewPoint(
			slot,
			testHashBytes("descent-block-"+tag),
		),
		TxHash: testHashBytes("descent-tx-" + tag),
		Cause: errors.New(
			"conway utxo validation: withdrawal not delegated to a drep",
		),
	}
}

// TestTryRecoverFromTxValidationErrorAtTipStopsDescendingRewindLoop verifies
// that a descending series of distinct at-tip validation failures trips the
// non-convergence guard: recovery holds at the ledger tip and records the
// condition instead of rewinding the primary chain ever deeper (issue #2939).
func TestTryRecoverFromTxValidationErrorAtTipStopsDescendingRewindLoop(
	t *testing.T,
) {
	ls, ledgerTip := newAtTipDescentLedger(t)

	// Feed a descending series of DISTINCT failures. Each first appears
	// (attempt 1: shallow rewind to ledger tip) then repeats (attempt 2:
	// same block re-delivered), mirroring the field log in #2939.
	for _, slot := range []uint64{500, 480, 460, 440} {
		ferr := atTipDescentFailure(slot, fmt.Sprintf("%d", slot))
		for range 2 {
			recovered, err := ls.tryRecoverFromTxValidationError(ferr)
			require.NoError(t, err)
			require.True(t, recovered)
		}
	}

	assert.True(
		t,
		ls.atTipRecoveryHolding,
		"descending distinct failures should trip the non-convergence guard",
	)
	assert.GreaterOrEqual(
		t,
		promtestutil.ToFloat64(ls.metrics.atTipRecoveryNonConverging),
		1.0,
		"holding should be recorded via the non-convergence metric",
	)
	// The primary chain must never be rewound below the ledger tip.
	assert.GreaterOrEqual(
		t,
		ls.chain.Tip().Point.Slot,
		ledgerTip.Point.Slot,
	)
}

// TestTryRecoverFromTxValidationErrorAtTipDoesNotHoldOnSameBlockEscalation
// verifies the guard does not fire for the legitimate same-block escalation
// (fork escape): repeating the identical (block, tx) failure must keep
// escalating without ever entering the hold state.
func TestTryRecoverFromTxValidationErrorAtTipDoesNotHoldOnSameBlockEscalation(
	t *testing.T,
) {
	ls, _ := newAtTipDescentLedger(t)

	ferr := atTipDescentFailure(500, "stable")
	for i := 0; i <= maxAtTipRecoveryAttempts+2; i++ {
		recovered, err := ls.tryRecoverFromTxValidationError(ferr)
		require.NoError(t, err)
		require.True(t, recovered)
	}

	assert.False(
		t,
		ls.atTipRecoveryHolding,
		"same-block escalation must not trip the non-convergence guard",
	)
	assert.Equal(t, 0, ls.atTipRecoveryDescentCount)
	assert.Equal(
		t,
		0.0,
		promtestutil.ToFloat64(ls.metrics.atTipRecoveryNonConverging),
	)
}

// TestTryRecoverFromTxValidationErrorAtTipResetsDescentOnForwardProgress
// verifies that a distinct failure at a HIGHER slot (forward progress past the
// previous failing point) resets the descent tracking, so an unrelated later
// failure gets a fresh recovery budget rather than being treated as part of a
// descent.
func TestTryRecoverFromTxValidationErrorAtTipResetsDescentOnForwardProgress(
	t *testing.T,
) {
	ls, _ := newAtTipDescentLedger(t)

	// Enough descending distinct failures to enter the hold state.
	for _, slot := range []uint64{500, 480, 460} {
		ferr := atTipDescentFailure(slot, fmt.Sprintf("%d", slot))
		recovered, err := ls.tryRecoverFromTxValidationError(ferr)
		require.NoError(t, err)
		require.True(t, recovered)
	}
	require.Equal(
		t,
		maxAtTipRecoveryDescents,
		ls.atTipRecoveryDescentCount,
	)
	require.True(t, ls.atTipRecoveryHolding)

	// A distinct failure at a higher slot is forward progress and clears it.
	recovered, err := ls.tryRecoverFromTxValidationError(
		atTipDescentFailure(600, "ahead"),
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Equal(t, 0, ls.atTipRecoveryDescentCount)
	assert.False(t, ls.atTipRecoveryHolding)
}

func TestTryRecoverFromTxValidationErrorAtTipRejectsRewindBelowMithrilBoundary(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("at-tip-mithril-parent", 100, 1, nil)
	ledgerTipBlock := testRawBlock(
		"at-tip-mithril-ledger-tip",
		60000,
		2,
		parentBlock.Hash,
	)
	failingBlock := testRawBlock(
		"at-tip-mithril-failing",
		60020,
		3,
		ledgerTipBlock.Hash,
	)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{
				parentBlock,
				ledgerTipBlock,
				failingBlock,
			},
		),
	)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})
	activeConnId := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 3000,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.ParseIP("192.0.2.12"),
			Port: 3001,
		},
	}
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		EventBus:          bus,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			return &activeConnId
		},
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	ledgerTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ledgerTipBlock.Slot, ledgerTipBlock.Hash),
		BlockNumber: ledgerTipBlock.BlockNumber,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))
	ls.currentTip = ledgerTip
	ls.currentTipBlockNonce = []byte("nonce-ledger-tip")
	ls.mithrilLedgerSlot = ledgerTip.Point.Slot
	ls.reachedTip.Store(true)
	ls.publishSnapshotsLocked()

	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(
			failingBlock.Slot,
			failingBlock.Hash,
		),
		TxHash: testHashBytes("failing-live-mithril-tx"),
		Inputs: []lcommon.TransactionInput{
			&replayRecoveryInput{
				txId:  testHashBytes("producer-live-mithril-tx"),
				index: 0,
			},
		},
		Cause: errors.New("bad input"),
	}
	ls.lastAtTipRecovery = newAtTipRecoveryAttempt(validationErr)
	ls.lastAtTipRecovery.Attempts = 1

	recovered, err := ls.tryRecoverFromTxValidationError(validationErr)
	require.NoError(t, err)
	require.True(t, recovered)

	assert.Equal(t, ledgerTip, ls.currentTip)
	assert.Equal(t, ledgerTip, ls.chain.Tip())
	dbTip, err := ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, ledgerTip, dbTip)
	_, err = database.BlockByPoint(
		db,
		ocommon.NewPoint(failingBlock.Slot, failingBlock.Hash),
	)
	assert.ErrorIs(t, err, models.ErrBlockNotFound)

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected Mithril boundary resync after at-tip recovery rejection",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		resync.Reason,
	)
	assert.Equal(t, activeConnId.String(), resync.ConnectionId.String())
	assert.Equal(t, ledgerTip.Point, resync.Point)
	require.NotNil(t, ls.lastAtTipRecovery)
	assert.Equal(t, 2, ls.lastAtTipRecovery.Attempts)
}

func TestTryRecoverFromTxValidationErrorFallsBackToTxBlobOffsets(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("blob-parent", 200, 1, nil)
	producerBlock := testRawBlock("blob-producer", 220, 2, parentBlock.Hash)
	currentBlock := testRawBlock("blob-current", 260, 3, producerBlock.Hash)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{
				parentBlock,
				producerBlock,
				currentBlock,
			},
		),
	)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	parentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(parentBlock.Slot, parentBlock.Hash),
		BlockNumber: parentBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			parentTip.Point.Hash,
			parentTip.Point.Slot,
			[]byte("nonce-parent"),
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	txHash := testHashBytes("blob-only-producer-tx")
	offset := &database.CborOffset{
		BlockSlot:  producerBlock.Slot,
		ByteOffset: 10,
		ByteLength: 20,
	}
	copy(offset.BlockHash[:], producerBlock.Hash)
	blobTxn := db.BlobTxn(true)
	require.NotNil(t, blobTxn)
	require.NoError(
		t,
		blobTxn.Do(func(txn *database.Txn) error {
			return db.Blob().SetTx(
				txn.Blob(),
				txHash,
				database.EncodeTxOffset(offset),
			)
		}),
	)

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")
	ls.publishSnapshotsLocked()

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("blob-only-failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  txHash,
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Less(t, ls.currentTip.Point.Slot, currentTip.Point.Slot)
	assert.LessOrEqual(t, ls.currentTip.Point.Slot, parentTip.Point.Slot)
}

func TestTryRecoverFromTxValidationErrorFallsBackToChainScan(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type replayRecoveryChainBlock struct {
		raw   chain.RawBlock
		block gledger.Block
	}
	var chainBlocks []replayRecoveryChainBlock
	producerIdx := -1
	for i := 0; i < 200 && (len(chainBlocks) < 12 || producerIdx < 0); i++ {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, immBlock)
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		chainBlocks = append(chainBlocks, replayRecoveryChainBlock{
			raw: chain.RawBlock{
				Slot:        block.SlotNumber(),
				Hash:        block.Hash().Bytes(),
				BlockNumber: block.BlockNumber(),
				Type:        uint(block.Type()),
				PrevHash:    block.PrevHash().Bytes(),
				Cbor:        block.Cbor(),
			},
			block: block,
		})
		if producerIdx < 0 &&
			len(chainBlocks) > 1 &&
			len(block.Transactions()) > 0 {
			producerIdx = len(chainBlocks) - 1
		}
	}
	require.GreaterOrEqual(t, len(chainBlocks), 12)
	require.GreaterOrEqual(t, producerIdx, 1)
	currentIdx := len(chainBlocks) - 1
	require.Greater(t, currentIdx, producerIdx)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(func() []chain.RawBlock {
			ret := make([]chain.RawBlock, 0, len(chainBlocks))
			for _, block := range chainBlocks {
				ret = append(ret, block.raw)
			}
			return ret
		}()),
	)

	parentBlock := chainBlocks[producerIdx-1]
	producerBlock := chainBlocks[producerIdx]
	currentBlock := chainBlocks[currentIdx]
	parentTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			parentBlock.raw.Slot,
			parentBlock.raw.Hash,
		),
		BlockNumber: parentBlock.raw.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			currentBlock.raw.Slot,
			currentBlock.raw.Hash,
		),
		BlockNumber: currentBlock.raw.BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			parentTip.Point.Hash,
			parentTip.Point.Slot,
			[]byte("nonce-parent"),
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")
	ls.publishSnapshotsLocked()

	producerTxs := producerBlock.block.Transactions()
	require.NotEmpty(t, producerTxs)
	producerTx := producerTxs[0]
	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("chain-scan-failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  producerTx.Hash().Bytes(),
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Less(t, ls.currentTip.Point.Slot, currentTip.Point.Slot)
	assert.LessOrEqual(t, ls.currentTip.Point.Slot, parentTip.Point.Slot)
}

func TestTryRecoverFromTxValidationErrorRecoversDependencyClosure(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type replayRecoveryChainBlock struct {
		raw   chain.RawBlock
		block gledger.Block
	}
	type replayRecoverySeenTx struct {
		block replayRecoveryChainBlock
		tx    lcommon.Transaction
	}
	var chainBlocks []replayRecoveryChainBlock
	seenTxs := make(map[string]replayRecoverySeenTx)
	var producerBlock replayRecoveryChainBlock
	var intermediateBlock replayRecoveryChainBlock
	var failingBlock replayRecoveryChainBlock
	var failingInput lcommon.TransactionInput
	found := false
	for i := 0; i < 20000 && !found; i++ {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, immBlock)
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		chainBlock := replayRecoveryChainBlock{
			raw: chain.RawBlock{
				Slot:        block.SlotNumber(),
				Hash:        block.Hash().Bytes(),
				BlockNumber: block.BlockNumber(),
				Type:        uint(block.Type()),
				PrevHash:    block.PrevHash().Bytes(),
				Cbor:        block.Cbor(),
			},
			block: block,
		}
		chainBlocks = append(chainBlocks, chainBlock)
		for _, tx := range block.Transactions() {
			for _, input := range collectReferencedInputs(tx) {
				mid, ok := seenTxs[string(input.Id().Bytes())]
				if !ok {
					continue
				}
				for _, midInput := range collectReferencedInputs(mid.tx) {
					producer, ok := seenTxs[string(midInput.Id().Bytes())]
					if !ok {
						continue
					}
					producerBlock = producer.block
					intermediateBlock = mid.block
					failingBlock = chainBlock
					failingInput = input
					found = true
					break
				}
				if found {
					break
				}
			}
			seenTxs[string(tx.Hash().Bytes())] = replayRecoverySeenTx{
				block: chainBlock,
				tx:    tx,
			}
			if found {
				break
			}
		}
	}
	require.True(t, found, "expected to find a two-hop tx dependency in immutable test data")
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(func() []chain.RawBlock {
			ret := make([]chain.RawBlock, 0, len(chainBlocks))
			for _, block := range chainBlocks {
				ret = append(ret, block.raw)
			}
			return ret
		}()),
	)

	currentTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			failingBlock.raw.Slot,
			failingBlock.raw.Hash,
		),
		BlockNumber: failingBlock.raw.BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")
	ls.publishSnapshotsLocked()

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("dependency-closure-failing"),
			Inputs: []lcommon.TransactionInput{
				failingInput,
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Less(t, ls.currentTip.Point.Slot, producerBlock.raw.Slot)
	assert.Less(
		t,
		ls.currentTip.Point.Slot,
		intermediateBlock.raw.Slot,
	)
}

func TestTryRecoverFromTxValidationErrorFallsBackToSecurityParamWindow(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	blocks := []chain.RawBlock{
		testRawBlock("fallback-1", 100, 1, nil),
		testRawBlock("fallback-2", 120, 2, testHashBytes("fallback-1")),
		testRawBlock("fallback-3", 140, 3, testHashBytes("fallback-2")),
		testRawBlock("fallback-4", 160, 4, testHashBytes("fallback-3")),
	}
	blocks[1].PrevHash = blocks[0].Hash
	blocks[2].PrevHash = blocks[1].Hash
	blocks[3].PrevHash = blocks[2].Hash
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(blocks))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(blocks[3].Slot, blocks[3].Hash),
		BlockNumber: blocks[3].BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")
	ls.publishSnapshotsLocked()

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("fallback-failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  testHashBytes("missing-producer"),
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Equal(t, ochainsync.Tip{}, ls.currentTip)
}

func TestTryRecoverFromTxValidationErrorSkipsUnknownProducer(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: ocommon.NewPoint(100, testHashBytes("current")),
			TxHash:     testHashBytes("failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  testHashBytes("missing-producer"),
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	assert.False(t, recovered)
}

func testRawBlock(
	seed string,
	slot uint64,
	blockNumber uint64,
	prevHash []byte,
) chain.RawBlock {
	hash := testHashBytes(seed)
	return chain.RawBlock{
		Slot:        slot,
		Hash:        hash,
		BlockNumber: blockNumber,
		Type:        1,
		PrevHash:    bytes.Clone(prevHash),
		Cbor:        []byte{0x80},
	}
}

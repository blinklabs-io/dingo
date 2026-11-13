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
	"context"
	"encoding/hex"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestBlocksAppliedUpToDropsCompetingBlockAtTipSlot(t *testing.T) {
	t.Parallel()

	tip := ocommon.NewPoint(10, bytes.Repeat([]byte{0x0A}, 32))
	applied := models.Block{Slot: 10, Hash: tip.Hash}
	competing := models.Block{Slot: 10, Hash: bytes.Repeat([]byte{0x0B}, 32)}
	below := models.Block{Slot: 9, Hash: bytes.Repeat([]byte{0x09}, 32)}
	above := models.Block{Slot: 11, Hash: bytes.Repeat([]byte{0x0C}, 32)}

	got := blocksAppliedUpTo(
		[]models.Block{above, competing, applied, below},
		tip,
	)
	require.Equal(t, []models.Block{applied, below}, got)
}

func TestUtxoWholeDetachedPointOffChainIsRejectedInSetOnlyMode(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	ls.config.UtxoSetOnly = true
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(300, bytes.Repeat([]byte{0xAB}, 32)),
	}, nil))
	ctx := withUtxoWholeDetach(
		t.Context(),
		func() (QueryPoint, bool) {
			return QueryPoint{
				Slot: 200,
				Hash: bytes.Repeat([]byte{0xCD}, 32),
			}, true
		},
	)

	_, err := ls.queryShelleyUtxoWhole(ctx, QueryPoint{}, nil)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

func TestNewLedgerStateRejectsUtxoSetReadsWithoutReader(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)

	ls, err := NewLedgerState(LedgerStateConfig{
		ChainManager: cm,
		Database:     db,
		Logger:       testLogger(),
		UtxoSetReads: UtxoSetReadsVerify,
	})
	if ls != nil {
		_ = ls.Close()
	}
	require.ErrorContains(t, err, "UtxoSetReads")
}

// unencodableOutput has no original CBOR and cannot be encoded: the channel
// field is unsupported by the CBOR encoder.
type unencodableOutput struct {
	lcommon.TransactionOutput
	Unsupported chan int
}

func (unencodableOutput) Cbor() []byte { return nil }

func TestOutputCborReportsEncodeFailure(t *testing.T) {
	t.Parallel()

	c, err := outputCbor(unencodableOutput{})
	require.Error(t, err)
	require.Empty(t, c)
}

func TestEmitEndorserUndoReportsDecodeFailure(t *testing.T) {
	t.Parallel()

	ls, _ := newDiskTestLedger(t)
	bus := newTestEventBus(t, nil)
	ls.config.EventBus = bus
	subID, errCh := bus.SubscribeWithBuffer(LedgerErrorEventType, 8)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, subID) })

	block := models.Block{Slot: 7, Hash: bytes.Repeat([]byte{0x07}, 32)}
	ls.emitEndorserUndo(
		t.Context(),
		block,
		[]durableEndorserTx{{Cbor: []byte{0xFF}, Index: 3}},
	)

	evt := testutil.RequireReceive(
		t, errCh, testutil.AsyncWait,
		"ledger error for the undecodable endorser transaction",
	)
	le, ok := evt.Data.(LedgerErrorEvent)
	require.True(t, ok, "unexpected payload %T", evt.Data)
	require.Equal(t, "rollback_tx_undo_decode", le.Operation)
	require.Equal(t, block.Slot, le.Point.Slot)
	require.Error(t, le.Error)
}

// absentUtxoSet is a live UTxO set that holds nothing, so every verify-mode
// comparison against a stored UTxO is a mismatch.
type absentUtxoSet struct{ reads atomic.Uint64 }

func (*absentUtxoSet) ApplyUtxoChanges(*ocommon.Point, []UtxoEvent) error {
	return nil
}

func (s *absentUtxoSet) Get([]byte, uint32) ([]byte, bool, error) {
	s.reads.Add(1)
	return nil, false, nil
}

func (*absentUtxoSet) Sum([]byte) (uint64, error) { return 0, nil }

func TestUtxoByTxInQueryDoesNotRunValidationVerify(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	set := &absentUtxoSet{}
	ls.config.UtxoSet = set
	ls.config.UtxoSetReads = UtxoSetReadsVerify
	txID := seedBabbageUtxo(t, db, 0xE1, 0, queryViewTestAddress(t), 2_000_000)
	txIn := ledger.MustNewShelleyTransactionInput(
		hex.EncodeToString(txID), 0,
	)

	res, err := ls.queryShelleyUtxoByTxIn(
		t.Context(),
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, utxoMapLen(t, res))
	require.Zero(t, set.reads.Load(), "a client query read the live set")
	require.Zero(t, promtestutil.ToFloat64(ls.metrics.utxoSetReadsVerified))
	require.Zero(t, ls.utxoSetReadMismatches.Load())
}

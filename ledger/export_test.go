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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// DeferredHeaderRecoveryFixture is a ledger whose primary chain carries a
// block that a deferred apply-time header check rejects, for tests in the
// external ledger_test package that compose the ledger with the networking
// packages the ledger itself must not import.
type DeferredHeaderRecoveryFixture struct {
	// Ledger is the ledger under test, usable as a mempool validator.
	Ledger *LedgerState

	t       testing.TB
	cm      *chain.ChainManager
	blocks  []models.Block
	tb      *testBlockResult
	rewound ocommon.Point
}

// NewDeferredHeaderRecoveryFixture builds a ledger with blocks 1..4 stored,
// the ledger tip at block 3 and no pool state, so the stateful header check
// rejects any block marked as deferred. Resync events are published on bus.
func NewDeferredHeaderRecoveryFixture(
	t *testing.T,
	bus *event.EventBus,
) *DeferredHeaderRecoveryFixture {
	t.Helper()
	tb := createTestBlock(t, [32]byte{77}, 0, tamperNone)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := newEligibilityTestLedgerOnDB(t, tb.epochNonce, db)

	blocks := make([]models.Block, 0, 4)
	for slot := uint64(1); slot <= 4; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append(
				[]byte(nil), blocks[len(blocks)-1].Hash...,
			)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(blocks[2]),
		BlockNumber: blocks[2].Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls.chain = cm.PrimaryChain()
	ls.currentTip = ledgerTip
	ls.config.ChainManager = cm
	ls.config.EventBus = bus
	ls.config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	ls.metrics.init(prometheus.NewRegistry())
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(context.Background()))
	return &DeferredHeaderRecoveryFixture{
		Ledger:  ls,
		t:       t,
		cm:      cm,
		blocks:  blocks,
		tb:      tb,
		rewound: ledgerTip.Point,
	}
}

// SupplyStateInvalidDeferredBlock has source supply the failing block as a
// deferred candidate, runs the apply-time header verdict, and runs the
// recovery that verdict triggers. The verdict is the ledger's own; only its
// block point is re-aimed at the stored chain block, because the stored
// blocks carry placeholder bodies. It reports whether recovery ran.
func (f *DeferredHeaderRecoveryFixture) SupplyStateInvalidDeferredBlock(
	source ouroboros.ConnectionId,
) bool {
	f.t.Helper()
	point := ocommon.NewPoint(
		f.tb.block.SlotNumber(),
		f.tb.block.Hash().Bytes(),
	)
	f.Ledger.markDeferredHeaderValidationFrom(point, source)
	err := f.Ledger.verifyDeferredBlockHeaderState(
		context.Background(), nil, point, f.tb.block,
	)
	require.Error(f.t, err)
	var verdict *headerValidationError
	require.ErrorAs(f.t, err, &verdict)
	verdict.BlockPoint = makeTestPoint(f.blocks[3])
	recovered, recoverErr := f.Ledger.tryRecoverFromHeaderValidationError(
		verdict,
	)
	require.NoError(f.t, recoverErr)
	return recovered
}

// PrimaryTip returns the primary chain tip.
func (f *DeferredHeaderRecoveryFixture) PrimaryTip() ocommon.Point {
	return f.cm.PrimaryChain().Tip().Point
}

// RejectedPoint returns the point of the block the verdict rejects.
func (f *DeferredHeaderRecoveryFixture) RejectedPoint() ocommon.Point {
	return makeTestPoint(f.blocks[3])
}

// RewindPoint returns the point recovery rewinds to.
func (f *DeferredHeaderRecoveryFixture) RewindPoint() ocommon.Point {
	return f.rewound
}

// AppendReplacementBlock extends the rewound chain with a block that replaces
// the rejected one and returns its point.
func (f *DeferredHeaderRecoveryFixture) AppendReplacementBlock() ocommon.Point {
	f.t.Helper()
	rejected := f.blocks[3]
	replacement := makeTestBlock(rejected.Slot+1, rejected.ID)
	require.NoError(f.t, f.cm.PrimaryChain().AddRawBlocks(context.Background(),
		[]chain.RawBlock{{
			Slot:        replacement.Slot,
			Hash:        replacement.Hash,
			BlockNumber: replacement.Number,
			Type:        replacement.Type,
			PrevHash:    f.rewound.Hash,
			Cbor:        replacement.Cbor,
		}},
	))
	return makeTestPoint(replacement)
}

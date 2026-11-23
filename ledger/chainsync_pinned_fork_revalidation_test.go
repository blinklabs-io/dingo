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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestPinnedForkRollbackRevalidatesAndQueuesSuffix resolves a fork whose
// headers are signed and registered, so each one passes peer-relative
// verification again after the rollback. The revalidated suffix must be
// queued, no persistent-fork resync may be requested, and the generation that
// authorized the queued header must replace the pre-rollback one everywhere
// the admitted header is reported.
func TestPinnedForkRollbackRevalidatesAndQueuesSuffix(t *testing.T) {
	t.Parallel()

	cfg := newHighFreqShelleyGenesisCfg(t)
	epochNonce := bytes.Repeat([]byte{0x21}, lcommon.Blake2b256Size)
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	// ancestor <- local tip, and ancestor <- fork0 <- fork1; the fork is one
	// block longer, so it wins the chain comparison.
	// One pool produces every block, so its stake is the whole snapshot.
	sign := func(
		slot, number uint64,
		prev []byte,
	) *testBlockResult {
		return createTestBlockWithNonceAtSlotsAndBlockNumber(
			t, [32]byte{140}, epochNonce, slot, slot+1, prev, &number,
			tamperNone,
		)
	}
	ancestor := sign(2_010, 1, make([]byte, lcommon.Blake2b256Size))
	local := sign(2_100, 2, ancestor.block.Hash().Bytes())
	fork0 := sign(2_200, 2, ancestor.block.Hash().Bytes())
	fork1 := sign(2_300, 3, fork0.block.Hash().Bytes())
	seedBlockPoolRegistration(t, db, ancestor.block)
	pool := ancestor.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 5, pool[:], 1_000_000_000)
	for _, b := range []*testBlockResult{ancestor, local} {
		require.NoError(
			t,
			cm.PrimaryChain().AddLocalBlock(context.Background(), b.block),
		)
	}

	// The rollback reloads the epoch cache from the database, so the epoch
	// the fork headers belong to must be stored.
	require.NoError(t, db.SetEpoch(
		0, 6, epochNonce, epochNonce, epochNonce, nil,
		eras.BabbageEraDesc.Id, 1_000, 432_000, nil,
	))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		UtxoSet:           NewMemLiveSetForTest(),
		ChainManager:      cm,
		CardanoNodeConfig: cfg,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.metrics.init(prometheus.NewRegistry())
	ls.config.UtxoSet = newQuerySetHistory()
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	resyncCh := subscribeChainsyncResync(t, bus)

	tipOf := func(b *testBlockResult) ochainsync.Tip {
		return ochainsync.Tip{
			Point: ocommon.NewPoint(
				b.block.SlotNumber(), b.block.Hash().Bytes(),
			),
			BlockNumber: b.block.BlockNumber(),
		}
	}
	ancestorTip, localTip := tipOf(ancestor), tipOf(local)
	ancestorNonce, err := eras.BabbageEraDesc.CalculateEtaVFunc(
		cfg, epochNonce, ancestor.block,
	)
	require.NoError(t, err)
	localNonce, err := eras.BabbageEraDesc.CalculateEtaVFunc(
		cfg, ancestorNonce, local.block,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetBlockNonce(
		ancestorTip.Point.Hash, ancestorTip.Point.Slot, ancestorNonce,
		true, nil,
	))
	require.NoError(t, db.SetBlockNonce(
		localTip.Point.Hash, localTip.Point.Slot, localNonce, false, nil,
	))
	require.NoError(t, db.SetTip(localTip, nil))

	epoch := models.Epoch{
		EpochId:        6,
		StartSlot:      0,
		SlotLength:     1_000,
		LengthInSlots:  432_000,
		EraId:          eras.BabbageEraDesc.Id,
		Nonce:          epochNonce,
		EvolvingNonce:  epochNonce,
		CandidateNonce: epochNonce,
	}
	ls.Lock()
	ls.slotsPerKESPeriod.Store(ancestor.slotsPerKesPeriod)
	ls.currentEpoch = epoch
	ls.currentEra = eras.BabbageEraDesc
	ls.epochCache = []models.Epoch{epoch}
	ls.currentTip = localTip
	ls.currentTipBlockNonce = append([]byte(nil), localNonce...)
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()
	ls.Unlock()
	preRollbackGeneration := ls.loadConsensusSnapshot().generation

	connId := ouroboros.ConnectionId{}
	parent := fork0.block.Header()
	ls.config.PeerHeaderLookupFunc = func(
		conn ouroboros.ConnectionId, hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if !bytes.Equal(hash, parent.Hash().Bytes()) {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			ConnectionId:                       conn,
			HeaderCryptoVerificationGeneration: preRollbackGeneration,
			Point: ocommon.NewPoint(
				parent.SlotNumber(), parent.Hash().Bytes(),
			),
			BlockHeader: parent,
		}, parent.PrevHash().Bytes(), true
	}

	header := fork1.block.Header()
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
	update := &chainselection.PeerTipUpdateEvent{
		ConnectionId:                       connId,
		HeaderCryptoVerificationGeneration: preRollbackGeneration,
	}
	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId:                       connId,
		HeaderCryptoVerificationGeneration: preRollbackGeneration,
		BlockHeader:                        header,
		Point:                              point,
		Tip: ochainsync.Tip{
			Point:       point,
			BlockNumber: header.BlockNumber(),
		},
		PeerTipUpdate: update,
	})
	require.NoError(t, err)

	rollbackResync := testutil.RequireReceive(t, resyncCh, time.Second,
		"the rollback itself must request a re-intersect")
	require.Equal(
		t,
		event.ChainsyncResyncReasonLocalLedgerRollback,
		rollbackResync.Reason,
	)
	testutil.RequireNoReceive(t, resyncCh, 200*time.Millisecond,
		"revalidated fork headers must not trigger a persistent-fork resync")

	require.Equal(t, ancestorTip, ls.chain.Tip())
	require.Equal(t, 2, ls.chain.HeaderCount(),
		"the revalidated fork suffix must be queued")
	require.Equal(t, point, ls.chain.HeaderTip().Point)

	postRollbackGeneration := ls.loadConsensusSnapshot().generation
	require.Greater(t, postRollbackGeneration, preRollbackGeneration)
	queued := ls.chain.QueuedHeaderCryptoVerificationGeneration(point)
	require.NotZero(t, queued)
	require.NotEqual(t, preRollbackGeneration, queued,
		"the queued header must not keep the pre-rollback generation")
	require.Equal(t, queued, update.HeaderCryptoVerificationGeneration,
		"the peer tip update must carry the queued header's generation")
	require.True(t, ls.headerCryptoVerificationGenerationCurrent(queued))
}

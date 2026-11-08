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

package ouroboros

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type checkpointRollForwardFixture struct {
	o          *Ouroboros
	connId     ouroboros.ConnectionId
	state      *dchainsync.State
	ledgerCh   <-chan event.Event
	observedCh <-chan event.Event
	recycleCh  <-chan event.Event
}

// newCheckpointRollForwardFixture builds a handler whose chain selection
// rejects every candidate that clears the checkpoint gate, so a header that
// passes the gate is observable as errChainsyncCandidateRejected.
func newCheckpointRollForwardFixture(
	t *testing.T,
	checkpoints map[uint64]string,
) *checkpointRollForwardFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, observedCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	state := dchainsync.NewState(bus, nil)
	connId := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(connId))
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(
			chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			return chainselection.PeerTipUpdateRejected
		},
	})
	o.chainsyncState = state
	o.chainsyncHeaderCheckpoint = func(h gledger.BlockHeader) error {
		return ledger.ValidateHeaderCheckpoint(checkpoints, h)
	}
	return &checkpointRollForwardFixture{
		o: o, connId: connId, state: state,
		ledgerCh: ledgerCh, observedCh: observedCh, recycleCh: recycleCh,
	}
}

func (f *checkpointRollForwardFixture) rollForward(
	header gledger.BlockHeader,
	blockType uint,
) error {
	return f.o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: f.connId},
		blockType,
		header,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes()),
			BlockNumber: header.BlockNumber(),
		},
		time.Now(),
	)
}

func TestChainsyncRollForwardRejectsCheckpointMismatch(t *testing.T) {
	t.Parallel()

	header := newTestBlockHeader(100, 7, 0xaa)
	f := newCheckpointRollForwardFixture(t, map[uint64]string{
		7: "00000000000000000000000000000000000000000000000000000000000000ff",
	})

	err := f.rollForward(header, 0)

	var cpErr *ledger.CheckpointMismatchError
	require.ErrorAs(t, err, &cpErr)
	require.Equal(t, uint64(7), cpErr.BlockNo)
	evt := testutil.RequireReceive(
		t, f.recycleCh, time.Second,
		"a checkpoint-mismatched header must recycle the delivering connection",
	)
	recycle := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
	require.Equal(t, f.connId, recycle.ConnectionId)
	require.Equal(t, "header_verification_failure", recycle.Reason)
	testutil.RequireNoReceive(
		t, f.observedCh, 50*time.Millisecond,
		"a checkpoint-mismatched header must not reach chain selection",
	)
	testutil.RequireNoReceive(
		t, f.ledgerCh, 50*time.Millisecond,
		"a checkpoint-mismatched header must not reach the ledger queue",
	)
}

func TestChainsyncRollForwardAcceptsCheckpointMatchAndOtherHeights(t *testing.T) {
	t.Parallel()

	matching := newTestBlockHeader(100, 7, 0xaa)
	f := newCheckpointRollForwardFixture(t, map[uint64]string{
		7: matching.Hash().String(),
		9: "00000000000000000000000000000000000000000000000000000000000000ff",
	})
	// Reaching chain selection (which the fixture rejects) proves the gate
	// let the header through.
	require.ErrorIs(t, f.rollForward(matching, 0), errChainsyncCandidateRejected)
	other := newTestBlockHeader(101, 8, 0xbb)
	require.ErrorIs(t, f.rollForward(other, 0), errChainsyncCandidateRejected)
	testutil.RequireNoReceive(
		t, f.recycleCh, 50*time.Millisecond,
		"accepted headers must not recycle the connection",
	)
}

func TestChainsyncRollForwardCheckpointSkipsByronEbb(t *testing.T) {
	t.Parallel()

	f := newCheckpointRollForwardFixture(t, map[uint64]string{
		0: "00000000000000000000000000000000000000000000000000000000000000ff",
	})
	// The EBB reaches the gate through the real decode path in production;
	// the zero-value header never gets past the skip, which precedes any
	// header accessor.
	ebb := &byron.ByronEpochBoundaryBlockHeader{}
	err := f.o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: f.connId},
		gledger.BlockTypeByronEbb,
		ebb,
		ochainsync.Tip{},
		time.Now(),
	)
	var cpErr *ledger.CheckpointMismatchError
	require.NotErrorAs(t, err, &cpErr)
	testutil.RequireNoReceive(
		t, f.recycleCh, 50*time.Millisecond,
		"an EBB must not be treated as a checkpoint mismatch",
	)
}

func TestNewOuroborosWiresHeaderCheckpointFromLedgerConfig(t *testing.T) {
	t.Parallel()

	nodeCfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preview/config.json",
	)
	require.NoError(t, err)
	var height uint64
	var expected string
	for h, hash := range nodeCfg.Checkpoints() {
		height, expected = h, hash
		break
	}
	require.NotEmpty(t, expected, "preview config must ship checkpoints")

	_, db := newTestLedgerStateWithChain(t, 1)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: nodeCfg,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	o := newOuroboros(OuroborosConfig{LedgerState: ls})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.NotNil(t, o.chainsyncHeaderCheckpoint)

	var cpErr *ledger.CheckpointMismatchError
	require.ErrorAs(
		t,
		o.chainsyncHeaderCheckpoint(newTestBlockHeader(1, height, 0xaa)),
		&cpErr,
	)
	require.Equal(t, expected, cpErr.Expected)
	unchecked := uint64(1)
	for _, ok := nodeCfg.Checkpoints()[unchecked]; ok; _, ok = nodeCfg.Checkpoints()[unchecked] {
		unchecked++
	}
	require.NoError(
		t,
		o.chainsyncHeaderCheckpoint(newTestBlockHeader(1, unchecked, 0xaa)),
	)
}

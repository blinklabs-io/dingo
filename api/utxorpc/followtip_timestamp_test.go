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

package utxorpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
)

// followTipTimestampLedger stubs the SlotToTime/GetBlock/Tip boundary that
// followTipResponse calls. Everything else panics if reached, so a call that
// slips past a returned error is visible immediately.
type followTipTimestampLedger struct {
	UtxorpcLedgerState

	tip ochainsync.Tip

	block    models.Block
	blockErr error

	// slotToTimeErrOnSlot fails SlotToTime for exactly this slot; every
	// other slot succeeds with a fixed time built from the slot number.
	slotToTimeErrOnSlot uint64
	slotToTimeErr       error
}

func (l *followTipTimestampLedger) Tip() ochainsync.Tip {
	return l.tip
}

func (l *followTipTimestampLedger) GetBlock(
	ocommon.Point,
) (models.Block, error) {
	return l.block, l.blockErr
}

func (l *followTipTimestampLedger) SlotToTime(
	slot uint64,
) (time.Time, error) {
	if slot == l.slotToTimeErrOnSlot {
		return time.Time{}, l.slotToTimeErr
	}
	return time.UnixMilli(int64(slot) * 1000), nil
}

func newFollowTipTimestampServer(
	ls UtxorpcLedgerState,
) *syncServiceServer {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		LedgerState: ls,
	})
	return &syncServiceServer{utxorpc: u}
}

// TestFollowTipResponse_RollbackSlotToTimeErrorPropagates: a SlotToTime
// failure for a non-origin rollback point must make FollowTip return an
// error, not a Reset action carrying Timestamp 0. Before the fix, the error
// from SlotToTime was discarded (`err == nil` guard) and timestamp stayed at
// its zero value.
func TestFollowTipResponse_RollbackSlotToTimeErrorPropagates(t *testing.T) {
	t.Parallel()
	rollbackPoint := ocommon.NewPoint(100, []byte{0x01, 0x02, 0x03})
	wantErr := errors.New("slot to time stub failure")
	stub := &followTipTimestampLedger{
		block:               models.Block{Number: 42},
		slotToTimeErrOnSlot: rollbackPoint.Slot,
		slotToTimeErr:       wantErr,
	}
	srv := newFollowTipTimestampServer(stub)

	resp, err := srv.followTipResponse(&chain.ChainIteratorResult{
		Point:    rollbackPoint,
		Rollback: true,
	})

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resp, "no response should be built when SlotToTime fails")
}

// TestFollowTipResponse_RollbackSucceedsWithTimestamp is the positive
// counterpart: a successful SlotToTime call still produces a non-zero
// timestamp on the Reset action.
func TestFollowTipResponse_RollbackSucceedsWithTimestamp(t *testing.T) {
	t.Parallel()
	rollbackPoint := ocommon.NewPoint(100, []byte{0x01, 0x02, 0x03})
	stub := &followTipTimestampLedger{
		block: models.Block{Number: 42},
		tip:   ochainsync.Tip{Point: ocommon.NewPoint(200, []byte{0xaa})},
	}
	srv := newFollowTipTimestampServer(stub)

	resp, err := srv.followTipResponse(&chain.ChainIteratorResult{
		Point:    rollbackPoint,
		Rollback: true,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	reset, ok := resp.Action.(*sync.FollowTipResponse_Reset_)
	require.True(t, ok)
	require.Equal(t, uint64(42), reset.Reset_.Height)
	require.Equal(
		t,
		uint64(100_000),
		reset.Reset_.Timestamp,
		"timestamp should reflect the stubbed slot time",
	)
}

// TestFollowTipResponse_TipSlotToTimeErrorPropagates covers the second
// SlotToTime call site: the current-tip timestamp populated on every
// response, reset or apply. Using a rollback-to-origin result reaches this
// call without needing a GetBlock stub or a decodable block CBOR fixture.
func TestFollowTipResponse_TipSlotToTimeErrorPropagates(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("tip slot to time stub failure")
	tipPoint := ocommon.NewPoint(555, []byte{0xbe, 0xef})
	stub := &followTipTimestampLedger{
		tip:                 ochainsync.Tip{Point: tipPoint, BlockNumber: 9},
		slotToTimeErrOnSlot: tipPoint.Slot,
		slotToTimeErr:       wantErr,
	}
	srv := newFollowTipTimestampServer(stub)

	// Rollback to origin: slot 0, empty hash. This skips the rollback-block
	// lookup and its own SlotToTime call, so only the tip's SlotToTime call
	// is exercised.
	resp, err := srv.followTipResponse(&chain.ChainIteratorResult{
		Point:    ocommon.NewPoint(0, nil),
		Rollback: true,
	})

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resp)
}

// slotToTimeFailingLedger is a real ledger whose SlotToTime always fails, so
// FollowTip runs its real chain iterator and only the conversion fails.
type slotToTimeFailingLedger struct {
	*ledger.LedgerState

	err error
}

func (l slotToTimeFailingLedger) SlotToTime(uint64) (time.Time, error) {
	return time.Time{}, l.err
}

// TestConnect_FollowTip_SlotToTimeErrorEndsStream drives the FollowTip handler
// end to end: a SlotToTime failure must reach the client as a stream error,
// not as an Apply frame whose Tip carries Timestamp 0.
func TestConnect_FollowTip_SlotToTimeErrorEndsStream(t *testing.T) {
	t.Parallel()
	const n = 8
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: n})
	blocks := loadTestChainBlocks(t, n)
	require.Len(t, blocks, n)
	inter := blocks[5]

	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: h.EB,
		LedgerState: slotToTimeFailingLedger{
			LedgerState: h.LS,
			err:         errors.New("slot to time stub failure"),
		},
		Mempool: h.MP,
	})
	srv := httptest.NewUnstartedServer(testUtxorpcHTTPHandler(u))
	srv.Config.Protocols = unencryptedHTTP2Protocols()
	srv.Start()
	t.Cleanup(srv.Close)

	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		srv.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	stream, err := cli.FollowTip(ctx, connect.NewRequest(&sync.FollowTipRequest{
		Intersect: []*sync.BlockRef{
			{
				Slot:   inter.Slot,
				Hash:   append([]byte(nil), inter.Hash...),
				Height: inter.Number,
			},
		},
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	require.False(
		t,
		stream.Receive(),
		"expected no frame when SlotToTime fails, got %T",
		stream.Msg().GetAction(),
	)
	require.ErrorContains(t, stream.Err(), "slot to time stub failure")
}

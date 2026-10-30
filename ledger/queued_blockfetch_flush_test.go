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
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type queuedFlushTestTimer struct {
	callback func()
	stopped  atomic.Bool
}

type queuedFlushTestRequest struct {
	conn       ouroboros.ConnectionId
	start, end ocommon.Point
}

func TestQueuedBlockfetchBatchHasBoundedAccumulation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		change         func(*testing.T, *LedgerState, *ouroboros.ConnectionId)
		want           bool
		normalDispatch bool
	}{
		{name: "selected live owner", want: true},
		{name: "current selection replaced", want: true, change: func(
			_ *testing.T, _ *LedgerState, conn *ouroboros.ConnectionId,
		) {
			*conn = ouroboros.ConnectionId{
				LocalAddr:  &net.TCPAddr{Port: 6001},
				RemoteAddr: &net.TCPAddr{Port: 3002},
			}
		}},
		{name: "replaced head", change: func(
			t *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ls.chain.ClearHeaders()
			require.NoError(t, ls.chain.AddBlockHeader(context.Background(),
				mockHeader{
					hash:        lcommon.NewBlake2b256([]byte("replacement")),
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1, slot: 100,
				}))
		}},
		{name: "canceled context", change: func(
			_ *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ctx, cancel := context.WithCancel(ls.ctx)
			cancel()
			ls.ctx = ctx
		}},
		{name: "missing context", change: func(
			_ *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ls.ctx = nil
		}},
		{name: "normal threshold dispatch", normalDispatch: true,
			change: func(t *testing.T, ls *LedgerState,
				conn *ouroboros.ConnectionId,
			) {
				prev := lcommon.NewBlake2b256(ls.chain.HeaderTip().Point.Hash)
				for i := uint64(21); i <= 256; i++ {
					hash := lcommon.NewBlake2b256(fmt.Appendf(nil, "hdr-%d", i))
					require.NoError(t, ls.handleEventChainsyncBlockHeader(
						ChainsyncEvent{
							ConnectionId: *conn,
							Point:        ocommon.NewPoint(i, hash.Bytes()),
							BlockHeader: mockHeader{
								hash: hash, prevHash: prev,
								blockNumber: i - 1, slot: i,
							},
							Tip: ochainsync.Tip{
								Point:       ocommon.NewPoint(1280, []byte("tip")),
								BlockNumber: 1280,
							},
						},
					))
					prev = hash
				}
			}},
		{name: "no selected peer", change: func(
			_ *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
				return nil
			}
		}},
		{name: "disconnected selected peer", change: func(
			_ *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ls.config.ConnectionLiveFunc = func(ouroboros.ConnectionId) bool {
				return false
			}
		}},
		{name: "cleared queue", change: func(
			_ *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ls.clearQueuedHeaders()
		}},
		{name: "closed ledger", change: func(
			t *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			require.NoError(t, ls.Close())
		}},
		{name: "batch already active", change: func(
			_ *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ls.chainsyncBlockfetchReadyChan = make(chan struct{})
		}},
		{name: "continuation already pending", change: func(
			_ *testing.T, ls *LedgerState, _ *ouroboros.ConnectionId,
		) {
			ls.blockfetchContinuationPending = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := ouroboros.ConnectionId{
				LocalAddr:  &net.TCPAddr{Port: 6000},
				RemoteAddr: &net.TCPAddr{Port: 3001},
			}
			ctx, cancel := context.WithCancel(context.Background())
			requests := make(chan queuedFlushTestRequest, 4)
			timers := make(chan *queuedFlushTestTimer, 4)
			ls := &LedgerState{
				ctx:   ctx,
				chain: &chain.Chain{},
				config: LedgerStateConfig{
					Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
					GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
						return &conn
					},
					BlockfetchRequestRangeFunc: func(
						peer ouroboros.ConnectionId,
						start, end ocommon.Point,
					) (uint64, error) {
						requests <- queuedFlushTestRequest{peer, start, end}
						return 1, nil
					},
				},
			}
			ls.queuedBlockfetchSchedule = func(
				delay time.Duration, callback func(),
			) func() {
				require.Equal(t, queuedBlockfetchMaxWait, delay)
				timer := &queuedFlushTestTimer{callback: callback}
				timers <- timer
				return func() { timer.stopped.Store(true) }
			}
			t.Cleanup(func() {
				ls.closed.Store(true)
				cancel()
				ls.stopQueuedBlockfetchFlush()
				ls.blockfetchContinuationWG.Wait()
				ls.chainsyncBlockfetchMutex.Lock()
				if ls.chainsyncBlockfetchTimeoutTimer != nil {
					ls.chainsyncBlockfetchTimeoutTimer.Stop()
				}
				ls.chainsyncBlockfetchMutex.Unlock()
			})
			var first, last ocommon.Point
			prevHash := lcommon.NewBlake2b256(nil)
			for i := uint64(1); i <= 20; i++ {
				hash := lcommon.NewBlake2b256(fmt.Appendf(nil, "hdr-%d", i))
				point := ocommon.NewPoint(i, hash.Bytes())
				require.NoError(t, ls.handleEventChainsyncBlockHeader(
					ChainsyncEvent{
						ConnectionId: conn,
						Point:        point,
						BlockHeader: mockHeader{
							hash: hash, prevHash: prevHash,
							blockNumber: i - 1, slot: i,
						},
						Tip: ochainsync.Tip{
							Point:       ocommon.NewPoint(1280, []byte("tip")),
							BlockNumber: 1280,
						},
					},
				))
				if i == 1 {
					first = point
				}
				last, prevHash = point, hash
			}
			require.Equal(t, 20, ls.chain.HeaderCount())
			require.Empty(t, requests, "normal batch threshold still applies")
			require.Len(t, timers, 1,
				"production header admission must bound the initial wait")
			timer := <-timers
			if tc.change != nil {
				tc.change(t, ls, &conn)
			}
			if tc.normalDispatch || tc.name == "closed ledger" ||
				tc.name == "cleared queue" {
				require.True(t, timer.stopped.Load(),
					"teardown must cancel the accumulation deadline")
			}
			if tc.normalDispatch {
				request := testutil.RequireReceive(t, requests, time.Second,
					"normal header threshold must dispatch without the timer")
				require.Equal(t, first, request.start)
				require.EqualValues(t, 256, request.end.Slot)
			}
			// Fire even a stopped callback to model a timer already racing
			// cancellation. Pointer identity and live state must reject it.
			timer.callback()
			ls.blockfetchContinuationWG.Wait()
			if tc.want {
				request := testutil.RequireReceive(t, requests, time.Second,
					"a quiet admitted queue must start fetching at max wait")
				require.Equal(t, conn, request.conn)
				require.Equal(t, first, request.start)
				require.Equal(t, last, request.end)
			} else {
				require.Empty(t, requests,
					"timer cannot bypass selection, lifecycle, or active work")
			}
			timer.callback()
			ls.blockfetchContinuationWG.Wait()
			require.Empty(t, requests, "a deadline may dispatch at most once")
			require.Empty(t, timers, "inactive queues cannot spin new timers")
		})
	}
}

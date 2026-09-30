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
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	syncapi "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	watchapi "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
)

// Only the synchronous lookup boundary is supplied: reaching any later
// ledger method would mean the handler ignored the lookup error.
type intersectionLimitLedger struct {
	UtxorpcLedgerState
	points []ocommon.Point
	calls  int
	err    error
}

func (l *intersectionLimitLedger) GetIntersectPoint(
	points []ocommon.Point,
) (*ocommon.Point, error) {
	l.calls++
	l.points = points
	return nil, l.err
}

func (l *intersectionLimitLedger) Tip() ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.NewPoint(42, bytes.Repeat([]byte{1}, 32)),
	}
}

func TestStreamingIntersectionLimit(t *testing.T) {
	const limit = 2
	for _, method := range []string{"FollowTip", "WatchTx"} {
		for _, count := range []int{0, limit - 1, limit, limit + 1} {
			t.Run(
				fmt.Sprintf("%s/count_%d", method, count),
				func(t *testing.T) {
					var logs bytes.Buffer
					lookupErr := errors.New("intersection lookup reached")
					state := &intersectionLimitLedger{err: lookupErr}
					u := NewUtxorpc(UtxorpcConfig{
						Logger:       slog.New(slog.NewTextHandler(&logs, nil)),
						LedgerState:  state,
						MaxBlockRefs: limit,
					})
					// Duplicate valid points count toward the cap before any deduplication.
					point := state.Tip().Point
					var err error
					switch method {
					case "FollowTip":
						refs := make([]*syncapi.BlockRef, count)
						for i := range refs {
							refs[i] = &syncapi.BlockRef{
								Slot: point.Slot,
								Hash: point.Hash,
							}
						}
						err = (&syncServiceServer{utxorpc: u}).FollowTip(
							t.Context(),
							connect.NewRequest(
								&syncapi.FollowTipRequest{Intersect: refs},
							),
							nil,
						)
					case "WatchTx":
						refs := make([]*watchapi.BlockRef, count)
						for i := range refs {
							refs[i] = &watchapi.BlockRef{
								Slot: point.Slot,
								Hash: point.Hash,
							}
						}
						err = (&watchServiceServer{utxorpc: u}).WatchTx(
							t.Context(),
							connect.NewRequest(
								&watchapi.WatchTxRequest{Intersect: refs},
							),
							nil,
						)
					}
					if count > limit {
						require.Zero(
							t,
							state.calls,
							"oversized intersections must not reach ledger lookup",
						)
						require.Equal(
							t,
							connect.CodeInvalidArgument,
							connect.CodeOf(err),
						)
						require.ErrorContains(
							t,
							err,
							"too many block refs: 3 exceeds maximum of 2",
						)
						require.Empty(
							t,
							logs.String(),
							"oversized intersections must not be logged",
						)
						return
					}
					require.ErrorIs(
						t,
						err,
						lookupErr,
						"allowed requests must reach the ledger",
					)
					require.Equal(t, 1, state.calls)
					require.Len(t, state.points, max(count, 1))
					for _, got := range state.points {
						require.Equal(t, point, got)
					}
				},
			)
		}
	}
}

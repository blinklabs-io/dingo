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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestChainsyncBlockZeroWaitsAtOrigin(t *testing.T) {
	t.Parallel()
	for _, slot := range []uint64{0, 3} {
		t.Run(fmt.Sprint(slot), func(t *testing.T) {
			t.Parallel()
			f := newForecastHorizonFixture(t, "10.0.0.31:3001")
			f.o.ledgerState = newTestLedgerState(t)
			t.Cleanup(func() { require.NoError(t, f.o.ledgerState.Close()) })
			f.o.chainsyncLocalTipBlock = func() (uint64, uint64) { return f.o.ledgerState.Tip().BlockNumber, 400 }
			f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) { return 1, nil }
			var eligible atomic.Bool
			f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool { return eligible.Load() }
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			releaseWait := func() { once.Do(func() { close(release) }) }
			done := make(chan error, 1)
			exited := make(chan struct{})
			t.Cleanup(func() {
				releaseWait()
				testutil.RequireReceive(
					t,
					exited,
					testutil.AsyncWait,
					"callback must exit before ledger cleanup",
				)
			})
			f.o.config.ChainsyncAwaitCorroboration = func(ctx context.Context, _ ouroboros.ConnectionId) (bool, error) {
				entered <- struct{}{}
				select {
				case <-release:
					return eligible.Load(), nil
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}
			header := newTestBlockHeader(slot, 0, 0xa1)
			go func() {
				defer close(exited)
				done <- f.o.chainsyncClientRollForward(ochainsync.CallbackContext{ConnectionId: f.conn}, 0, header, ochainsync.Tip{Point: ocommon.NewPoint(10_000_000, []byte("remote-tip")), BlockNumber: 500_000})
			}()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf(
					"first header was discarded before corroboration: %v",
					err,
				)
			case <-time.After(testutil.AsyncWait):
				t.Fatal("first header never entered corroboration")
			}
			select {
			case evt := <-f.ledgerCh:
				t.Fatalf("uncorroborated first header reached ledger: %+v", evt)
			default:
			}
			eligible.Store(true)
			releaseWait()
			require.NoError(
				t,
				testutil.RequireReceive(
					t,
					done,
					testutil.AsyncWait,
					"corroborated block zero must proceed",
				),
			)
			evt := testutil.RequireReceive(
				t,
				f.ledgerCh,
				testutil.AsyncWait,
				"first header must reach ledger",
			)
			data, ok := evt.Data.(ledger.ChainsyncEvent)
			require.True(t, ok)
			require.Equal(t, header.Hash(), data.BlockHeader.Hash())
			require.Zero(t, data.BlockHeader.BlockNumber())
			setTestLedgerTip(
				t,
				f.o,
				ochainsync.Tip{
					Point:       ocommon.NewPoint(slot, header.Hash().Bytes()),
					BlockNumber: 0,
				},
			)
			require.True(
				t,
				f.o.headerAtOrBelowLocalTip(header),
				"an applied block zero is historical, including at slot zero",
			)
		})
	}
}

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

package mempool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The capacity function may block on ledger and database reads. The ledger
// reaches the pool only through Transactions and RemoveTxsByHash, often while
// holding its own locks, so neither may wait on a capacity read in flight.
func TestMempoolCapacityFuncBlockingDoesNotStallLedgerCalls(t *testing.T) {
	t.Parallel()
	first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 1)
	second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 1)
	txType := uint(conway.EraIdConway)

	var blocking atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	pool, err := NewFIFO(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: 1 << 30,
		CapacityFunc: func() int64 {
			if blocking.Load() {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			}
			return 1 << 30
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	require.NoError(t, pool.Start(context.Background()))
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })

	require.NoError(t, pool.AddTransaction(context.Background(), txType, first))
	held := pool.Transactions()
	require.Len(t, held, 1)

	blocking.Store(true)
	admitted := make(chan error, 1)
	go func() {
		admitted <- pool.AddTransaction(context.Background(), txType, second)
	}()
	dingotestutil.RequireReceive(t, entered, 2*time.Second, "admission reads capacity")

	ledgerDone := make(chan struct{})
	go func() {
		_ = pool.Transactions()
		pool.RemoveTxsByHash([]string{held[0].Hash})
		close(ledgerDone)
	}()
	dingotestutil.RequireReceive(
		t, ledgerDone, 2*time.Second,
		"ledger calls complete while a capacity read is in flight",
	)

	blocking.Store(false)
	close(release)
	require.NoError(
		t,
		dingotestutil.RequireReceive(t, admitted, 2*time.Second, "admission finishes"),
	)
	require.Len(t, pool.Transactions(), 1)
}

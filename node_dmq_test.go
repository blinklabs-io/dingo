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

package dingo

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/ledger"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestConfigPopulateDMQNetworkMagic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		dmq       internalconfig.DMQConfig
		network   string
		wantMagic uint32
		wantErr   string
	}{
		{
			name:    "disabled needs no magic",
			dmq:     internalconfig.DMQConfig{Topic: "mithril"},
			network: "devnet",
		},
		{
			name: "mithril mainnet",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network:   "mainnet",
			wantMagic: 2912307721,
		},
		{
			name: "mithril preprod",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network:   "preprod",
			wantMagic: 2147483649,
		},
		{
			name: "mithril preview",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network:   "preview",
			wantMagic: 2147483650,
		},
		{
			name: "explicit magic wins",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril", NetworkMagic: 42,
			},
			network:   "mainnet",
			wantMagic: 42,
		},
		{
			name: "network without a default",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "mithril",
			},
			network: "devnet",
			wantErr: "set dmq.networkMagic",
		},
		{
			name: "unknown topic",
			dmq: internalconfig.DMQConfig{
				Enabled: true, Topic: "other",
			},
			network: "mainnet",
			wantErr: `no network magic for topic "other"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := NewConfig()
			cfg.cfg.Network = tt.network
			cfg.cfg.DMQ = tt.dmq
			n := &Node{config: cfg}
			err := n.configPopulateDMQNetworkMagic()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantMagic, n.config.cfg.DMQ.NetworkMagic)
		})
	}
}

func TestDMQStakeAuthorityWithoutLedger(t *testing.T) {
	t.Parallel()
	_, err := (&dmqStakeAuthority{}).PoolActiveStake(
		ocommon.PoolKeyHash{},
	)
	require.ErrorContains(t, err, "ledger state unavailable")
}

// A live truncate closes and replaces the ledger state while DMQ submissions
// keep looking up pool stake; the lookups must never touch a ledger state
// being swapped, and must follow the rebuilt one afterwards.
func TestDMQStakeAuthorityAcrossLiveTruncate(t *testing.T) {
	t.Parallel()
	const numBlocks = 6
	n, points := newLiveLifecycleTestNode(t, numBlocks)
	auth := &n.dmqStake
	auth.setLedgerState(n.ledgerState)
	stop := make(chan struct{})
	started := make(chan struct{})
	var lookups atomic.Int64
	var wg sync.WaitGroup
	// Only the detached state may fail a lookup: any other error means a
	// lookup reached storage that was closed or not yet reopened.
	var lookupErr error
	wg.Go(func() {
		for {
			_, err := auth.PoolActiveStake(ocommon.PoolKeyHash{})
			if err != nil && err.Error() != "ledger state unavailable" {
				lookupErr = err
				return
			}
			if lookups.Add(1) == 1 {
				close(started)
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	})
	<-started
	targetSlot := points[numBlocks/2].Slot
	before := lookups.Load()
	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	during := lookups.Load() - before
	close(stop)
	wg.Wait()
	require.NoError(t, err)
	require.NoError(t, lookupErr)
	require.Positive(t, during, "no stake lookup ran during the truncate")
	_, err = auth.PoolActiveStake(ocommon.PoolKeyHash{})
	require.NoError(t, err)
}

// Quiesce must not wait without bound on a stake lookup stalled inside the
// ledger: a lookup that never returns may still be reading the storage about
// to be closed, so it escalates like any other unconfirmed drain.
func TestQuiesceBoundsDMQStakeDetach(t *testing.T) {
	t.Parallel()
	n := &Node{}
	n.dmqStake.setLedgerState(&ledger.LedgerState{})
	n.config.cfg = &internalconfig.Config{ShutdownTimeout: "20ms"}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	// A lookup in flight holds the read side for as long as it runs.
	n.dmqStake.mu.RLock()
	t.Cleanup(n.dmqStake.mu.RUnlock)

	done := make(chan error, 1)
	go func() { done <- n.quiesceForLiveLifecycleOp(context.Background()) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("quiesce waited on a stalled dmq stake lookup without bound")
	}
	require.ErrorIs(t, err, errStorageDrainUnconfirmed)
	require.ErrorContains(t, err, "dmq stake lookups")
}

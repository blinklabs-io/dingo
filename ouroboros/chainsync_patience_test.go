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
	"errors"
	"testing"
	"time"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type patienceRollForwardFixture struct {
	o     *Ouroboros
	state *dchainsync.State
	conn  ouroboros.ConnectionId
	now   time.Time
}

func newPatienceRollForwardFixture(
	t *testing.T,
	eligible bool,
) *patienceRollForwardFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	f := &patienceRollForwardFixture{
		conn: newTestConnId("127.0.0.1:6000", "10.0.0.1:3001"),
		now:  time.Unix(1_700_000_000, 0),
	}
	cfg := dchainsync.DefaultConfig()
	cfg.Patience = dchainsync.PatienceConfig{
		Enabled:  true,
		Capacity: 100,
		Rate:     1,
	}
	cfg.PatienceActiveFunc = func() bool { return true }
	cfg.Now = func() time.Time { return f.now }
	f.state = dchainsync.NewStateWithConfig(bus, nil, cfg)
	require.True(t, f.state.AddClientConnId(f.conn))
	// Start the leak as an earlier accepted header would have.
	f.state.PatienceHeaderAccepted(f.conn, 0, false)
	f.o = newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return eligible
		},
	})
	f.o.chainsyncState = f.state
	return f
}

func (f *patienceRollForwardFixture) rollForward(
	t *testing.T,
	header gledger.BlockHeader,
) {
	t.Helper()
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(1_000_000, []byte("far")),
		BlockNumber: 1_000_000,
	}
	require.NoError(t, f.o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		header,
		tip,
		f.now,
	))
}

// TestRollForwardChargesPatienceOnlyUntilArrival pins the hook placement: the
// peer is charged up to the header's arrival, local work inside the callback
// is free, and an accepted header resumes the leak with a token.
func TestRollForwardChargesPatienceOnlyUntilArrival(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		f.now = f.now.Add(time.Hour)
		return true, nil
	}

	f.now = f.now.Add(40 * time.Second)
	f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))

	tc := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tc)
	require.False(t, tc.Patience.Exhausted)
	require.False(t, tc.Patience.Paused, "an accepted header resumes the leak")
	require.Equal(t, uint64(1), tc.Patience.BestBlockNumber)
	require.InDelta(t, 61, tc.Patience.Tokens, 1e-9)
}

func TestRollForwardGrantsNoPatienceForRejectedHeaders(t *testing.T) {
	t.Parallel()
	t.Run("verification failure", func(t *testing.T) {
		t.Parallel()
		f := newPatienceRollForwardFixture(t, true)
		f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool {
			return true
		}
		f.o.chainSelectionVerifyHeaderCrypto = func(gledger.BlockHeader) error {
			return errors.New("bad vrf")
		}
		f.now = f.now.Add(40 * time.Second)
		f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))
		tc := f.state.GetTrackedClient(f.conn)
		require.Zero(t, tc.Patience.BestBlockNumber)
		require.InDelta(t, 60, tc.Patience.Tokens, 1e-9)
	})
	t.Run("not ingress eligible", func(t *testing.T) {
		t.Parallel()
		f := newPatienceRollForwardFixture(t, false)
		f.now = f.now.Add(40 * time.Second)
		f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))
		tc := f.state.GetTrackedClient(f.conn)
		require.Zero(t, tc.Patience.BestBlockNumber)
		require.True(
			t,
			tc.Patience.Paused,
			"a peer whose headers are not verified is not held to patience",
		)
	})
}

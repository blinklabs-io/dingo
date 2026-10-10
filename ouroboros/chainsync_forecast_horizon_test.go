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
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func beyondForecastHorizonErr() error {
	return fmt.Errorf(
		"header verification deferred: %w",
		ledger.ErrHeaderBeyondForecastHorizon,
	)
}

type forecastHorizonFixture struct {
	o        *Ouroboros
	conn     ouroboros.ConnectionId
	tipCh    <-chan event.Event
	ledgerCh <-chan event.Event
	recycle  <-chan event.Event
}

func newForecastHorizonFixture(
	t *testing.T,
	remote string,
) forecastHorizonFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6020", remote)
	require.True(t, state.AddClientConnId(conn))
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	return forecastHorizonFixture{
		o:        o,
		conn:     conn,
		tipCh:    tipCh,
		ledgerCh: ledgerCh,
		recycle:  recycleCh,
	}
}

func (f forecastHorizonFixture) rollForward(
	t *testing.T,
	header gledger.BlockHeader,
) {
	t.Helper()
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		header,
		ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
	))
}

// A header the ledger cannot forecast is withheld from chain selection and
// from the ledger's header queue, and the peer is not penalized for it:
// unlike other deferred results, it cannot be validated at all yet.
func TestChainsyncClientRollForwardWithholdsHeaderPastForecastHorizon(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.20:3001")
	f.o.chainSelectionVerifyHeaderCrypto = func(gledger.BlockHeader) error {
		return beyondForecastHorizonErr()
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xa1))

	testutil.RequireNoReceive(
		t,
		f.tipCh,
		200*time.Millisecond,
		"a header past the forecast horizon must not extend the peer's candidate",
	)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		200*time.Millisecond,
		"a header past the forecast horizon must not be queued for blockfetch",
	)
	testutil.RequireNoReceive(
		t,
		f.recycle,
		200*time.Millisecond,
		"a header past the forecast horizon is not a peer fault",
	)
}

// The horizon can move back after admission let a header through, for
// example on a ledger rollback. The header then waits for admission again and
// counts once it verifies.
func TestChainsyncClientRollForwardRewaitsForecastHorizonBeforeObserving(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.21:3001")
	var admissions, verifications atomic.Int32
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		admissions.Add(1)
		return true, nil
	}
	f.o.chainSelectionVerifyHeaderCrypto = func(gledger.BlockHeader) error {
		if verifications.Add(1) == 1 {
			return beyondForecastHorizonErr()
		}
		return nil
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xa2))
	require.EqualValues(t, 2, admissions.Load(),
		"a past-horizon verification result must wait for admission again")
	require.EqualValues(t, 2, verifications.Load())

	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"a header verified after the re-wait must be observed",
	)
	testutil.RequireReceive(
		t,
		f.ledgerCh,
		testutil.AsyncWait,
		"a header verified after the re-wait must reach the ledger",
	)
}

// A connection that closes while a header waits in admission must end the
// wait. The client's DoneChan cannot: it closes only after this callback's
// protocol loop returns.
func TestChainsyncAdmissionWaitEndsWhenConnectionCloses(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.22:3001")
	waiting := make(chan struct{})
	f.o.chainsyncHeaderAdmission = func(
		ctx context.Context,
		_ ledger.ChainsyncEvent,
	) (bool, error) {
		close(waiting)
		<-ctx.Done()
		return false, ctx.Err()
	}
	connDone := make(chan any)
	header := newTestBlockHeader(400, 1, 0xa3)
	returned := make(chan error, 1)
	go func() {
		returned <- f.o.chainsyncClientRollForward(
			ochainsync.CallbackContext{
				ConnectionId:       f.conn,
				ConnectionDoneChan: connDone,
			},
			0,
			header,
			ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		)
	}()
	testutil.RequireReceive(
		t,
		waiting,
		testutil.AsyncWait,
		"admission did not start waiting",
	)

	close(connDone)
	err := testutil.RequireReceive(
		t,
		returned,
		5*time.Second,
		"closing the connection must end the admission wait",
	)
	require.ErrorIs(t, err, context.Canceled)
}

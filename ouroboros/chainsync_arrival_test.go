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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// chainsyncClientRollForward retains an explicit decoded-handler entry point
// for package tests. Production registers the raw callback and records arrival
// before decoding; direct decoded tests timestamp at their own call boundary.
func (o *Ouroboros) chainsyncClientRollForward(
	ctx ochainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip ochainsync.Tip,
) error {
	return o.chainsyncClientRollForwardAt(
		ctx,
		blockType,
		blockData,
		tip,
		time.Now(),
	)
}

func TestChainsyncClientRollForwardRecordsHeaderArrival(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	before := time.Now()
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID},
		0,
		header,
		tip,
	))
	after := time.Now()
	evt := testutil.RequireReceive(
		t,
		ledgerCh,
		2*time.Second,
		"roll-forward should publish a ledger ChainSync event",
	)
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.False(t, data.ArrivalTime.Before(before))
	require.False(t, data.ArrivalTime.After(after))
}

func TestChainsyncClientRollForwardRawRecordsArrivalBeforeDecodeWait(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	headerType, raw := conwayHeaderFixtureBytes(t)
	header, err := o.decodeChainsyncHeader(headerType, raw)
	require.NoError(t, err)
	key := hashDecodeInput(headerType, raw)

	// Claim this decode key so the real raw callback has to wait. The arrival
	// timestamp must already be captured before it joins that wait.
	o.headerDecodeCache.mu.Lock()
	o.headerDecodeCache.inFlight[key] = nil
	o.headerDecodeCache.mu.Unlock()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			o.headerDecodeCache.finishDecode(key, header, nil)
		})
	}
	defer release()

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- o.chainsyncClientRollForwardRaw(
			ochainsync.CallbackContext{
				ConnectionId: newTestConnId(
					"127.0.0.1:6000",
					"1.1.1.1:3001",
				),
			},
			headerType,
			raw,
			ochainsync.Tip{},
		)
	}()
	testutil.WaitForCondition(t, func() bool {
		o.headerDecodeCache.mu.Lock()
		defer o.headerDecodeCache.mu.Unlock()
		return len(o.headerDecodeCache.inFlight[key]) == 1
	}, 2*time.Second, "raw callback should wait on the claimed decode")
	releasedAt := time.Now()
	release()
	require.NoError(t, testutil.RequireReceive(
		t,
		resultCh,
		2*time.Second,
		"raw callback should finish after decode release",
	))

	evt := testutil.RequireReceive(
		t,
		ledgerCh,
		2*time.Second,
		"raw roll-forward should publish a ledger ChainSync event",
	)
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.True(t, data.ArrivalTime.Before(releasedAt))
}

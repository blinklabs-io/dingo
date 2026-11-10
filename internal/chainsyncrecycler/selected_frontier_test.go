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

package chainsyncrecycler

import (
	"fmt"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

func deliveredPlateauPeer(
	connId ouroboros.ConnectionId,
	delivered uint64,
	advertised uint64,
) *chainselection.PeerChainTip {
	tip := chainselection.NewPeerChainTip(connId, testTip(delivered, 50), nil)
	tip.UpdateTipWithObserved(
		testTip(advertised, 100),
		testTip(delivered, 50),
		nil,
	)
	return tip
}

func TestTickAdvertisedPlateauOnlyResyncsItsOwnConnection(t *testing.T) {
	t.Parallel()
	for _, awaiting := range []bool{false, true} {
		t.Run(fmt.Sprint(awaiting), func(t *testing.T) {
			t.Parallel()
			conn := testConnId(31)
			other := testConnId(32)
			active := conn
			ledger := &fakeLedger{
				tip:                 testTip(100, 50),
				primaryChainTipSlot: 100,
				atTip:               true,
			}
			state := &fakeChainsyncState{
				tracked: []chainsync.TrackedClient{
					activeClient(conn, 100),
					activeClient(other, 400),
				},
				activeConn: &active,
			}
			tip := deliveredPlateauPeer(conn, 100, 500)
			if awaiting {
				tip = rollbackRegisteredPeer(conn, 100, 500)
			}
			require.Equal(t, awaiting, tip.AwaitingFirstHeader())
			selector := &fakeChainSelector{
				bestPeer: &active,
				peerTips: map[string]*chainselection.PeerChainTip{
					conn.String(): tip,
				},
			}
			pub := newFakePublisher()
			r, _ := newTestRecycler(
				t,
				ledger,
				state,
				selector,
				pub,
				Config{Cooldown: 2 * time.Minute},
			)
			now := time.Now()
			st := newTestTickState(100, now.Add(-25*time.Minute))
			st.lastPrimaryChainTipSlot = 100
			live := LiveComponents{
				Ledger:         ledger,
				ChainsyncState: state,
				ChainSelector:  selector,
			}
			runTickWith(r, st, live, now, 100)
			events := pub.byType(event.ChainsyncResyncEventType)
			require.Len(
				t,
				events,
				1,
				"untrusted advertisement cannot authorize cross-peer realignment",
			)
			resync := events[0].evt.Data.(event.ChainsyncResyncEvent)
			require.True(t, conn == resync.ConnectionId)
			require.Equal(
				t,
				event.ChainsyncResyncReasonLocalTipPlateau,
				resync.Reason,
			)
			st.lastProgressAt = now.Add(-25 * time.Minute)
			runTickWith(r, st, live, now.Add(time.Second), 100)
			require.Len(
				t,
				pub.byType(event.ChainsyncResyncEventType),
				1,
				"self-recovery remains rate limited",
			)
		})
	}
}

func TestTickDeliveredAdvertisementRespectsExistingSafetyGates(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"at tip", "recent progress", "cooldown", "primary advancing", "applied advancing", "block processing", "ingest held", "backlog", "eagerness", "third party", "replacement"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			conn := testConnId(33)
			active := conn
			advertised := uint64(1000)
			now := time.Now()
			ledger := &fakeLedger{
				tip:                 testTip(100, 50),
				primaryChainTipSlot: 100,
				atTip:               true,
			}
			st := newTestTickState(100, now.Add(-25*time.Minute))
			st.lastPrimaryChainTipSlot = 100
			selector := &fakeChainSelector{
				bestPeer: &conn,
				paused:   map[string]bool{},
			}
			switch name {
			case "at tip":
				advertised = 100
			case "recent progress":
				st.lastProgressAt = now.Add(-time.Second)
			case "cooldown":
				st.lastRecycled[conn.String()] = now.Add(-time.Minute)
			case "primary advancing":
				ledger.primaryChainTipSlot = 200
			case "applied advancing":
				st.lastProgressSlot = 99
			case "block processing":
				ledger.blockProcessingSeen = true
				ledger.blockProcessingSlot = 101
			case "ingest held":
				ledger.held = true
			case "backlog":
				ledger.primaryChainTipSlot = 900
				st.lastPrimaryChainTipSlot = 900
			case "eagerness":
				selector.paused[conn.String()] = true
			case "third party":
				active = testConnId(34)
			case "replacement":
				active = testConnId(33)
				require.Equal(t, conn.String(), active.String())
				require.False(t, conn == active)
			}
			tip := deliveredPlateauPeer(conn, 100, advertised)
			require.False(t, tip.AwaitingFirstHeader())
			selector.peerTips = map[string]*chainselection.PeerChainTip{
				conn.String(): tip,
			}
			state := &fakeChainsyncState{
				tracked: []chainsync.TrackedClient{
					activeClient(active, 100),
				},
				activeConn: &active,
			}
			pub := newFakePublisher()
			r, _ := newTestRecycler(
				t,
				ledger,
				state,
				selector,
				pub,
				Config{Cooldown: 2 * time.Minute},
			)
			runTickWith(
				r,
				st,
				LiveComponents{
					Ledger:         ledger,
					ChainsyncState: state,
					ChainSelector:  selector,
				},
				now,
				100,
			)
			require.Empty(t, pub.byType(event.ChainsyncResyncEventType))
		})
	}
}

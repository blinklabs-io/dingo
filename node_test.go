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
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNodeTestConnId(id uint) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 6000,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: int(id),
		},
	}
}

func TestHandleChainSwitchEventUpdatesActiveConnection(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.DefaultConfig(),
	)
	connA := newNodeTestConnId(3001)
	connB := newNodeTestConnId(3002)
	state.AddClientConnId(connA)
	state.AddClientConnId(connB)
	state.SetClientConnId(connA)
	pointA := ocommon.NewPoint(100, []byte("hash-a"))
	pointB := ocommon.NewPoint(200, []byte("hash-b"))
	tipA := ochainsync.Tip{Point: pointA, BlockNumber: 10}
	tipB := ochainsync.Tip{Point: pointB, BlockNumber: 20}
	state.UpdateClientTip(connA, pointA, tipA)
	state.UpdateClientTip(connB, pointB, tipB)
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
	}

	n.handleChainSwitchEvent(
		event.NewEvent(
			chainselection.ChainSwitchEventType,
			chainselection.ChainSwitchEvent{
				PreviousConnectionId: connA,
				NewConnectionId:      connB,
				NewTip:               tipB,
			},
		),
	)

	active := state.GetClientConnId()
	require.NotNil(t, active)
	assert.Equal(t, connB, *active)
	assert.Equal(t, pointA, state.GetTrackedClient(connA).Cursor)
	assert.Equal(t, pointB, state.GetTrackedClient(connB).Cursor)
	assert.Equal(t, uint64(1), state.GetTrackedClient(connA).HeadersRecv)
	assert.Equal(t, uint64(1), state.GetTrackedClient(connB).HeadersRecv)
}

func TestRunStallCheckerTickRecoversAndAllowsFutureTicks(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	var ticks atomic.Int32

	assert.NotPanics(t, func() {
		n.runStallCheckerTick(func() {
			ticks.Add(1)
			panic("boom")
		})
	})

	n.runStallCheckerTick(func() {
		ticks.Add(1)
	})

	assert.Equal(t, int32(2), ticks.Load())
}

func TestRunStallCheckerLoopRecoversAndSupportsRestart(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	var attempts atomic.Int32

	assert.NotPanics(t, func() {
		for {
			recovered := n.runStallCheckerLoop(func() {
				if attempts.Add(1) == 1 {
					panic("boom")
				}
			})
			if !recovered {
				return
			}
		}
	})

	assert.Equal(t, int32(2), attempts.Load())
}

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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// Every range request, served or rejected, must return its point-lookup slot.
// A leaked slot is invisible until the ceiling is reached, after which the
// server answers NoBlocks to every peer, so drive more requests than the
// ceiling holds and require each served range to keep being served.
func TestBlockfetchServerRequestRange_ReleasesLookupSlots(t *testing.T) {
	t.Parallel()

	ledgerState := newTestLedgerState(t)
	blocks, err := testfixtures.GenerateConwayChain(1)
	require.NoError(t, err)
	require.NoError(t, ledgerState.Chain().
		AddBlock(context.Background(), blocks[0], nil))
	served := ocommon.NewPoint(blocks[0].SlotNumber(), blocks[0].Hash().Bytes())
	missingHash := make([]byte, 32)
	copy(missingHash, "missing-start")
	missing := ocommon.NewPoint(blocks[0].SlotNumber(), missingHash)

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	manager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger, EventBus: bus},
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, manager.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		Logger: logger, EventBus: bus, ConnManager: manager,
	})
	o.ledgerState = ledgerState
	peer := newRegisteredBlockfetchServerPeer(t, o)

	readType := func() byte {
		t.Helper()
		protocolID, payload := peer.readMessage(t, testutil.AsyncWait)
		require.Equal(t, oblockfetch.ProtocolId, protocolID)
		require.GreaterOrEqual(t, len(payload), 2)
		return payload[1]
	}
	// A served range resets the failed-lookup budget, so alternating keeps
	// the missing-point requests below the stuck-peer close threshold.
	for i := range cap(o.blockfetchLookupSlots) + 1 {
		peer.send(t, oblockfetch.ProtocolId,
			oblockfetch.NewMsgRequestRange(served, served))
		require.Equal(t, byte(oblockfetch.MessageTypeStartBatch), readType(),
			"served range %d rejected: lookup slot leaked", i)
		require.Equal(t, byte(oblockfetch.MessageTypeBlock), readType())
		require.Equal(t, byte(oblockfetch.MessageTypeBatchDone), readType())

		peer.send(t, oblockfetch.ProtocolId,
			oblockfetch.NewMsgRequestRange(missing, missing))
		require.Equal(t, byte(oblockfetch.MessageTypeNoBlocks), readType(),
			"missing range %d", i)
	}
	testutil.WaitForCondition(t, func() bool {
		return len(o.blockfetchLookupSlots) == 0
	}, testutil.AsyncWait, "point-lookup slots held after requests finished")
}

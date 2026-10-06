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

package chainsync_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A peer that keeps repeating the rollback it already sent has not advanced,
// so those messages must not keep its stall clock from expiring.
func TestRepeatedRollbackToCursorDoesNotRefreshStallClock(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.PatienceConfig{})
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	point := ocommon.NewPoint(90, []byte("rollback"))
	tip := ochainsync.Tip{Point: ocommon.NewPoint(110, []byte("tip"))}

	require.True(t, h.state.UpdateClientRollback(conn, point, tip))
	h.advance(chainsync.DefaultStallTimeout * 3 / 4)
	require.True(t, h.state.UpdateClientRollback(conn, point, tip))
	h.advance(chainsync.DefaultStallTimeout / 2)

	assert.Equal(
		t,
		[]ouroboros.ConnectionId{conn},
		h.state.CheckStalledClients(),
		"repeated rollback to the same point must not count as activity",
	)
}

// A rollback to a different point is real movement and still refreshes the
// stall clock.
func TestRollbackToNewPointRefreshesStallClock(t *testing.T) {
	t.Parallel()
	h := newPatienceHarness(t, chainsync.PatienceConfig{})
	conn := newTestConnId(1)
	require.True(t, h.state.AddClientConnId(conn))
	tip := ochainsync.Tip{Point: ocommon.NewPoint(110, []byte("tip"))}

	require.True(t, h.state.UpdateClientRollback(
		conn, ocommon.NewPoint(90, []byte("a")), tip,
	))
	h.advance(chainsync.DefaultStallTimeout * 3 / 4)
	require.True(t, h.state.UpdateClientRollback(
		conn, ocommon.NewPoint(80, []byte("b")), tip,
	))
	h.advance(chainsync.DefaultStallTimeout / 2)

	assert.Empty(t, h.state.CheckStalledClients())
}

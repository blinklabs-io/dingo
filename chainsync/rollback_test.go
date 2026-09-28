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

package chainsync

import (
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/connection"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestUpdateClientRollbackRefreshesActivityAndOwnsHashes(t *testing.T) {
	connID := connection.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 3001},
	}
	oldActivity := time.Unix(1, 0)
	state := NewState(nil, nil)
	state.trackedClients[connID] = &TrackedClient{
		ConnId:       connID,
		LastActivity: oldActivity,
		Status:       ClientStatusStalled,
	}
	point := ocommon.NewPoint(90, []byte("rollback"))
	tip := ochainsync.Tip{Point: ocommon.NewPoint(110, []byte("tip"))}
	require.True(t, state.UpdateClientRollback(connID, point, tip))
	current := state.GetTrackedClient(connID)
	require.True(t, current.LastActivity.After(oldActivity))
	require.Equal(t, ClientStatusSyncing, current.Status)
	point.Hash[0] = 'X'
	tip.Point.Hash[0] = 'X'
	current = state.GetTrackedClient(connID)
	require.Equal(t, []byte("rollback"), current.Cursor.Hash)
	require.Equal(t, []byte("tip"), current.Tip.Point.Hash)
}

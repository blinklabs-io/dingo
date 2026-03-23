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
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ouroboros "github.com/blinklabs-io/gouroboros"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
)

func TestKeepaliveClientResponsePublishesPeerActivity(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	_, evtCh := bus.Subscribe(chainselection.PeerActivityEventType)
	o := NewOuroboros(OuroborosConfig{EventBus: bus})
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 3001},
	}

	err := o.keepaliveClientResponse(
		okeepalive.CallbackContext{ConnectionId: connId},
		42,
	)
	require.NoError(t, err)

	select {
	case evt := <-evtCh:
		activityEvt, ok := evt.Data.(chainselection.PeerActivityEvent)
		require.True(t, ok, "expected PeerActivityEvent")
		assert.Equal(t, connId.String(), activityEvt.ConnectionId.String())
	default:
		t.Fatal("expected peer activity event")
	}
}

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

package connmanager

import (
	"net"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
)

func TestNormalizePeerAddr(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ipv4 preserved",
			input:    "1.2.3.4:3001",
			expected: "1.2.3.4:3001",
		},
		{
			name:     "ipv6 canonicalized",
			input:    "[2001:0db8::1]:3001",
			expected: "[2001:db8::1]:3001",
		},
		{
			name:     "hostname lowercased",
			input:    "Relay.Example.Com:3001",
			expected: "relay.example.com:3001",
		},
		{
			name:     "invalid hostport lowercased",
			input:    "Relay.Example.Com",
			expected: "relay.example.com",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, NormalizePeerAddr(test.input))
		})
	}
}

func TestHasInboundPeerAddress(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		OutboundSourcePort: 3001,
	})
	cm.connections[ouroboros.ConnectionId{}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: true,
		peerAddr:  "1.2.3.4:3001",
	}
	cm.inboundPeerAddrs[NormalizePeerAddr("1.2.3.4:3001")]++
	cm.connections[ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 3002},
	}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: true,
		peerAddr:  "1.2.3.4:3002",
	}
	cm.inboundPeerAddrs[NormalizePeerAddr("1.2.3.4:3002")]++
	cm.connections[ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 3999},
	}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: false,
		peerAddr:  "1.2.3.4:3001",
	}
	cm.connections[ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 3003},
	}] = &connectionInfo{
		conn:      nil,
		isInbound: true,
		peerAddr:  "1.2.3.4:3003",
	}

	assert.True(t, cm.HasInboundPeerAddress("1.2.3.4:3001"))
	assert.True(t, cm.HasInboundPeerAddress("1.2.3.4:3002"))
	assert.False(t, cm.HasInboundPeerAddress("1.2.3.4:3999"))
	assert.False(t, cm.HasInboundPeerAddress("1.2.3.4:3003"))
}

func TestHasInboundPeerAddressDisabledWithoutPortReuse(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{})
	cm.connections[ouroboros.ConnectionId{}] = &connectionInfo{
		conn:      &ouroboros.Connection{},
		isInbound: true,
		peerAddr:  "1.2.3.4:3001",
	}
	cm.inboundPeerAddrs[NormalizePeerAddr("1.2.3.4:3001")]++

	assert.False(t, cm.HasInboundPeerAddress("1.2.3.4:3001"))
}

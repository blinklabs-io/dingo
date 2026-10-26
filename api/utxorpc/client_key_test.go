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

package utxorpc

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		addr string
		want string
	}{
		{"ipv4 with port", "10.0.0.1:1000", "10.0.0.1"},
		{"ipv4 other host", "10.0.0.2:1000", "10.0.0.2"},
		{"ipv4 without port", "10.0.0.1", "10.0.0.1"},
		{"ipv4-mapped keeps full address", "[::ffff:1.2.3.4]:80", "1.2.3.4"},
		{"ipv4-mapped other host", "[::ffff:1.2.3.5]:80", "1.2.3.5"},
		{"ipv6 host and port", "[2001:db8:1:2:aaaa::1]:443", "2001:db8:1:2::/64"},
		{"ipv6 same /64 other host", "[2001:db8:1:2:bbbb::9]:443", "2001:db8:1:2::/64"},
		{"ipv6 different /64", "[2001:db8:1:3::1]:443", "2001:db8:1:3::/64"},
		{"ipv6 without port", "2001:db8:1:2::7", "2001:db8:1:2::/64"},
		{"ipv6 zone stripped", "[fe80::1%eth0]:443", "fe80::/64"},
		{"ipv6 zone same as no zone", "[fe80::1]:443", "fe80::/64"},
		{"garbage falls back to raw", "not an address", "not an address"},
		{"empty falls back to raw", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, clientKey(tc.addr))
		})
	}
}

func TestStreamLimiterSharesCapAcrossIPv6Prefix(t *testing.T) {
	t.Parallel()

	l := newStreamLimiter(10, 2)
	_, err := l.acquire("[2001:db8:1:2::1]:1000")
	require.NoError(t, err)
	_, err = l.acquire("[2001:db8:1:2::2]:1000")
	require.NoError(t, err)

	_, err = l.acquire("[2001:db8:1:2:ffff::3]:1000")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	// A different /64 has its own allowance.
	_, err = l.acquire("[2001:db8:1:3::1]:1000")
	require.NoError(t, err)
}

func TestReplayLimiterSharesSlotsAcrossIPv6Prefix(t *testing.T) {
	t.Parallel()

	l := newReplayLimiter(10, 1, 1000, 100, time.Hour)
	_, err := l.acquireSlot("[2001:db8:1:2::1]:1000")
	require.NoError(t, err)

	_, err = l.acquireSlot("[2001:db8:1:2::2]:1000")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	_, err = l.acquireSlot("[2001:db8:1:3::1]:1000")
	require.NoError(t, err)
}

func TestReplayLimiterSharesBudgetAcrossIPv6Prefix(t *testing.T) {
	t.Parallel()

	l := newReplayLimiter(10, 1, 1000, 2, time.Hour)
	now := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return now }
	ctx := context.Background()

	require.NoError(t, l.charge(ctx, "[2001:db8:1:2::1]:1000"))
	require.NoError(t, l.charge(ctx, "[2001:db8:1:2::2]:1000"))

	err := l.charge(ctx, "[2001:db8:1:2::3]:1000")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	require.NoError(t, l.charge(ctx, "[2001:db8:1:3::1]:1000"))
}

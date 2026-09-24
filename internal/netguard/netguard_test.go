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

package netguard

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsBlockedIP(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		ip      string
		blocked bool
	}{
		{ip: "93.184.216.34"},
		{ip: "10.0.0.1", blocked: true},
		{ip: "100.64.0.1", blocked: true},
		{ip: "198.18.0.1", blocked: true},
		{ip: "192.0.0.1", blocked: true},
		{ip: "240.0.0.1", blocked: true},
		{ip: "255.255.255.255", blocked: true},
		{ip: "::ffff:127.0.0.1", blocked: true},
		{ip: "64:ff9b::7f00:1", blocked: true},
		{ip: "64:ff9b:1::1", blocked: true},
		{ip: "2001:db8::1", blocked: true},
	} {
		t.Run(test.ip, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.blocked, IsBlockedIP(net.ParseIP(test.ip)))
		})
	}
}

func TestDialContextRejectsMixedDNSAnswersBeforeDial(t *testing.T) {
	t.Parallel()
	var dialed atomic.Bool
	_, err := DialContext(
		context.Background(),
		"tcp",
		"public.example:443",
		func(context.Context, string, string) (net.Conn, error) {
			dialed.Store(true)
			return nil, errors.New("unexpected dial")
		},
		func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{
				{IP: net.ParseIP("93.184.216.34")},
				{IP: net.ParseIP("127.0.0.1")},
			}, nil
		},
	)
	require.ErrorIs(t, err, ErrBlockedDestination)
	require.False(t, dialed.Load())
}

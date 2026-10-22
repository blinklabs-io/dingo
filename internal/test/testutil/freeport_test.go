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

package testutil

import (
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// lowestEphemeralPort is the first port the host assigns to an unbound
// socket: Linux reports its range in procfs, and macOS and Windows both
// start at 49152, above the 32768 the test caps at regardless.
func lowestEphemeralPort(t *testing.T) int {
	t.Helper()
	raw, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 32768
	}
	first, err := strconv.Atoi(strings.Fields(string(raw))[0])
	require.NoError(t, err)
	return min(first, 32768)
}

// TestFreePortIsOutsideTheEphemeralRange pins why FreePort exists. A test
// that stops a server and then rebinds or probes its port needs nothing else
// to take that port in between, and every other process on the host takes
// ports from the ephemeral range for its outgoing connections and port-0
// listeners. A port drawn from that range was taken by a concurrent package
// between a Stop and the rebind it was testing.
func TestFreePortIsOutsideTheEphemeralRange(t *testing.T) {
	t.Parallel()
	limit := lowestEphemeralPort(t)
	for range 50 {
		_, portStr, err := net.SplitHostPort(FreePort(t))
		require.NoError(t, err)
		port, err := strconv.Atoi(portStr)
		require.NoError(t, err)
		require.Less(t, port, limit, "FreePort handed out an ephemeral port")
		require.GreaterOrEqual(t, port, 1024)
	}
}

// TestFreePortNeverRepeatsInAProcess guards the other way two tests can meet
// on one port: parallel tests in one binary each asking for a port before
// either has bound it.
func TestFreePortNeverRepeatsInAProcess(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{})
	for range 400 {
		addr := FreePort(t)
		require.NotContains(t, seen, addr)
		seen[addr] = struct{}{}
	}
}

// TestFreePortSkipsAPortServedOnTheWildcardAddress guards against handing out
// a port another process serves on every interface. macOS lets a loopback
// bind succeed alongside that wildcard listener, so a probe of the loopback
// address alone reports the port free, and a test that later dials it to
// prove its own server stopped reaches the other process instead.
func TestFreePortSkipsAPortServedOnTheWildcardAddress(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	port := ln.Addr().(*net.TCPAddr).Port

	addr, err := pickFreePort(port, port+1)
	require.Error(
		t, err,
		"a port served on the wildcard address was handed out as %s", addr,
	)
}

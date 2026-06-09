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

	ouroboros_conn "github.com/blinklabs-io/gouroboros/connection"
)

func TestSameConnectionIdHandlesPartialNilAddrs(t *testing.T) {
	remoteAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001}
	remoteOnly := ouroboros_conn.ConnectionId{RemoteAddr: remoteAddr}
	remoteOnlySame := ouroboros_conn.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
	remoteOnlyOther := ouroboros_conn.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	localOnly := ouroboros_conn.ConnectionId{LocalAddr: remoteAddr}

	if !sameConnectionId(remoteOnly, remoteOnly) {
		t.Fatal("sameConnectionId() = false, want true for identical remote-only ids")
	}
	if !sameConnectionId(remoteOnly, remoteOnlySame) {
		t.Fatal("sameConnectionId() = false, want true for equal remote-only ids")
	}
	if sameConnectionId(remoteOnly, remoteOnlyOther) {
		t.Fatal("sameConnectionId() = true, want false for differing remote-only ids")
	}
	if sameConnectionId(remoteOnly, localOnly) {
		t.Fatal("sameConnectionId() = true, want false for remote-only vs local-only")
	}
	if sameConnectionId(remoteOnly, ouroboros_conn.ConnectionId{}) {
		t.Fatal("sameConnectionId() = true, want false for remote-only vs zero")
	}
	if sameConnectionId(remoteOnly, testConnId()) {
		t.Fatal("sameConnectionId() = true, want false for remote-only vs full id")
	}
}

func TestSameConnectionIdZeroValues(t *testing.T) {
	if !sameConnectionId(
		ouroboros_conn.ConnectionId{},
		ouroboros_conn.ConnectionId{},
	) {
		t.Fatal("sameConnectionId() = false, want true for zero values")
	}
	if sameConnectionId(ouroboros_conn.ConnectionId{}, testConnId()) {
		t.Fatal("sameConnectionId() = true, want false for zero vs real")
	}
	if sameConnectionId(testConnId(), ouroboros_conn.ConnectionId{}) {
		t.Fatal("sameConnectionId() = true, want false for real vs zero")
	}
}

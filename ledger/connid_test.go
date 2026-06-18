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

package ledger

import (
	"net"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

func TestSameConnectionIdHandlesPartialNilAddrs(t *testing.T) {
	remoteAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001}
	remoteOnly := ouroboros.ConnectionId{RemoteAddr: remoteAddr}
	remoteOnlySame := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
	remoteOnlyOther := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	localOnly := ouroboros.ConnectionId{LocalAddr: remoteAddr}
	fullId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}

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
	if sameConnectionId(remoteOnly, ouroboros.ConnectionId{}) {
		t.Fatal("sameConnectionId() = true, want false for remote-only vs zero")
	}
	if sameConnectionId(remoteOnly, fullId) {
		t.Fatal("sameConnectionId() = true, want false for remote-only vs full id")
	}
}

func TestConnIdKeyHandlesPartialNilAddrs(t *testing.T) {
	remoteOnly := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
	localOnly := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}

	if connIdKey(ouroboros.ConnectionId{}) != "" {
		t.Fatal("connIdKey() != \"\" for zero value")
	}
	if connIdKey(remoteOnly) == "" {
		t.Fatal("connIdKey() = \"\" for remote-only id, want non-empty")
	}
	if connIdKey(remoteOnly) == connIdKey(localOnly) {
		t.Fatal("connIdKey() equal for remote-only vs local-only, want distinct")
	}
}

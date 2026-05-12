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
	"strings"
	"testing"
)

func FuzzNormalizePeerAddr(f *testing.F) {
	f.Add("")
	f.Add("Example.COM:3001")
	f.Add("127.000.000.001:3001")
	f.Add("[0:0:0:0:0:0:0:1]:3001")
	f.Add("malformed:address:with:colons")

	f.Fuzz(func(t *testing.T, peerAddr string) {
		normalized := NormalizePeerAddr(peerAddr)
		if NormalizePeerAddr(normalized) != normalized {
			t.Fatalf("NormalizePeerAddr is not idempotent: %q -> %q",
				normalized,
				NormalizePeerAddr(normalized),
			)
		}

		inputHost, inputPort, err := net.SplitHostPort(peerAddr)
		if err != nil {
			if normalized != strings.ToLower(peerAddr) {
				t.Fatalf("malformed address normalized to %q, want lowercase %q",
					normalized,
					strings.ToLower(peerAddr),
				)
			}
			return
		}

		host, port, err := net.SplitHostPort(normalized)
		if err != nil {
			t.Fatalf("normalized address is not host:port: %q", normalized)
		}
		if port != inputPort {
			t.Fatalf("normalized port = %q, want %q", port, inputPort)
		}
		if net.ParseIP(inputHost) == nil && host != strings.ToLower(inputHost) {
			t.Fatalf("normalized hostname = %q, want %q", host, strings.ToLower(inputHost))
		}
	})
}

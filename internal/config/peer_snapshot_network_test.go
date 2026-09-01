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

package config

import "testing"

func TestPeerSnapshotNetworkMismatch(t *testing.T) {
	for _, tt := range []struct {
		name          string
		snapshotMagic uint32
		networkMagic  uint32
		want          bool
	}{
		{
			name:          "different networks mismatch",
			snapshotMagic: 764824073,
			networkMagic:  2,
			want:          true,
		},
		{
			name:          "same network does not",
			snapshotMagic: 2,
			networkMagic:  2,
		},
		{
			// A snapshot that omits the field must not be rejected on the
			// strength of an absent value; no real network uses magic 0.
			name:         "unset snapshot magic is unspecified",
			networkMagic: 2,
		},
		{
			// Startup resolves the magic from the network name before
			// validating, so this is defensive rather than reachable.
			name:          "unset node magic is unspecified",
			snapshotMagic: 2,
		},
		{
			name: "both unset",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := PeerSnapshotNetworkMismatch(
				tt.snapshotMagic,
				tt.networkMagic,
			)
			if got != tt.want {
				t.Fatalf(
					"PeerSnapshotNetworkMismatch(%d, %d) = %v, want %v",
					tt.snapshotMagic,
					tt.networkMagic,
					got,
					tt.want,
				)
			}
		})
	}
}

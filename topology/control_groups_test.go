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

package topology

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerControlGroupRejectsMalformedTrustMetadata(t *testing.T) {
	t.Parallel()
	for _, g := range []PeerControlGroup{
		{}, {Identity: " padded ", AccessPoints: []TopologyConfigP2PAccessPoint{{Address: "relay.example", Port: 3001}}},
		{Identity: "operator"},
		{Identity: "operator", AccessPoints: []TopologyConfigP2PAccessPoint{{Address: "host:3001", Port: 3001}}},
		{Identity: "operator", AccessPoints: []TopologyConfigP2PAccessPoint{{Address: "0.0.0.0", Port: 3001}}},
		{Identity: "operator", AccessPoints: []TopologyConfigP2PAccessPoint{{Address: "relay.example", Port: 0}}},
	} {
		require.Error(t, g.Validate())
	}
}

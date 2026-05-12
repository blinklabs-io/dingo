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

package topology_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/topology"
)

func FuzzNewTopologyConfigFromReader(f *testing.F) {
	for _, tc := range topologyTests {
		f.Add(tc.jsonData)
	}
	f.Add(`{"localRoots":[],"publicRoots":[],"bootstrapPeers":[],"useLedgerAfterSlot":0}`)

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 1024*1024 {
			t.Skip("topology corpus input is too large for fast fuzzing")
		}

		cfg, err := topology.NewTopologyConfigFromReader(strings.NewReader(input))
		if err != nil {
			return
		}
		if cfg == nil {
			return
		}

		encoded, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("json.Marshal(parsed topology): %v", err)
		}

		roundTrip, err := topology.NewTopologyConfigFromReader(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("NewTopologyConfigFromReader(marshaled topology): %v", err)
		}
		if roundTrip == nil {
			return
		}
		if !reflect.DeepEqual(roundTrip, cfg) {
			t.Fatalf("round-trip topology = %#v, want %#v", roundTrip, cfg)
		}
	})
}

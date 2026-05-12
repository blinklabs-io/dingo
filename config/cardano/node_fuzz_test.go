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

package cardano

import (
	"bytes"
	"testing"

	"gopkg.in/yaml.v3"
)

func FuzzNewCardanoNodeConfigFromReader(f *testing.F) {
	f.Add("")
	f.Add("Protocol: Cardano\nRequiresNetworkMagic: RequiresMagic\n")
	f.Add("PeerSharing: true\nTargetNumberOfActivePeers: 20\n")

	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 64*1024 {
			t.Skip("config input is too large for fast fuzzing")
		}

		cfg, err := NewCardanoNodeConfigFromReader(bytes.NewBufferString(data))
		if err != nil {
			return
		}
		if cfg == nil {
			t.Fatalf("NewCardanoNodeConfigFromReader returned nil without an error")
		}

		encoded, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatalf("yaml.Marshal(parsed config): %v", err)
		}
		if _, err := NewCardanoNodeConfigFromReader(bytes.NewReader(encoded)); err != nil {
			t.Fatalf("reparse marshaled config: %v", err)
		}
	})
}

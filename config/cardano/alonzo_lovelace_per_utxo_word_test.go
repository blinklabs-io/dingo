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
	"path/filepath"
	"testing"
)

// TestAlonzoLovelacePerUtxoWord pins both halves of the contract the
// database.Config construction sites rely on: a site holding only a network
// resolves the real genesis word, and every unavailable shape resolves zero
// rather than a plausible-looking wrong number, since zero is what
// database.Config reads as "not supplied" and answers with a resync instead
// of an in-place repair.
func TestAlonzoLovelacePerUtxoWord(t *testing.T) {
	t.Parallel()

	for _, network := range []string{"preview", "preprod", "mainnet"} {
		if got := AlonzoLovelacePerUtxoWord(nil, "", network); got == 0 {
			t.Errorf("network %q resolved no Alonzo genesis word", network)
		}
	}

	loaded, err := LoadCardanoNodeConfigWithFallback(
		EmbeddedConfigPath("preview"),
		"preview",
		EmbeddedConfigFS,
	)
	if err != nil {
		t.Fatalf("load preview config: %v", err)
	}
	fromConfig := AlonzoLovelacePerUtxoWord(loaded, "", "")
	fromNetwork := AlonzoLovelacePerUtxoWord(nil, "", "preview")
	if fromConfig != fromNetwork {
		t.Errorf(
			"already-loaded config gave %d, loading by network gave %d",
			fromConfig,
			fromNetwork,
		)
	}

	unavailable := []struct {
		name    string
		cfgPath string
		network string
	}{
		{"nothing to load from", "", ""},
		{"unknown network", "", "nosuchnetwork"},
		{
			"path that is neither on disk nor embedded",
			filepath.Join(t.TempDir(), "absent.json"),
			"preview",
		},
	}
	for _, tt := range unavailable {
		if got := AlonzoLovelacePerUtxoWord(nil, tt.cfgPath, tt.network); got != 0 {
			t.Errorf("%s: got %d, want 0", tt.name, got)
		}
	}
}

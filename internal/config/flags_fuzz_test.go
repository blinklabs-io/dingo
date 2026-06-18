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

import (
	"strings"
	"testing"
)

func FuzzNormalizeRunMode(f *testing.F) {
	f.Add("")
	f.Add("serve")
	f.Add("LOAD")
	f.Add("dev")
	f.Add("leios")

	f.Fuzz(func(t *testing.T, value string) {
		normalized, err := normalizeRunMode(value)
		if err != nil {
			return
		}
		if normalized != strings.ToLower(value) {
			t.Fatalf("normalizeRunMode(%q) = %q, want lowercase input", value, normalized)
		}
		switch normalized {
		case "", string(RunModeServe), string(RunModeLoad), string(RunModeDev), string(RunModeLeios):
		default:
			t.Fatalf("normalizeRunMode accepted unknown mode %q", normalized)
		}
	})
}

func FuzzNormalizeStartEra(f *testing.F) {
	f.Add("")
	f.Add("dijkstra")
	f.Add("DIJKSTRA")

	f.Fuzz(func(t *testing.T, value string) {
		normalized, err := normalizeStartEra(value)
		if err != nil {
			return
		}
		if normalized != strings.ToLower(value) {
			t.Fatalf("normalizeStartEra(%q) = %q, want lowercase input", value, normalized)
		}
		switch normalized {
		case string(StartEraDefault), string(StartEraDijkstra):
		default:
			t.Fatalf("normalizeStartEra accepted unknown era %q", normalized)
		}
	})
}

func FuzzNormalizeStorageMode(f *testing.F) {
	f.Add("")
	f.Add("core")
	f.Add("API")

	f.Fuzz(func(t *testing.T, value string) {
		normalized, err := normalizeStorageMode(value)
		if err != nil {
			return
		}
		if normalized != strings.ToLower(value) {
			t.Fatalf("normalizeStorageMode(%q) = %q, want lowercase input", value, normalized)
		}
		switch normalized {
		case storageModeCore, storageModeAPI:
		default:
			t.Fatalf("normalizeStorageMode accepted unknown mode %q", normalized)
		}
	})
}

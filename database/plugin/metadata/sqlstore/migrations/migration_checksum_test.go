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

package migrations

import "testing"

func TestChecksumIncludesPhaseBoundaries(t *testing.T) {
	base := Migration{
		Version:          1,
		Name:             "v1alpha1",
		BackfillRevision: "none",
		SQL: map[string]SQL{
			"sqlite": {
				Expand:   []string{"one", "two"},
				Contract: []string{"three"},
			},
		},
	}
	moved := base
	moved.SQL = map[string]SQL{
		"sqlite": {Expand: []string{"one"}, Contract: []string{"two", "three"}},
	}
	if base.checksum() == moved.checksum() {
		t.Fatal("moving SQL between migration phases must change checksum")
	}
}

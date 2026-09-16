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

package nodeparity

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEpochLengthSlotsByNetworkMatchesGenesisConfig proves
// epochLengthSlotsByNetwork's hardcoded values match the real
// config/cardano/{preview,preprod}/shelley-genesis.json epochLength fields
// on disk, rather than trusting two independently-typed copies of the same
// number to stay in sync by hand -- this is exactly the class of bug this
// map was added to fix (preprod's epoch length was previously hardcoded to
// preview's).
func TestEpochLengthSlotsByNetworkMatchesGenesisConfig(t *testing.T) {
	for network, wantLength := range epochLengthSlotsByNetwork {
		t.Run(network, func(t *testing.T) {
			path := "../../config/cardano/" + network + "/shelley-genesis.json"
			data, err := os.ReadFile(path)
			require.NoError(t, err, "reading %s", path)

			var genesis struct {
				EpochLength uint64 `json:"epochLength"`
			}
			require.NoError(t, json.Unmarshal(data, &genesis))
			require.Equal(
				t, genesis.EpochLength, wantLength,
				"epochLengthSlotsByNetwork[%q] must match %s's epochLength",
				network, path,
			)
		})
	}
}

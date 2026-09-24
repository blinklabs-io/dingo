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
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const testByronGenesisJSON = `{
  "avvmDistr": {},
  "blockVersionData": {
    "heavyDelThd": "0", "maxBlockSize": "1",
    "maxHeaderSize": "1", "maxProposalSize": "1",
    "maxTxSize": "1", "mpcThd": "0", "scriptVersion": 0,
    "slotDuration": "20000",
    "softforkRule": {"initThd": "0", "minThd": "0", "thdDecrement": "0"},
    "txFeePolicy": {"multiplier": "0", "summand": "0"},
    "unlockStakeEpoch": "0", "updateImplicit": "0",
    "updateProposalThd": "0", "updateVoteThd": "0"
  },
  "protocolConsts": {"k": 432, "protocolMagic": 2},
  "startTime": 0, "bootStakeholders": {},
  "heavyDelegation": {}, "nonAvvmBalances": {}
}`

func testByronGenesisJSONForK(k uint64) string {
	return strings.Replace(
		testByronGenesisJSON,
		`"k": 432`,
		`"k": `+strconv.FormatUint(k, 10),
		1,
	)
}

func completeTestByronGenesisJSON(t testing.TB, input string) string {
	t.Helper()
	var base map[string]json.RawMessage
	var overrides map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(testByronGenesisJSON), &base))
	require.NoError(t, json.Unmarshal([]byte(input), &overrides))
	for key, value := range overrides {
		if key == "blockVersionData" || key == "protocolConsts" {
			var defaults map[string]json.RawMessage
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(base[key], &defaults))
			require.NoError(t, json.Unmarshal(value, &fields))
			for field, fieldValue := range fields {
				defaults[field] = fieldValue
			}
			merged, err := json.Marshal(defaults)
			require.NoError(t, err)
			base[key] = merged
			continue
		}
		base[key] = value
	}
	merged, err := json.Marshal(base)
	require.NoError(t, err)
	return string(merged)
}

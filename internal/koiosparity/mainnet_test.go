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

package koiosparity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStakeAddressFromCredentialUsesNetworkID(t *testing.T) {
	t.Parallel()

	key := make([]byte, 28)
	for _, tc := range []struct {
		network string
		prefix  string
		tag     uint8
	}{
		{"mainnet", "stake1", 0},
		{"mainnet", "stake1", 1},
		{"preview", "stake_test1", 0},
		{"preprod", "stake_test1", 1},
	} {
		addr, err := StakeAddressFromCredential(tc.network, key, tc.tag)
		require.NoError(t, err)
		require.Contains(t, addr, tc.prefix, "%s tag %d", tc.network, tc.tag)
		if tc.prefix == "stake1" {
			require.NotContains(t, addr, "stake_test")
		}
	}

	_, err := StakeAddressFromCredential("sanchonet", key, 0)
	require.Error(t, err)
}

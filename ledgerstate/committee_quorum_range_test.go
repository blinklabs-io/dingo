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

package ledgerstate

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// TestParseCommitteeQuorumRange checks that an imported committee quorum must
// lie in the UnitInterval. A zero quorum stays valid.
func TestParseCommitteeQuorumRange(t *testing.T) {
	t.Parallel()
	encodeCommittee := func(t *testing.T, quorum any) []byte {
		t.Helper()
		// StrictMaybe SJust: [[members_map, quorum]]
		raw, err := cbor.Encode([]any{[]any{map[uint]uint{}, quorum}})
		require.NoError(t, err)
		return raw
	}
	rat := func(n, d int64) any {
		return cbor.Tag{
			Number:  30,
			Content: []any{n, d},
		}
	}
	for _, tc := range []struct {
		name    string
		quorum  any
		want    *big.Rat
		wantErr string
	}{
		{"zero", rat(0, 1), big.NewRat(0, 1), ""},
		{"one half", rat(1, 2), big.NewRat(1, 2), ""},
		{"one", rat(1, 1), big.NewRat(1, 1), ""},
		{"negative", rat(-1, 2), nil, "outside [0,1]"},
		{"above one", rat(3, 2), nil, "outside [0,1]"},
		{"integer above one", rat(2, 1), nil, "outside [0,1]"},
		{"zero denominator", rat(1, 0), nil, "denominator"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, quorum, err := parseCommittee(encodeCommittee(t, tc.quorum))
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				require.Nil(
					t,
					quorum,
					"an out-of-range quorum must not be surfaced to callers",
				)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, quorum)
			require.Zero(t, tc.want.Cmp(quorum.Rat))
		})
	}
}

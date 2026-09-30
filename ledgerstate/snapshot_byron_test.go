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
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// TestParseSnapshotDataRefusesByronEra pins that a ledger state whose current
// era is Byron is refused by name. Importing one would start a node inside
// Byron with no record of the update state that decides its adopted block
// size limits and fee policy.
func TestParseSnapshotDataRefusesByronEra(t *testing.T) {
	t.Parallel()
	bound := []any{uint64(0), uint64(0), uint64(0)}
	// ByronLedgerState: [tip block number, chain validation state, transition].
	byronState := []any{[]any{uint64(5)}, []any{uint64(1)}, uint64(0)}
	header := []any{[]any{}, []any{}}
	telescopes := map[string]any{
		"nested":      []any{uint64(0), []any{bound, byronState}},
		"flat single": []any{[]any{bound, byronState}},
	}
	for name, telescope := range telescopes {
		for _, utxoHD := range []bool{false, true} {
			var outer any = []any{telescope, header}
			if utxoHD {
				outer = []any{uint64(1), outer}
			}
			data, err := cbor.Encode(outer)
			require.NoError(t, err)
			_, err = parseSnapshotData(data)
			require.ErrorIs(
				t, err, ErrByronSnapshotUnsupported,
				"%s telescope, UTxO-HD %v", name, utxoHD,
			)
		}
	}
}

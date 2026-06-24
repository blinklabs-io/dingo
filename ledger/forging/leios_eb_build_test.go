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

package forging

import (
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestBuildLeiosEBBodiesAlignWithRefs verifies that buildLeiosEB returns the
// transaction bodies in the same order as the manifest references, and that a
// transaction dropped from the manifest (invalid hash or size) is dropped from
// the bodies too, so body i stays aligned with reference i.
func TestBuildLeiosEBBodiesAlignWithRefs(t *testing.T) {
	txs := []MempoolTransaction{
		{Hash: strings.Repeat("11", 32), Cbor: []byte{0x01}},
		{Hash: "not-hex", Cbor: []byte{0x02}},            // dropped: bad hash
		{Hash: strings.Repeat("22", 32), Cbor: []byte{}}, // dropped: zero size
		{Hash: strings.Repeat("33", 32), Cbor: []byte{0x03, 0x04}},
	}

	ebCbor, ebHash, bodies, err := buildLeiosEB(txs)
	require.NoError(t, err)
	require.NotEmpty(t, ebHash)

	// Only the two valid transactions survive, in input order.
	require.Len(t, bodies, 2)
	require.Equal(t, []byte{0x01}, bodies[0])
	require.Equal(t, []byte{0x03, 0x04}, bodies[1])

	// The manifest references match the surviving bodies in order and size.
	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(ebCbor)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, len(bodies))
	for i, ref := range eb.TransactionReferences {
		require.Equalf(
			t,
			len(bodies[i]),
			int(ref.TransactionSize),
			"reference %d size matches body length",
			i,
		)
	}
}

// TestBuildLeiosEBNoValidRefs verifies buildLeiosEB returns errNoValidTxRefs
// when no transaction yields a valid reference.
func TestBuildLeiosEBNoValidRefs(t *testing.T) {
	_, _, bodies, err := buildLeiosEB([]MempoolTransaction{
		{Hash: "not-hex", Cbor: []byte{0x01}},
	})
	require.ErrorIs(t, err, errNoValidTxRefs)
	require.Nil(t, bodies)
}

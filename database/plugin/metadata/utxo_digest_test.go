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

package metadata_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func digestOne(t *testing.T, output []byte) metadata.UtxoSetDigest {
	t.Helper()
	d := metadata.NewUtxoDigester()
	d.Add(
		[]byte{1, 2, 3}, 1, "5000000",
		[]byte{4}, nil, nil, true, output,
	)
	return d.Sum()
}

func mustCbor(t *testing.T, v any) []byte {
	t.Helper()
	b, err := cbor.Encode(v)
	require.NoError(t, err)
	return b
}

// TestUtxoDigesterLegacyArrayOutputMatchesMapForm checks that an output
// created in the pre-Babbage array form digests the same as the map form a
// snapshot import stores for it.
func TestUtxoDigesterLegacyArrayOutputMatchesMapForm(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	addr[0] = 0x60
	datumHash := make([]byte, 32)
	datumHash[0] = 0xaa

	cases := map[string]struct{ legacy, mapped []byte }{
		"two element": {
			legacy: mustCbor(t, []any{addr, uint64(5000000)}),
			mapped: mustCbor(t, map[uint64]any{
				0: addr, 1: uint64(5000000),
			}),
		},
		"datum hash": {
			legacy: mustCbor(t, []any{addr, uint64(5000000), datumHash}),
			mapped: mustCbor(t, map[uint64]any{
				0: addr, 1: uint64(5000000),
				2: []any{uint64(0), datumHash},
			}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t, digestOne(t, tc.mapped), digestOne(t, tc.legacy),
				"array and map encodings of one output must digest equal",
			)
		})
	}
}

// TestUtxoDigesterDistinguishesOutputs guards against canonicalisation
// collapsing outputs that differ.
func TestUtxoDigesterDistinguishesOutputs(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	a := mustCbor(t, []any{addr, uint64(1)})
	b := mustCbor(t, []any{addr, uint64(2)})
	require.NotEqual(t, digestOne(t, a), digestOne(t, b))
	require.NotEqual(t, digestOne(t, a), digestOne(t, []byte{0xff}))
}

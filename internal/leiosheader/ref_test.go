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

package leiosheader

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func mustRaw(t *testing.T, value any) cbor.RawMessage {
	t.Helper()
	raw, err := cbor.Encode(value)
	require.NoError(t, err)
	return cbor.RawMessage(raw)
}

func testHash(seed byte) []byte {
	hash := make([]byte, lcommon.Blake2b256Size)
	for i := range hash {
		hash[i] = seed + byte(i)
	}
	return hash
}

func TestReferencedEndorserBlockUsesLeiosAnnouncement(t *testing.T) {
	hash := testHash(0x20)
	header := &dijkstra.DijkstraBlockHeader{
		LeiosHeaderExtension: []cbor.RawMessage{
			mustRaw(t, true),
			mustRaw(t, []any{hash, uint64(4096)}),
		},
	}

	gotHash, gotSize, ok := ReferencedEndorserBlock(header)
	require.True(t, ok)
	require.Equal(t, hash, gotHash.Bytes())
	require.Equal(t, uint64(4096), gotSize)
}

func TestReferencedEndorserBlockUsesLegacyOneFieldExtension(t *testing.T) {
	hash := testHash(0x40)
	header := &dijkstra.DijkstraBlockHeader{
		LeiosHeaderExtension: []cbor.RawMessage{
			mustRaw(t, []any{hash, uint64(8192)}),
		},
	}

	gotHash, gotSize, ok := ReferencedEndorserBlock(header)
	require.True(t, ok)
	require.Equal(t, hash, gotHash.Bytes())
	require.Equal(t, uint64(8192), gotSize)
}

func TestReferencedEndorserBlockRejectsMalformedCurrentExtension(t *testing.T) {
	legacyHash := testHash(0x60)
	header := &dijkstra.DijkstraBlockHeader{
		LeiosHeaderExtension: []cbor.RawMessage{
			mustRaw(t, []any{legacyHash, uint64(8192)}),
			{0xf6},
		},
	}

	_, _, ok := ReferencedEndorserBlock(header)
	require.False(t, ok)
}

func TestReferencedEndorserBlockReturnsFalseWithoutReference(t *testing.T) {
	header := &dijkstra.DijkstraBlockHeader{
		LeiosHeaderExtension: []cbor.RawMessage{
			mustRaw(t, false),
			{0xf6},
		},
	}
	_, _, ok := ReferencedEndorserBlock(header)
	require.False(t, ok)

	_, _, ok = ReferencedEndorserBlock(&babbage.BabbageBlockHeader{})
	require.False(t, ok)
}

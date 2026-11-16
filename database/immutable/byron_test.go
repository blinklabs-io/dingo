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

package immutable

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// preprodGeometry is the geometry cardano-node used for the Byron fixture
// chunks: 21600 slots per chunk and a leading slot for the epoch boundary
// block.
var preprodGeometry = Geometry{ChunkSlots: 21600, CanContainEBB: true}

// The fixture is chunks 0 and 1 of the preprod ImmutableDB as cardano-node
// wrote them: the genesis epoch boundary block and Byron main blocks. Writing
// the same blocks through the Store must reproduce all three files byte for
// byte, which pins the secondary entry's header offset and size, the epoch
// boundary block's relative slot and BlockOrEbb value, and the primary index.
func TestStoreReproducesCardanoNodeByronChunks(t *testing.T) {
	t.Parallel()
	const fixtures = "testdata_byron"
	src, err := New(fixtures)
	require.NoError(t, err)
	dir := t.TempDir()
	s, err := OpenStore(dir, preprodGeometry)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var ebbs, main int
	for n := range 2 {
		it, err := src.BlocksInChunk(uint64(n))
		require.NoError(t, err)
		for {
			b, err := it.Next()
			require.NoError(t, err)
			if b == nil {
				break
			}
			if b.Type == 0 {
				ebbs++
			} else {
				main++
			}
			_, err = s.Append(WriteBlock{
				Hash: b.Hash, Cbor: b.Cbor, Type: b.Type, Slot: b.Slot,
			})
			require.NoError(t, err)
		}
		require.NoError(t, it.Close())
	}
	require.Equal(t, 1, ebbs, "the genesis epoch boundary block")
	require.Positive(t, main)
	require.NoError(t, s.Sync())

	// Chunk 0 is complete once chunk 1 started; chunk 1's primary is padded
	// only when a later chunk begins, so compare its chunk and secondary.
	for _, name := range []string{
		"00000.chunk", "00000.secondary", "00000.primary",
		"00001.chunk", "00001.secondary",
	} {
		want, err := os.ReadFile(filepath.Join(fixtures, name))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, want, got, name)
	}
}

// The fixture must be genuine preprod chain data, not blocks the store wrote
// itself: the genesis epoch boundary block names the preprod Byron genesis
// hash as its predecessor, every later block names the header hash of the one
// before it, and each header hash recomputes from the block bytes.
func TestByronFixtureIsPreprodChain(t *testing.T) {
	t.Parallel()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preprod"), "preprod",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesisHash, err := hex.DecodeString(nodeCfg.ByronGenesisHash)
	require.NoError(t, err)
	require.Len(t, genesisHash, 32)

	src, err := New("testdata_byron")
	require.NoError(t, err)
	prev := genesisHash
	var count int
	for n := range 2 {
		it, err := src.BlocksInChunk(uint64(n))
		require.NoError(t, err)
		for {
			b, err := it.Next()
			require.NoError(t, err)
			if b == nil {
				break
			}
			blk, err := gledger.NewBlockFromCbor(
				b.Type,
				b.Cbor,
				lcommon.VerifyConfig{SkipBodyHashValidation: true},
			)
			require.NoError(t, err)
			require.Equal(t, hex.EncodeToString(prev),
				blk.PrevHash().String(), "block %d prev hash", count)
			require.Equal(t, hex.EncodeToString(b.Hash),
				blk.Hash().String(), "block %d header hash", count)
			prev = b.Hash
			count++
		}
		require.NoError(t, it.Close())
	}
	require.Greater(t, count, 20)
}

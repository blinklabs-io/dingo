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

package blockverify

import (
	"bytes"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

const testSlot = uint64(1000)

func realBlock(t *testing.T) gledger.Block {
	t.Helper()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		testSlot,
		10,
		1,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Equal(t, testSlot, blocks[0].SlotNumber(),
		"fixture invariant: block must be at testSlot")
	return blocks[0]
}

// TestHashAcceptsMatchingContent proves the happy path: the true block CBOR
// against its own hash and slot decodes and verifies.
func TestHashAcceptsMatchingContent(t *testing.T) {
	block := realBlock(t)
	hash := block.Hash()

	decoded, err := Hash(
		gledger.BlockTypeConway,
		testSlot,
		block.Cbor(),
		hash[:],
	)
	require.NoError(t, err)
	require.Equal(t, hash, decoded.Hash())
}

// TestHashRejectsContentMismatch proves a remote store that hands back a
// different (but still validly decodable) block than the one requested is
// caught: the bytes decode cleanly, but the recomputed hash disagrees with
// the hash the caller asked for.
func TestHashRejectsContentMismatch(t *testing.T) {
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		testSlot,
		10,
		2,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	requestedHash := blocks[0].Hash()
	wrongBlockCbor := blocks[1].Cbor()

	_, err = Hash(
		gledger.BlockTypeConway,
		blocks[1].SlotNumber(),
		wrongBlockCbor,
		requestedHash[:],
	)
	require.ErrorIs(t, err, ErrHashMismatch)
}

// TestHashRejectsUndecodableContent proves garbage bytes claiming to be a
// block of a given type are rejected as undecodable rather than panicking
// or being treated as a hash mismatch.
func TestHashRejectsUndecodableContent(t *testing.T) {
	garbage := bytes.Repeat([]byte{0xff}, 16)
	_, err := Hash(gledger.BlockTypeConway, testSlot, garbage, make([]byte, 32))
	require.ErrorIs(t, err, ErrUndecodable)
}

// TestHashRejectsSlotMismatch proves a remote store that hands back the
// genuinely requested block's bytes and hash, but resolved through a key
// naming a different slot than the block's own, is caught: the hash alone
// does not pin the point, so the decoded block's own slot must also match
// what was requested.
func TestHashRejectsSlotMismatch(t *testing.T) {
	block := realBlock(t)
	hash := block.Hash()

	_, err := Hash(
		gledger.BlockTypeConway,
		testSlot+1,
		block.Cbor(),
		hash[:],
	)
	require.ErrorIs(t, err, ErrSlotMismatch)
}

// TestHashRejectsEraMismatch proves the case bark's verifyArchiveBlock
// already defends against: the block hash for Shelley and later eras
// covers the header alone, and adjacent eras share that header layout, so
// the same bytes can decode -- with an identical hash and slot -- under
// more than one era. The hash and slot checks alone cannot catch a caller
// claiming the wrong era, so Hash must independently derive the era from
// the decoded header and reject a disagreement.
func TestHashRejectsEraMismatch(t *testing.T) {
	tests := []struct {
		name        string
		generate    func(uint64, lcommon.Blake2b256, uint64, uint64, int) ([]gledger.Block, error)
		trueType    uint
		claimedType uint
	}{
		{
			name:        "babbage served as conway",
			generate:    fixtures.GenerateBabbageChain,
			trueType:    gledger.BlockTypeBabbage,
			claimedType: gledger.BlockTypeConway,
		},
		{
			name:        "shelley served as mary",
			generate:    fixtures.GenerateShelleyChain,
			trueType:    gledger.BlockTypeShelley,
			claimedType: gledger.BlockTypeMary,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blocks, err := tc.generate(1, lcommon.Blake2b256{}, testSlot, 1, 1)
			require.NoError(t, err)
			require.Len(t, blocks, 1)
			raw := blocks[0].Cbor()

			decoded, err := gledger.NewBlockFromCbor(tc.trueType, raw)
			require.NoError(t, err, "fixture must decode in its genuine era")
			hash := decoded.Hash()

			// The misclassified era must still decode to the same hash and
			// slot, otherwise this test would pass for the wrong reason
			// (caught by the hash or slot check instead of the era check).
			crossDecoded, err := gledger.NewBlockFromCbor(tc.claimedType, raw)
			require.NoError(t, err,
				"cross-era decode must succeed for this test to be meaningful")
			crossHash := crossDecoded.Hash()
			require.Equal(t, hash, crossHash,
				"cross-era decode must hash identically for this test to "+
					"be meaningful")
			require.Equal(t, decoded.SlotNumber(), crossDecoded.SlotNumber(),
				"cross-era decode must report the same slot for this test "+
					"to be meaningful")

			_, err = Hash(
				tc.claimedType,
				decoded.SlotNumber(),
				raw,
				hash[:],
			)
			require.ErrorIs(t, err, ErrTypeMismatch)
		})
	}
}

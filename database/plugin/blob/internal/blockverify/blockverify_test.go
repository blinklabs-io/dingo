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

func realBlock(t *testing.T) gledger.Block {
	t.Helper()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		1000,
		10,
		1,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	return blocks[0]
}

// TestHashAcceptsMatchingContent proves the happy path: the true block CBOR
// against its own hash decodes and verifies.
func TestHashAcceptsMatchingContent(t *testing.T) {
	block := realBlock(t)
	hash := block.Hash()

	decoded, err := Hash(gledger.BlockTypeConway, block.Cbor(), hash[:])
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
		1000,
		10,
		2,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	requestedHash := blocks[0].Hash()
	wrongBlockCbor := blocks[1].Cbor()

	_, err = Hash(gledger.BlockTypeConway, wrongBlockCbor, requestedHash[:])
	require.ErrorIs(t, err, ErrHashMismatch)
}

// TestHashRejectsUndecodableContent proves garbage bytes claiming to be a
// block of a given type are rejected as undecodable rather than panicking
// or being treated as a hash mismatch.
func TestHashRejectsUndecodableContent(t *testing.T) {
	garbage := bytes.Repeat([]byte{0xff}, 16)
	_, err := Hash(gledger.BlockTypeConway, garbage, make([]byte, 32))
	require.ErrorIs(t, err, ErrUndecodable)
}

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

package database

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func payloadTestBlock() models.Block {
	return models.Block{
		Slot: 100, Number: 10, Type: 1,
		Hash: bytes.Repeat([]byte{0xc1}, 32),
		Cbor: []byte("block-prefix|the-datum|block-suffix"),
	}
}

func locatePayloadIn(
	t *testing.T,
	block models.Block,
	payload string,
) []byte {
	t.Helper()
	locator, err := types.NewBlockPayloadLocator(
		block.Slot, block.Hash, block.Cbor,
	)
	require.NoError(t, err)
	offset, err := locator.LocatePayload([]byte(payload))
	require.NoError(t, err)
	return offset
}

// A payload offset is read through the IndexerDB from the block the ledger's
// block store holds.
func TestResolvePayloadReadsTheNamedBlock(t *testing.T) {
	t.Parallel()
	pair := newIndexerTestDB(t, nil)
	block := payloadTestBlock()
	require.NoError(t, pair.ledger.BlockCreate(block, nil))

	got, err := pair.indexer.ResolvePayload(
		locatePayloadIn(t, block, "the-datum"),
	)
	require.NoError(t, err)
	require.Equal(t, []byte("the-datum"), got)
}

// A block a rollback removed from the chain is read from its retained body,
// so a row above the rollback point stays readable until the IndexerDB writer
// has removed it.
func TestResolvePayloadReadsARetainedRolledBackBody(t *testing.T) {
	t.Parallel()
	pair := newIndexerTestDB(t, nil)
	block := payloadTestBlock()
	offset := locatePayloadIn(t, block, "the-datum")

	_, err := pair.indexer.ResolvePayload(offset)
	require.ErrorIs(
		t, err, types.ErrBlobKeyNotFound, "no body is stored yet",
	)

	require.NoError(t, pair.ledger.RetainRolledBackBlock(
		block.Slot, block.Hash, block.Cbor,
	))
	got, err := pair.indexer.ResolvePayload(offset)
	require.NoError(t, err)
	require.Equal(t, []byte("the-datum"), got)
}

func TestResolvePayloadRefusesAnOffsetOutsideTheBlock(t *testing.T) {
	t.Parallel()
	pair := newIndexerTestDB(t, nil)
	block := payloadTestBlock()
	require.NoError(t, pair.ledger.BlockCreate(block, nil))
	offset := CborOffset{
		BlockSlot:  block.Slot,
		ByteOffset: uint32(len(block.Cbor)),
		ByteLength: 4,
	}
	copy(offset.BlockHash[:], block.Hash)

	_, err := pair.indexer.ResolvePayload(offset.Encode())
	require.ErrorContains(t, err, "out of bounds")
	_, err = pair.indexer.ResolvePayload([]byte("not an offset"))
	require.Error(t, err)
}

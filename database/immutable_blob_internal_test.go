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
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// unboundedGetStore offers only the plain Get every blob store has.
type unboundedGetStore struct {
	blob.BlobStore
	value []byte
}

func (s unboundedGetStore) Get(types.Txn, []byte) ([]byte, error) {
	return s.value, nil
}

// The bound applies to every key, including when the store beneath the
// wrapper cannot bound the read itself.
func TestImmutableGetContextLimitedBoundsFallbackRead(t *testing.T) {
	t.Parallel()
	s := &immutableBlobStore{
		BlobStore: unboundedGetStore{value: make([]byte, 24)},
	}
	_, err := s.GetContextLimited(
		context.Background(), nil, []byte("dingo:block-inventory:v1"), 8,
	)
	require.Error(t, err)
	got, err := s.GetContextLimited(
		context.Background(), nil, []byte("dingo:block-inventory:v1"), 24,
	)
	require.NoError(t, err)
	require.Len(t, got, 24)
}

// signingStore is an object store that signs whatever its bp object holds.
type signingStore struct {
	blob.BlobStore
	value  []byte
	signed bool
}

func (s *signingStore) GetContextLimited(
	_ context.Context, _ types.Txn, _ []byte, maxBytes int64,
) ([]byte, error) {
	if int64(len(s.value)) > maxBytes {
		return nil, errors.New("blob object exceeds maximum size")
	}
	return s.value, nil
}

func (s *signingStore) GetBlockURL(
	context.Context, types.Txn, ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	s.signed = true
	return types.SignedURL{}, types.BlockMetadata{}, nil
}

// A promoted block's object holds its marker, so signing it would hand an
// archive client the marker instead of the block.
func TestImmutableGetBlockURLRefusesPromotedBlock(t *testing.T) {
	t.Parallel()
	point := ocommon.Point{Slot: 7, Hash: make([]byte, 32)}
	promoted := &signingStore{value: types.ImmutableMarker{
		Chunk: 1, Offset: 2, Size: 3, CRC: 4,
	}.Encode()}
	_, _, err := (&immutableBlobStore{BlobStore: promoted}).GetBlockURL(
		context.Background(), nil, point,
	)
	require.ErrorIs(t, err, ErrImmutableBlockNotSignable)
	require.False(t, promoted.signed)

	body := &signingStore{value: make([]byte, 64)}
	_, _, err = (&immutableBlobStore{BlobStore: body}).GetBlockURL(
		context.Background(), nil, point,
	)
	require.NoError(t, err)
	require.True(t, body.signed)
}

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
	"fmt"
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
	value   []byte
	readErr error
	signed  bool
}

func (s *signingStore) GetContextLimited(
	_ context.Context, _ types.Txn, _ []byte, maxBytes int64,
) ([]byte, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	if int64(len(s.value)) > maxBytes {
		return nil, fmt.Errorf("%w of %d bytes", types.ErrBlobTooLarge, maxBytes)
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

// A read that fails for any reason other than size says nothing about what
// the object holds, so it must not go on to sign what may be a marker.
func TestImmutableGetBlockURLFailsClosedOnReadError(t *testing.T) {
	t.Parallel()
	point := ocommon.Point{Slot: 7, Hash: make([]byte, 32)}
	readErr := errors.New("injected transient read failure")
	promoted := &signingStore{
		value: types.ImmutableMarker{
			Chunk: 1, Offset: 2, Size: 3, CRC: 4,
		}.Encode(),
		readErr: readErr,
	}
	_, _, err := (&immutableBlobStore{BlobStore: promoted}).GetBlockURL(
		context.Background(), nil, point,
	)
	require.ErrorIs(t, err, readErr)
	require.False(t, promoted.signed)
}

// The wrapper's own bounded read reports an oversized value with the sentinel
// a caller stacked above it tests for.
func TestImmutableGetContextLimitedReportsBlobTooLarge(t *testing.T) {
	t.Parallel()
	s := &immutableBlobStore{
		BlobStore: unboundedGetStore{value: make([]byte, 24)},
	}
	_, err := s.GetContextLimited(
		context.Background(), nil, []byte("dingo:block-inventory:v1"), 8,
	)
	require.ErrorIs(t, err, types.ErrBlobTooLarge)
}

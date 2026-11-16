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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
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

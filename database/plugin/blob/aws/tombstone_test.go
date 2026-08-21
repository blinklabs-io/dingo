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

package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestTombstoneBlockRetainsMetadata(t *testing.T) {
	store, err := NewWithOptions()
	require.NoError(t, err)
	store.client = new(s3.Client)
	txn := store.NewTransaction(true)
	slot := uint64(42)
	hash := []byte("s3-tombstone-metadata")

	require.NoError(t, store.SetBlock(
		txn, slot, hash, []byte{0x80}, 7, 1, 6, nil,
	))
	require.NoError(t, store.TombstoneBlock(txn, slot, hash))

	_, metadata, err := store.GetBlock(txn, slot, hash)
	require.ErrorIs(t, err, types.ErrHistoryExpired)
	require.Equal(t, uint64(7), metadata.ID)
}

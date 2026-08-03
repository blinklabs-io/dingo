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

package server_test

import (
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/midnight/server"
	"github.com/stretchr/testify/require"
)

// TestBlockByNumber_OverflowIsRejected verifies that a block number so large
// it would wrap the blob store's 1-based internal index back to 0 is
// rejected up front rather than silently looking up the wrong block.
func TestBlockByNumber_OverflowIsRejected(t *testing.T) {
	db := newTestDatabase(t)
	adapted := server.NewDatabase(db)

	_, err := adapted.BlockByNumber(math.MaxUint64)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestBlockByNumber_ResolvesInsertedBlock is the non-overflow control case:
// a normal block number round-trips through the 0-based/1-based translation.
func TestBlockByNumber_ResolvesInsertedBlock(t *testing.T) {
	db := newTestDatabase(t)
	blk := insertPlaceholderBlock(t, db, 5, 5, 0x05)
	adapted := server.NewDatabase(db)

	got, err := adapted.BlockByNumber(5)
	require.NoError(t, err)
	require.Equal(t, blk.Hash, got.Hash)
}

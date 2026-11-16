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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// A staged rewrite of a block's nonce keeps a checkpoint flag the view
// already holds, as the stores' upserts do, and takes the new nonce.
func TestNonceViewRewriteKeepsTheCheckpointFlag(t *testing.T) {
	t.Parallel()
	hash := []byte{0xaa}
	v := nonceView{}
	v.apply(nonceBatchOps{nonces: []models.BlockNonce{
		{Hash: hash, Slot: 10, Nonce: []byte{1}, IsCheckpoint: true},
	}}, 0, 100)
	v.apply(nonceBatchOps{nonces: []models.BlockNonce{
		{Hash: hash, Slot: 10, Nonce: []byte{2}},
	}}, 0, 100)
	got := v.sorted()
	require.Len(t, got, 1)
	require.True(t, got[0].IsCheckpoint)
	require.Equal(t, []byte{2}, got[0].Nonce)
}

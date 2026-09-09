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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestGenesisTransactionMetadataErrorWithShortHashes(t *testing.T) {
	db := newTestDB(t)
	for _, length := range []int{0, 1, 7, 8, 32} {
		for _, shortTx := range []bool{false, true} {
			t.Run(fmt.Sprintf("length_%d/short_tx_%t", length, shortTx), func(t *testing.T) {
				txHash := bytes.Repeat([]byte{0xab}, 32)
				blockHash := bytes.Repeat([]byte{0xcd}, 32)
				if shortTx {
					txHash = txHash[:length:length]
				} else {
					blockHash = blockHash[:length:length]
				}
				txn := db.Transaction(true)
				defer txn.Rollback() //nolint:errcheck
				// Keep the blob handle live while the metadata handle fails. This
				// reaches the genesis metadata error wrapper without writing outputs.
				require.NoError(t, txn.Metadata().Rollback())
				var err error
				require.NotPanics(t, func() {
					err = db.SetGenesisTransaction(txHash, blockHash, nil, nil, txn)
				})
				require.ErrorIs(t, err, types.ErrNilTxn)
				require.ErrorContains(t, err, "SetGenesisTransaction failed for tx")
			})
		}
	}
}

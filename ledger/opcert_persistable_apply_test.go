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
	"context"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerProcessBlockRecordsCounterAboveMaxInt64 covers the apply path at
// the boundary that signed SQL storage previously rejected.
func TestLedgerProcessBlockRecordsCounterAboveMaxInt64(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	var issuerVkey lcommon.IssuerVkey
	for i := range issuerVkey {
		issuerVkey[i] = byte(i + 1)
	}
	pkh := lcommon.PoolKeyHash(issuerVkey.Hash())
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	block := &babbage.BabbageBlock{
		BlockHeader: &babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				Slot:       10,
				IssuerVkey: issuerVkey,
				OpCert: babbage.BabbageOpCert{
					SequenceNumber: uint64(math.MaxInt64) + 1,
				},
			},
		},
	}

	err := db.Transaction(context.Background(), true).
		Do(func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(
				context.Background(),
				txn,
				ocommon.Point{Slot: 10},
				block,
				false,
				false,
				false,
				nil,
				envelopeParent{},
				nil,
				eras.BabbageEraDesc,
				nil,
				nil,
				0,
				0,
				false,
			)
			return err
		})
	require.NoError(t, err)

	var poolID [28]byte
	copy(poolID[:], pkh.Bytes())
	sequence, found, err := ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(math.MaxInt64)+1, sequence)
}

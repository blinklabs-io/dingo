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
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

type recoveryReadCounter struct {
	metadata.MetadataStore
	fullReads     int
	stateReads    int
	boundaryReads int
}

func (s *recoveryReadCounter) GetUtxoIncludingSpent(
	id []byte,
	index uint32,
	txn types.Txn,
) (*models.Utxo, error) {
	s.fullReads++
	return s.MetadataStore.GetUtxoIncludingSpent(id, index, txn)
}

func (s *recoveryReadCounter) GetSyncState(
	key string,
	txn types.Txn,
) (string, error) {
	if key == mithrilLedgerSlotSyncKey {
		s.boundaryReads++
	}
	return s.MetadataStore.GetSyncState(key, txn)
}

type recoveryStateCounter struct{ *recoveryReadCounter }

func (s *recoveryStateCounter) GetUtxoSpendState(
	id []byte,
	index uint32,
	txn types.Txn,
) (*models.UtxoSpendState, error) {
	s.stateReads++
	return s.MetadataStore.(metadata.UtxoSpendStateReader).GetUtxoSpendState(
		id,
		index,
		txn,
	)
}
func TestConsumedInputRecoveryReadsOnlySpendState(t *testing.T) {
	t.Parallel()
	for _, narrow := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			name := "fallback/missing"
			if narrow {
				name = "narrow/missing"
			}
			if present {
				if narrow {
					name = "narrow/present"
				} else {
					name = "fallback/present"
				}
			}
			t.Run(name, func(t *testing.T) {
				db := openTestDB(t)
				db.config.StrictUtxoValidation = true
				hash := bytes.Repeat([]byte{0x71}, 32)
				input, err := mockledger.NewSimpleTransactionInput(hash, 0)
				require.NoError(t, err)
				if present {
					require.NoError(
						t,
						db.utxoStore().CreateUtxo(nil, &models.Utxo{
							TxId: hash, OutputIdx: 0, AddedSlot: 1, Amount: 1_000_000,
							PaymentKey: bytes.Repeat([]byte{0x72}, 28),
						}),
					)
				}
				counter := &recoveryReadCounter{MetadataStore: db.metadata}
				if narrow {
					db.metadata = &recoveryStateCounter{counter}
				} else {
					db.metadata = counter
				}
				tx := mockledger.NewTransactionBuilder()
				tx.WithId(bytes.Repeat([]byte{0x73}, 32))
				tx.WithInputs(input)
				tx.WithValid(true)
				txn := db.Transaction(t.Context(), true)
				defer txn.Release()
				err = txn.Do(func(txn *Txn) error {
					return db.ensureTransactionConsumedUtxos(
						tx,
						ocommon.Point{Slot: 10},
						txn,
						nil,
						BatchedTxIngestOpts{},
					)
				})
				if present {
					require.NoError(t, err)
					require.Zero(t, counter.boundaryReads)
				} else {
					require.ErrorContains(t, err, "could not be recovered")
					require.Equal(t, 1, counter.boundaryReads)
				}
				if narrow {
					require.Equal(t, 1, counter.stateReads)
					require.Zero(t, counter.fullReads)
				} else {
					require.Equal(t, 1, counter.fullReads)
					require.Zero(t, counter.stateReads)
				}
			})
		}
	}
}

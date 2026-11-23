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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/utxoref"
)

// The validation session's applied predicate reads the store: an applied
// transaction is reported, an identity-only producer row and an unknown hash
// are not.
func TestTxValidationSessionIsAppliedReadsStore(t *testing.T) {
	t.Parallel()

	db := newUtxoStorageTestDB(t)
	iter := newUtxoStorageTestIterator(t)
	block, blockCbor := nextProducingBlock(t, db, iter)

	var appliedHash []byte
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		tx := storeBlockFirstTx(t, db, txn, block, blockCbor)
		appliedHash = tx.Hash().Bytes()
		return nil
	}))

	var identityOnly [32]byte
	identityOnly[0] = 0x93
	seed := db.MetadataTxn(context.Background(), true)
	require.NoError(t, seed.Do(func(txn *database.Txn) error {
		return db.Metadata().ImportUtxos([]models.Utxo{{
			TxId:      identityOnly[:],
			OutputIdx: 0,
			Amount:    types.Uint64(1_000_000),
		}}, txn.Metadata())
	}))
	seed.Release()

	ls := mainnetForecastLedgerState(t, eras.AlonzoEraDesc)
	ls.db = db
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEpoch = ls.epochCache[297]
	ls.currentPParams = &alonzo.AlonzoProtocolParameters{ProtocolMajor: 6}
	ls.publishSnapshotsLocked()

	ran := false
	require.NoError(t, ls.WithResolvingTxValidationSession(
		context.Background(),
		func(
			_ func(
				lcommon.Transaction,
				map[utxoref.Key]struct{},
				map[utxoref.Key]lcommon.Utxo,
				*utxoref.StateOverlay,
			) error,
			_ func() bool,
			_ func(func() error) (bool, error),
			_ func([]models.UtxoId) ([]models.Utxo, error),
			isApplied func([]byte) (bool, error),
		) error {
			ran = true
			applied, err := isApplied(appliedHash)
			require.NoError(t, err)
			require.True(t, applied, "an applied transaction")
			applied, err = isApplied(identityOnly[:])
			require.NoError(t, err)
			require.False(t, applied, "an identity-only producer row")
			applied, err = isApplied(make([]byte, 32))
			require.NoError(t, err)
			require.False(t, applied, "an unknown hash")
			return nil
		},
	))
	require.True(t, ran)
}

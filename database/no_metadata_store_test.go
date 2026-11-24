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

package database_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The SQL reads and writes that remain on the database API refuse on the
// ledger's database, which opens no metadata store, instead of reaching for
// the missing store.
func TestSQLReadsAndWritesRefuseOnTheLedgersDatabase(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.Nil(t, db.Metadata())
	ctx := t.Context()
	hash := bytes.Repeat([]byte{0x5c}, 32)
	key := bytes.Repeat([]byte{0x5d}, 28)
	ldb := dbadapter.New(db)

	for name, call := range map[string]func() error{
		"DeleteRewardStateAfterSlot": func() error {
			return db.DeleteRewardStateAfterSlot(1, nil)
		},
		"GetDatum": func() error {
			_, err := db.GetDatum(ctx, hash, nil)
			return err
		},
		"SetDatum": func() error {
			return db.SetDatum([]byte{0x01}, 10, nil)
		},
		"GetScript": func() error {
			_, err := db.GetScript(ctx, hash, nil)
			return err
		},
		"GetTransactionByHash": func() error {
			_, err := db.GetTransactionByHash(ctx, hash, nil)
			return err
		},
		"GetTransactionsByBlockHash": func() error {
			_, err := db.GetTransactionsByBlockHash(ctx, hash, nil)
			return err
		},
		"GetTransactionMetadataByHash": func() error {
			_, err := db.GetTransactionMetadataByHash(ctx, hash, nil)
			return err
		},
		"GetAddressesByCredential": func() error {
			_, err := db.GetAddressesByCredential(
				ctx, 0, key, 10, 0, "asc", nil,
			)
			return err
		},
		"GetRewardAccountOutputsByCredential": func() error {
			_, err := db.GetRewardAccountOutputsByCredential(
				ctx, 0, key, 10, 0, "asc", nil,
			)
			return err
		},
		"GetAccountSumsByCredential": func() error {
			_, err := db.GetAccountSumsByCredential(ctx, 0, key, nil)
			return err
		},
		"CountMetadataLabels": func() error {
			_, err := db.CountMetadataLabels(ctx, nil)
			return err
		},
		"GetLatestMidnightAriadneParams": func() error {
			_, err := db.GetLatestMidnightAriadneParams()
			return err
		},
		"GetBlockNonce": func() error {
			_, err := db.GetBlockNonce(ocommon.NewPoint(10, hash), nil)
			return err
		},
		"SetBlockNonce": func() error {
			return db.SetBlockNonce(hash, 10, hash, false, nil)
		},
		"DeleteNetworkStateAfterSlot": func() error {
			return db.DeleteNetworkStateAfterSlot(ctx, 10, nil)
		},
		"LatestPoolOpCertSequence": func() error {
			_, _, err := db.LatestPoolOpCertSequence(
				ctx, lcommon.PoolKeyHash(key), nil,
			)
			return err
		},
		"RemoveGenesisUtxos": func() error {
			_, _, err := db.RemoveGenesisUtxos(
				ctx, []models.UtxoId{{Hash: hash}}, [][]byte{hash}, nil,
			)
			return err
		},
		"CreateUtxo": func() error {
			return db.CreateUtxo(ctx, nil, &models.Utxo{TxId: hash})
		},
		"UtxoByRef": func() error {
			_, err := db.UtxoByRef(ctx, hash, 0, nil)
			return err
		},
		"LedgerDB.GetPoolStatus": func() error {
			_, err := ldb.GetPoolStatus(lcommon.PoolKeyHash(key), nil)
			return err
		},
		"LedgerDB.GetNetworkState": func() error {
			_, err := ldb.GetNetworkState(nil)
			return err
		},
		"LedgerDB.SumTransactionFeesInSlotRange": func() error {
			_, err := ldb.SumTransactionFeesInSlotRange(0, 10, nil)
			return err
		},
		"LedgerDB.GetLiveUtxoLovelace": func() error {
			_, err := ldb.GetLiveUtxoLovelace(nil)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, call(), database.ErrNoMetadataStore)
		})
	}
}

// A transaction on the ledger's database has no SQL connection to refuse a
// cancelled context, so Do itself must.
func TestLedgerDatabaseTransactionRefusesCancelledContext(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.Nil(t, db.Metadata())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for name, open := range map[string]func() *database.Txn{
		"Transaction read":  func() *database.Txn { return db.Transaction(ctx, false) },
		"Transaction write": func() *database.Txn { return db.Transaction(ctx, true) },
		"NewTxn":            func() *database.Txn { return database.NewTxn(ctx, db, false) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ran := false
			err := open().Do(func(*database.Txn) error {
				ran = true
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, ran)
		})
	}

	t.Run("live context runs", func(t *testing.T) {
		t.Parallel()
		ran := false
		require.NoError(t, db.Transaction(t.Context(), false).Do(
			func(*database.Txn) error {
				ran = true
				return nil
			},
		))
		require.True(t, ran)
	})

	t.Run("BlockBySlot", func(t *testing.T) {
		t.Parallel()
		_, err := database.BlockBySlot(ctx, db, 1)
		require.ErrorIs(t, err, context.Canceled)
	})
}

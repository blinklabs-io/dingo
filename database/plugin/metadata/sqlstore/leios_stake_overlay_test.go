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

package sqlstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func retainedStakeChain(
	t testing.TB,
	n int,
) (lcommon.Transaction, []lcommon.Transaction) {
	t.Helper()
	output := func(i int) lcommon.TransactionOutput {
		kind := uint8(lcommon.AddressTypeKeyKey)
		if i%3 == 1 {
			kind = lcommon.AddressTypeKeyScript
		}
		addr, err := lcommon.NewAddressFromParts(
			kind,
			lcommon.AddressNetworkTestnet,
			bytes.Repeat([]byte{0x31}, 28),
			bytes.Repeat([]byte{byte(0x41 + i%3)}, 28),
		)
		require.NoError(t, err)
		out, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(addr.String()).
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		return out
	}
	seed := mockledger.NewTransactionBuilder()
	seed.WithId(overlayTxID(0x61, 0))
	seed.WithOutputs(output(0))
	seed.WithValid(true)
	prior := seed.Hash().Bytes()
	txs := make([]lcommon.Transaction, 0, n)
	for i := range n {
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(overlayTxID(0x62, i))
		tx.WithInputs(overlayTxInput(t, prior, 0))
		tx.WithOutputs(output(i + 1))
		tx.WithValid(true)
		txs = append(txs, tx)
		prior = tx.Hash().Bytes()
	}
	return seed, txs
}

func retainedStakeState(t testing.TB, db queryer) string {
	t.Helper()
	var result []any
	for _, q := range []string{`SELECT * FROM reward_live_stake ORDER BY credential_tag,staking_key`, `SELECT name,seq FROM sqlite_sequence WHERE name='reward_live_stake'`} {
		rows, err := db.QueryContext(context.Background(), q)
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			result = append(result, values)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	return string(encoded)
}

func retainedStakeStore(
	t testing.TB,
	seed lcommon.Transaction,
	emptyBaseline bool,
) *Store {
	t.Helper()
	s := newForeignKeySQLiteStore(t)
	s.storageMode = types.StorageModeAPI
	require.NoError(
		t,
		s.SetTransaction(
			seed,
			ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
			0,
			nil,
			true,
			nil,
			0,
		),
	)
	require.NoError(
		t,
		s.CreateAccount(
			nil,
			&models.Account{
				StakingKey:    bytes.Repeat([]byte{0x41}, 28),
				CredentialTag: 0,
				AddedSlot:     1,
				CreatedSlot:   999,
				Active:        true,
				Reward:        5,
			},
		),
	)
	if emptyBaseline {
		_, err := s.writeDB.Exec(`UPDATE reward_live_stake SET utxo_stake=''`)
		require.NoError(t, err)
	}
	return s
}

func writeRetainedStakeChain(
	t testing.TB,
	s *Store,
	txs []lcommon.Transaction,
	enabled, barriers, commit bool,
) []string {
	t.Helper()
	txn := s.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := s.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	if enabled {
		n, err := s.PrepareLeiosClosureBatch(acc, txn, txs)
		require.NoError(t, err)
		require.Len(t, txs, n)
		require.NotNil(t, acc.(*transactionBatchAccumulator).overlay)
	}
	var observations []string
	contextSlot := uint64(99)
	for index, tx := range txs {
		require.NoError(
			t,
			s.SetTransactionLeiosClosureBatched(
				tx,
				ocommon.Point{Slot: 100, Hash: overlayTxID(0x63, 0)},
				uint32(index),
				nil,
				true,
				&contextSlot,
				acc,
				txn,
				0,
			),
		)
		if enabled {
			model, err := s.GetUtxo(tx.Hash().Bytes(), 0, txn)
			require.NoError(t, err)
			require.NotNil(t, model)
			original := bytes.Clone(model.StakingKey)
			model.StakingKey[0] ^= 0xff
			again, err := s.GetUtxo(tx.Hash().Bytes(), 0, txn)
			require.NoError(t, err)
			require.Equal(t, original, again.StakingKey)
			require.Empty(t, acc.(*transactionBatchAccumulator).stakeDeltaOrder)
		}
		if barriers {
			db, _, err := s.readDBFromTxn(txn)
			require.NoError(t, err)
			observations = append(observations, retainedStakeState(t, db))
		}
	}
	if enabled && !barriers {
		require.Zero(
			t,
			acc.(*transactionBatchAccumulator).overlay.stats.BarrierFlushes,
		)
	}
	require.NoError(t, s.FlushBatch(acc, txn))
	db, _, err := s.readDBFromTxn(txn)
	require.NoError(t, err)
	observations = append(observations, retainedStakeState(t, db))
	if commit {
		require.NoError(t, txn.Commit())
	} else {
		require.NoError(t, txn.Rollback())
	}
	return observations
}

func TestLeiosRetainedStakeMatchesPerTransactionState(t *testing.T) {
	t.Parallel()
	for _, emptyBaseline := range []bool{false, true} {
		for _, barriers := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("empty=%t/barriers=%t", emptyBaseline, barriers),
				func(t *testing.T) {
					seed, txs := retainedStakeChain(t, 12)
					var states [2]string
					var views [2][]string
					for i, enabled := range []bool{false, true} {
						s := retainedStakeStore(t, seed, emptyBaseline)
						views[i] = writeRetainedStakeChain(
							t,
							s,
							txs,
							enabled,
							barriers,
							true,
						)
						states[i] = retainedStakeHistoryState(
							t,
							s,
						) + retainedStakeState(
							t,
							s.writeDB,
						)
					}
					require.Equal(t, views[0], views[1])
					require.Equal(t, states[0], states[1])
				},
			)
		}
	}
}

func TestLeiosRetainedStakeRollback(t *testing.T) {
	t.Parallel()
	seed, txs := retainedStakeChain(t, 12)
	s := retainedStakeStore(t, seed, false)
	before := retainedStakeHistoryState(t, s) + retainedStakeState(t, s.writeDB)
	writeRetainedStakeChain(t, s, txs, true, true, false)
	require.Equal(
		t,
		before,
		retainedStakeHistoryState(t, s)+retainedStakeState(t, s.writeDB),
	)
}

func BenchmarkLeiosRetainedStake(b *testing.B) {
	seed, txs := retainedStakeChain(b, 1000)
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprint(enabled), func(b *testing.B) {
			s := retainedStakeStore(b, seed, false)
			b.ResetTimer()
			for range b.N {
				txn := s.Transaction(context.Background())
				acc := s.NewLeiosClosureBatchAccumulator()
				if enabled {
					ok, err := s.EnableLeiosClosureUtxoOverlay(acc, txn, txs)
					require.NoError(b, err)
					require.True(b, ok)
				}
				contextSlot := uint64(99)
				for index, tx := range txs {
					require.NoError(
						b,
						s.SetTransactionLeiosClosureBatched(
							tx,
							ocommon.Point{
								Slot: 100,
								Hash: overlayTxID(0x63, 0),
							},
							uint32(index),
							nil,
							true,
							&contextSlot,
							acc,
							txn,
							0,
						),
					)
				}
				require.NoError(b, s.FlushBatch(acc, txn))
				require.NoError(b, txn.Rollback())
				acc.Reset()
			}
		})
	}
}

func retainedStakeHistoryState(t testing.TB, s *Store) string {
	t.Helper()
	state := strings.Split(retainedOverlayAPIState(t, s), "\n")
	rows, err := s.writeDB.Query(`SELECT * FROM address_transaction`)
	require.NoError(t, err)
	columns, err := rows.Columns()
	require.NoError(t, err)
	var addresses []string
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		var semantic []any
		for i, column := range columns {
			if column != "id" {
				semantic = append(semantic, values[i])
			}
		}
		data, err := json.Marshal(semantic)
		require.NoError(t, err)
		addresses = append(addresses, string(data))
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	sort.Strings(addresses)
	// Address index IDs follow map iteration; public ordering uses transaction positions.
	for i, line := range state {
		if strings.HasPrefix(line, "address_transaction: ") {
			state[i] = fmt.Sprint("address_transaction: ", addresses)
		}
	}
	return strings.Join(state, "\n")
}

func TestLeiosRetainedStakeRejectsAccountMutation(t *testing.T) {
	t.Parallel()
	seed, txs := retainedStakeChain(t, 2)
	s := retainedStakeStore(t, seed, false)
	txn := s.Transaction(t.Context())
	defer func() { _ = txn.Rollback() }()
	acc := s.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := s.EnableLeiosClosureUtxoOverlay(acc, txn, txs)
	require.NoError(t, err)
	require.True(t, enabled)
	db, ctx, err := s.readDBFromTxn(txn)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE account SET reward='99'`)
	require.ErrorContains(
		t,
		err,
		"external mutation inside API closure UTxO overlay",
	)
	require.ErrorContains(t, err, "statement UPDATE is forbidden")
	require.ErrorContains(
		t,
		acc.(*transactionBatchAccumulator).BatchUtxoOverlayError(),
		"external mutation",
	)
}

func TestLeiosRetainedOverlayPreservesStorageError(t *testing.T) {
	t.Parallel()
	seed, txs := retainedStakeChain(t, 2)
	s := retainedStakeStore(t, seed, false)
	_, err := s.writeDB.Exec(
		`CREATE TRIGGER reject_overlay_utxo BEFORE INSERT ON utxo
BEGIN SELECT RAISE(ABORT, 'reject retained output'); END`,
	)
	require.NoError(t, err)
	txn := s.Transaction(t.Context())
	defer func() { _ = txn.Rollback() }()
	acc := s.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	n, err := s.PrepareLeiosClosureBatch(acc, txn, txs)
	require.NoError(t, err)
	require.Len(t, txs, n)
	require.NoError(t, s.SetTransactionLeiosClosureBatched(
		txs[0], ocommon.Point{Slot: 100, Hash: overlayTxID(0x63, 0)},
		0, nil, true, nil, acc, txn, 0,
	))
	db, ctx, err := s.readDBFromTxn(txn)
	require.NoError(t, err)
	rows, first := db.QueryContext(ctx, `SELECT id FROM utxo`)
	if rows != nil {
		require.NoError(t, rows.Close())
	}
	require.ErrorContains(t, first, "reject retained output")
	_, err = db.ExecContext(ctx, `UPDATE account SET reward='99'`)
	require.ErrorIs(
		t,
		err,
		first,
		"the original storage error must remain sticky",
	)
	require.ErrorIs(
		t,
		acc.(*transactionBatchAccumulator).BatchUtxoOverlayError(),
		first,
	)
	require.ErrorIs(t, s.FlushBatch(acc, txn), first)
}

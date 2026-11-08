//go:build dingo_db_integration

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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresEpochFeeSum(t *testing.T) {
	t.Parallel()
	dsn, schema := newPostgresIntegrationSchema(t)
	testEpochFeeSumBackend(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLEpochFeeSum(t *testing.T) {
	t.Parallel()
	dsn, name := newMySQLIntegrationDatabase(t)
	testEpochFeeSumBackend(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", name),
	)
}

func testEpochFeeSumBackend(t *testing.T, store *Store) {
	t.Helper()
	for i, row := range []struct {
		slot        uint64
		valid       bool
		fee         string
		collateral  string
		contextSlot uint64
	}{
		{10, true, "5", "bad", 0},
		{11, false, "bad", "7", 0},
		{90, true, "13", "bad", 11},
		{11, true, "17", "bad", 90},
		{80, false, "bad", "29", 12},
		{1000, true, "18446744073709551615", "0", 0},
	} {
		_, err := store.writeDB.ExecContext(
			t.Context(),
			store.dialect.Rebind(
				"INSERT INTO "+store.dialect.QuoteIdentifier("transaction")+
					" (id,hash,slot,valid,fee,collateral_fee) VALUES (?,?,?,?,?,?)"),
			i+1,
			bytes.Repeat([]byte{byte(i + 1)}, 32),
			row.slot,
			row.valid,
			row.fee,
			row.collateral,
		)
		require.NoError(t, err)
		if row.contextSlot != 0 {
			_, err = store.writeDB.ExecContext(
				t.Context(),
				store.dialect.Rebind(
					"INSERT INTO leios_transaction_context (transaction_id,slot) VALUES (?,?)",
				),
				i+1,
				row.contextSlot,
			)
			require.NoError(t, err)
		}
	}
	got, err := store.SumTransactionFeesInSlotRange(10, 12, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(54), got)
	got, err = store.SumTransactionFeesInSlotRange(1000, 1000, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), got)
	_, err = store.writeDB.ExecContext(t.Context(), store.dialect.Rebind(
		"INSERT INTO "+store.dialect.QuoteIdentifier("transaction")+
			" (id,hash,slot,valid,fee,collateral_fee) VALUES (7,?,1000,TRUE,'1','0')"),
		bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	_, err = store.SumTransactionFeesInSlotRange(1000, 1000, nil)
	require.Error(t, err)
}

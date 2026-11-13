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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestPostgresBlockTransactionTotals(t *testing.T) {
	t.Parallel()
	dsn, schema := newPostgresIntegrationSchema(t)
	testBlockTotalsBackend(t, newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema))
}

func TestMySQLBlockTransactionTotals(t *testing.T) {
	t.Parallel()
	dsn, name := newMySQLIntegrationDatabase(t)
	testBlockTotalsBackend(t, newIntegrationSQLStore(t, "mysql", dsn, "mysql", name))
}

func testBlockTotalsBackend(t *testing.T, s *Store) {
	t.Helper()
	hash := bytes.Repeat([]byte{0xa3}, 32)
	maxAmount := new(big.Int).SetUint64(math.MaxUint64).String()
	for i, valid := range []bool{true, true, false, false, true} {
		h := bytes.Repeat([]byte{byte(i + 1)}, 32)
		block := hash
		if i == 4 {
			block = bytes.Repeat([]byte{0xa4}, 32)
		}
		amount, fee := maxAmount, maxAmount
		if i >= 2 {
			amount, fee = "11", "7"
		}
		table := s.dialect.QuoteIdentifier("transaction")
		_, err := s.writeDB.ExecContext(t.Context(), s.dialect.Rebind(
			"INSERT INTO "+table+" (id,hash) VALUES (?,?)"), i+1, h)
		require.NoError(t, err)
		_, err = s.writeDB.ExecContext(t.Context(), s.dialect.Rebind(`
INSERT INTO applied_transaction (tx_id,block_hash,slot,fee,block_index,valid)
VALUES (?,?,?,?,?,?)`), i+1, block, 1, fee, i, valid)
		require.NoError(t, err)
		_, err = s.writeDB.ExecContext(t.Context(), s.dialect.Rebind(`
INSERT INTO utxo (id,tx_id,output_idx,amount,deleted_slot)
VALUES (?,?,?,?,?)`), i+101, i+1, 0, amount, 100)
		require.NoError(t, err)
	}
	_, err := s.writeDB.ExecContext(t.Context(), s.dialect.Rebind(`
INSERT INTO utxo (id,collateral_return_for_tx_id,tx_id,output_idx,amount)
VALUES (?,?,?,?,?)`), 201, 3, 3, 1, "19")
	require.NoError(t, err)
	// A valid transaction ignores an otherwise retained collateral return.
	_, err = s.writeDB.ExecContext(t.Context(), s.dialect.Rebind(`
INSERT INTO utxo (id,collateral_return_for_tx_id,tx_id,output_idx,amount)
VALUES (?,?,?,?,?)`), 202, 1, 1, 1, "99")
	require.NoError(t, err)
	twiceMax := new(big.Int).Mul(new(big.Int).SetUint64(math.MaxUint64), big.NewInt(2))
	want := models.BlockTransactionTotals{
		Output: new(big.Int).Add(new(big.Int).Set(twiceMax), big.NewInt(30)).String(),
		Fees:   new(big.Int).Add(new(big.Int).Set(twiceMax), big.NewInt(14)).String(),
	}
	got, err := s.GetBlockTransactionTotals(hash, nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	got, err = s.GetBlockTransactionTotals(bytes.Repeat([]byte{0xff}, 32), nil)
	require.NoError(t, err)
	require.Equal(t, models.BlockTransactionTotals{Output: "0", Fees: "0"}, got)
}

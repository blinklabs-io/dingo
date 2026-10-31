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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/bigintsum"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// TestGetControlledAmountByCredentialExcludesSpent pins the live-only total
// the database-side aggregate returns.
func TestGetControlledAmountByCredentialExcludesSpent(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(3))
	seedCredentialUtxos(
		t, store, 0, ref,
		[]uint64{1_000_000, 2_000_000, 3_000_000, 9_000_000},
		[]bool{false, false, false, true},
	)

	got, err := store.GetControlledAmountByCredential(
		ref.Tag, ref.Key, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6_000_000), got, "spent UTxOs are excluded")
}

// TestGetAccountSumsByCredentialWithdrawalsAggregate proves the withdrawal
// total sums only withdrawal rows of the requested credential.
func TestGetAccountSumsByCredentialWithdrawalsAggregate(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)
	credential := mirTestCredential(0x31).Credential[:]
	other := mirTestCredential(0x32).Credential[:]
	for i, row := range []struct {
		key        []byte
		amount     string
		withdrawal bool
	}{
		{credential, "1500000", true},
		{credential, "2500000", true},
		{credential, "7000000", false},
		{other, "9000000", true},
	} {
		_, err := store.writeDB.Exec(`
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal
) VALUES (?, 0, ?, ?, 10, ?)`,
			row.key, []byte{byte(i + 1)}, row.amount, row.withdrawal,
		)
		require.NoError(t, err)
	}
	sums, err := store.GetAccountSumsByCredential(0, credential, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(4_000_000), sums.WithdrawalsSum)
}

var rowCountStoreSequence atomic.Uint64

// rowCounter counts the rows the driver hands to database/sql while armed.
type rowCounter struct {
	armed atomic.Bool
	rows  atomic.Int64
}

type rowCountConnector struct {
	dsn     string
	counter *rowCounter
}

func (c rowCountConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := registeredSQLiteDriver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &rowCountConn{Conn: conn, counter: c.counter}, nil
}

func (c rowCountConnector) Driver() driver.Driver {
	return registeredSQLiteDriver()
}

// registeredSQLiteDriver returns the driver instance database/sql holds for
// "sqlite": user-defined functions are registered on that instance only.
func registeredSQLiteDriver() driver.Driver {
	db, err := sql.Open("sqlite", "file::memory:?_pragma=synchronous(OFF)")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	return db.Driver()
}

type rowCountConn struct {
	driver.Conn
	counter *rowCounter
}

func (c *rowCountConn) PrepareContext(
	ctx context.Context,
	query string,
) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *rowCountConn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions,
) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *rowCountConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *rowCountConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &rowCountRows{Rows: rows, counter: c.counter}, nil
}

type rowCountRows struct {
	driver.Rows
	counter *rowCounter
}

func (r *rowCountRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err == nil && r.counter.armed.Load() {
		r.counter.rows.Add(1)
	}
	if err != nil && err != io.EOF {
		return err
	}
	return err
}

func newRowCountingStore(t *testing.T) (*Store, *rowCounter) {
	t.Helper()
	counter := &rowCounter{}
	db := sql.OpenDB(rowCountConnector{
		dsn: fmt.Sprintf(
			"file:sqlstore_rowcount_%d?mode=memory&cache=shared",
			rowCountStoreSequence.Add(1),
		),
		counter: counter,
	})
	db.SetMaxOpenConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, counter
}

// TestAccountAggregatesDoNotStreamRows proves the controlled amount,
// withdrawal and MIR totals are summed by the database: a stake credential
// with many rows crosses to the caller as one row per aggregate, so the work
// the caller does no longer grows with the credential's history.
func TestAccountAggregatesDoNotStreamRows(t *testing.T) {
	t.Parallel()
	store, counter := newRowCountingStore(t)
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(5))
	const rows = 40
	amounts := make([]uint64, rows)
	for i := range amounts {
		amounts[i] = 1_000_000
	}
	seedCredentialUtxos(t, store, 0, ref, amounts, nil)
	for i := range rows {
		_, err := store.writeDB.Exec(`
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal
) VALUES (?, 0, ?, '1000', 10, TRUE)`, ref.Key, []byte{byte(i + 1)})
		require.NoError(t, err)
		seedMIRRewardRow(t, store, uint(i%2), ref.Key, "-3")
	}

	counter.armed.Store(true)
	controlled, err := store.GetControlledAmountByCredential(
		ref.Tag, ref.Key, nil,
	)
	require.NoError(t, err)
	sums, err := store.GetAccountSumsByCredential(ref.Tag, ref.Key, nil)
	require.NoError(t, err)
	counter.armed.Store(false)

	require.Equal(t, uint64(rows*1_000_000), controlled)
	require.Equal(t, uint64(rows*1_000), sums.WithdrawalsSum)
	require.Equal(t, "-60", sums.ReservesSum.String())
	require.Equal(t, "-60", sums.TreasurySum.String())
	// One row each: controlled amount, withdrawals, reserves, treasury.
	require.EqualValues(
		t, 4, counter.rows.Load(),
		"aggregates must not stream per-row results to the caller",
	)
}

// TestGetAccountSumsByCredentialMIRBeyondInt64 proves the MIR totals stay
// exact past 64 bits and through a negative intermediate total.
func TestGetAccountSumsByCredentialMIRBeyondInt64(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)
	credential := mirTestCredential(0x41).Credential[:]
	for _, amount := range []string{
		"-30000000000000000000000",
		"9223372036854775807",
		"9223372036854775807",
		"50000000000000000000000",
		"-1",
	} {
		seedMIRRewardRow(t, store, 0, credential, amount)
	}
	sums, err := store.GetAccountSumsByCredential(0, credential, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		"20018446744073709551613",
		sums.ReservesSum.String(),
	)
	require.Equal(t, "0", sums.TreasurySum.String())
}

// testAccountAggregateContract pins the account total contract every dialect
// must meet: exact totals past int64, a controlled amount at the uint64
// ceiling, signed MIR totals, zero for a credential with no rows, and an
// error rather than a wrapped value when a coin total leaves uint64.
func testAccountAggregateContract(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := store.writeDB.ExecContext(
			ctx,
			store.dialect.Rebind(query),
			args...,
		)
		require.NoError(t, err)
	}
	keyA := bytes.Repeat([]byte{0x5a}, 28)
	keyB := bytes.Repeat([]byte{0x5b}, 28)
	keyEmpty := bytes.Repeat([]byte{0x5c}, 28)
	for i, row := range []struct {
		key     []byte
		amount  string
		deleted int64
	}{
		{keyA, "18446744073709551614", 0},
		{keyA, "1", 0},
		{keyA, "5", 100},
		{keyB, "18446744073709551615", 0},
		{keyB, "1", 0},
	} {
		exec(`
INSERT INTO utxo (
    tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot,
    amount
) VALUES (?, 0, ?, 0, 1, ?, ?)`,
			bytes.Repeat([]byte{byte(i + 1)}, 32),
			row.key,
			row.deleted,
			row.amount,
		)
	}
	for i, row := range []struct {
		amount     string
		withdrawal bool
	}{
		{"9223372036854775807", true},
		{"9223372036854775807", true},
		{"5", false},
	} {
		exec(`
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal
) VALUES (?, 0, ?, ?, 10, ?)`,
			keyA,
			bytes.Repeat([]byte{byte(i + 1)}, 32),
			row.amount,
			row.withdrawal,
		)
	}
	exec(`
INSERT INTO move_instantaneous_rewards (
    id, pot, certificate_id, added_slot, other_pot
) VALUES (1, 0, 0, 100, '0'), (2, 1, 0, 100, '0')`)
	for _, row := range []struct {
		mirID  int64
		amount string
	}{
		{1, "-30000000000000000000000"},
		{1, "50000000000000000000000"},
		{1, "-1"},
		{2, "-5"},
	} {
		exec(`
INSERT INTO move_instantaneous_rewards_reward (
    credential, credential_tag, amount, mir_id
) VALUES (?, 0, ?, ?)`,
			keyA,
			row.amount,
			row.mirID,
		)
	}

	controlled, err := store.GetControlledAmountByCredential(0, keyA, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), controlled)
	sums, err := store.GetAccountSumsByCredential(0, keyA, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551614), sums.WithdrawalsSum)
	require.Equal(t, "19999999999999999999999", sums.ReservesSum.String())
	require.Equal(t, "-5", sums.TreasurySum.String())

	_, err = store.GetControlledAmountByCredential(0, keyB, nil)
	require.ErrorContains(t, err, "outside uint64")

	controlled, err = store.GetControlledAmountByCredential(0, keyEmpty, nil)
	require.NoError(t, err)
	require.Zero(t, controlled)
	sums, err = store.GetAccountSumsByCredential(0, keyEmpty, nil)
	require.NoError(t, err)
	require.Zero(t, sums.WithdrawalsSum)
	require.Equal(t, "0", sums.ReservesSum.String())
	require.Equal(t, "0", sums.TreasurySum.String())
}

func TestSQLiteAccountAggregateContract(t *testing.T) {
	t.Parallel()
	testAccountAggregateContract(t, newMigratedSQLiteStore(t))
}

// TestGetLiveUtxoLovelaceExcludesSpent pins the live-only total across every
// credential.
func TestGetLiveUtxoLovelaceExcludesSpent(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	refA := models.NewStakeCredentialRef(0, credentialKeyForIndex(4))
	refB := models.NewStakeCredentialRef(0, credentialKeyForIndex(5))
	seedCredentialUtxos(
		t, store, 0, refA,
		[]uint64{1_000_000, 2_000_000, 9_000_000},
		[]bool{false, false, true},
	)
	seedCredentialUtxos(
		t, store, 10, refB,
		[]uint64{4_000_000, 8_000_000},
		[]bool{false, true},
	)

	got, err := store.GetLiveUtxoLovelace(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(7_000_000), got, "spent UTxOs are excluded")
}

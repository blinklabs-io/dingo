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
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

// statementRecorder collects every SQL text the connections it wraps prepare,
// query or execute.
type statementRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (r *statementRecorder) record(query string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, query)
}

func (r *statementRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.statements...)
}

type recordingConnector struct {
	dsn      string
	recorder *statementRecorder
}

func (c recordingConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := (&sqlite.Driver{}).Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &recordingConn{Conn: conn, recorder: c.recorder}, nil
}

func (c recordingConnector) Driver() driver.Driver { return &sqlite.Driver{} }

// recordingConn forwards to the modernc connection, which implements the
// context-aware driver interfaces the wrapper delegates to.
type recordingConn struct {
	driver.Conn
	recorder *statementRecorder
}

func (c *recordingConn) PrepareContext(
	ctx context.Context,
	query string,
) (driver.Stmt, error) {
	c.recorder.record(query)
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *recordingConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	c.recorder.record(query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *recordingConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	c.recorder.record(query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *recordingConn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions,
) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func newRecordingSQLiteStore(t *testing.T) (*Store, *statementRecorder) {
	t.Helper()
	recorder := &statementRecorder{}
	dsn := fmt.Sprintf(
		"file:sqlstore_%d?mode=memory&cache=shared",
		testStoreSequence.Add(1),
	)
	seedMigratedSQLiteDSN(t, dsn)
	db := sql.OpenDB(recordingConnector{
		dsn:      dsn,
		recorder: recorder,
	})
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, recorder
}

// TestClaimUnfoldedRewardCreditsDoesNotScanEveryUnfoldedRow drives the claim
// and checks the plan of every statement it issued. A plan that reaches
// reward_account_output through only the (spendable, guarded) prefix of the
// pending-round index visits every spendable, unguarded row, folded or not.
func TestClaimUnfoldedRewardCreditsDoesNotScanEveryUnfoldedRow(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	require.NoError(t, store.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{
			{SnapshotEpoch: 1, BoundarySlot: 100},
			{SnapshotEpoch: 2, BoundarySlot: 200},
		}, nil,
	))
	outputs := make([]*models.RewardAccountOutput, 0, 7)
	for i := range 5 {
		outputs = append(outputs, &models.RewardAccountOutput{
			Epoch:         1,
			StakingKey:    bytesRepeat(byte(0x10+i), 28),
			PoolKeyHash:   bytesRepeat(0x31, 28),
			RewardType:    "member",
			Amount:        types.Uint64(uint64(i + 1)),
			Spendable:     true,
			BoundarySlot:  100,
			CredentialTag: 0,
		})
	}
	for i := range 2 {
		outputs = append(outputs, &models.RewardAccountOutput{
			Epoch:        2,
			StakingKey:   bytesRepeat(byte(0x40+i), 28),
			PoolKeyHash:  bytesRepeat(0x31, 28),
			RewardType:   "member",
			Amount:       types.Uint64(9),
			Spendable:    true,
			BoundarySlot: 200,
		})
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	for _, output := range outputs {
		require.NoError(t, store.CreateAccount(nil, &models.Account{
			StakingKey: output.StakingKey, CredentialTag: output.CredentialTag,
			Active: true,
		}))
	}
	var firstID int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT MIN(id) FROM reward_account_output WHERE epoch = 1`,
	).Scan(&firstID))
	_, err := store.writeDB.Exec(
		`UPDATE reward_account_output SET folded = TRUE WHERE id = ?`, firstID,
	)
	require.NoError(t, err)

	mark := len(recorder.snapshot())
	first, err := store.ClaimUnfoldedRewardCredits(1, 0, 3, nil)
	require.NoError(t, err)
	second, err := store.ClaimUnfoldedRewardCredits(1, 0, 3, nil)
	require.NoError(t, err)
	third, err := store.ClaimUnfoldedRewardCredits(1, 0, 3, nil)
	require.NoError(t, err)
	claimSQL := recorder.snapshot()[mark:]
	require.Len(t, first, 3)
	require.Len(t, second, 1, "four unfolded rows remain after the folded one")
	require.Empty(t, third)
	for i, output := range append(first, second...) {
		require.Equal(t, uint64(1), output.Epoch)
		require.Equal(t, firstID+int64(i)+1, int64(output.ID), "row order")
	}
	var otherEpochFolded int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM reward_account_output
WHERE epoch = 2 AND folded`,
	).Scan(&otherEpochFolded))
	require.Zero(t, otherEpochFolded)

	// The account-ordered selection must drive from the account rowid range
	// and probe the outputs by (epoch, credential): a probe that lacks the
	// credential reads every unfolded output of the round once per account.
	accountSeek := regexp.MustCompile(`SEARCH a USING INTEGER PRIMARY KEY \(rowid>\?\)`)
	outputSeek := regexp.MustCompile(
		`SEARCH o USING INDEX idx_reward_account_output_epoch_cred_pool_type ` +
			`\(epoch=\? AND credential_tag=\? AND staking_key=\?\)`,
	)
	tableScan := regexp.MustCompile(`(?m)^SCAN (rao|o)\b`)
	var ordered, unaccounted int
	for _, statement := range claimSQL {
		if !strings.Contains(statement, "reward_account_output") ||
			strings.HasPrefix(strings.TrimSpace(statement), "UPDATE") {
			continue
		}
		args := make([]any, strings.Count(statement, "?"))
		plan := queryPlan(t, store.writeDB, statement, args...)
		require.NotRegexp(t, tableScan, plan, "statement: %s", statement)
		// Ordering by output id within an account sorts only the last term
		// of the ORDER BY, one account's rows at a time; a sort of the whole
		// round would read "FOR ORDER BY".
		require.NotContains(t, plan, "USE TEMP B-TREE FOR ORDER BY", "statement: %s", statement)
		switch {
		case strings.Contains(statement, "GetUnfoldedRewardOutputsInAccountOrder"):
			ordered++
			require.Regexp(t, accountSeek, plan, "statement: %s", statement)
			require.Regexp(t, outputSeek, plan, "statement: %s", statement)
		case strings.Contains(statement, "GetUnaccountedRewardOutputs"):
			unaccounted++
		default:
			require.Failf(t, "unexpected claim statement", "%s", statement)
		}
	}
	require.Equal(t, 3, ordered, "one account-ordered selection per claim")
	require.Equal(t, 2, unaccounted, "a short claim also looks for outputs without an account")
}

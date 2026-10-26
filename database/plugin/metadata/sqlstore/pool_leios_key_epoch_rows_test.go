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
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/leioskeyepoch"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

// rowProbeName is a SQL function that counts the rows a statement visits. It
// is registered once for the process; each test passes its own token as the
// argument, so tests that run in parallel count into separate counters.
const rowProbeName = "dingo_leios_key_row_probe"

var (
	rowProbeOnce     sync.Once
	rowProbeCounters sync.Map // int64 token -> *atomic.Int64
	rowProbeTokens   atomic.Int64
)

func registerRowProbe() {
	rowProbeOnce.Do(func() {
		sqlite.MustRegisterScalarFunction(
			rowProbeName, 1,
			func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
				token, ok := args[0].(int64)
				if !ok {
					return nil, fmt.Errorf("row probe token %T", args[0])
				}
				if counter, ok := rowProbeCounters.Load(token); ok {
					counter.(*atomic.Int64).Add(1)
				}
				return int64(1), nil
			},
		)
	})
}

// leiosKeyStatement is the "-- name:" label a leioskeyepoch statement carries.
type leiosKeyStatement string

const (
	stmtEpochAtSlot         leiosKeyStatement = "LeiosKeyEpochAtSlot"
	stmtLatestRegistration  leiosKeyStatement = "LeiosKeyLatestRegistration"
	stmtLatestRegExecBefore leiosKeyStatement = "LeiosKeyLatestRegistrationExecutedBefore"
	stmtFirstRegistration   leiosKeyStatement = "LeiosKeyFirstRegistrationFrom"
	stmtSnapshotAtSlot      leiosKeyStatement = "LeiosKeySnapshotCurrent"
	stmtLatestSnapshot      leiosKeyStatement = "LeiosKeyLatestSnapshotCurrent"
	stmtLatestRetirement    leiosKeyStatement = "LeiosKeyLatestRetirement"
	stmtLatestRetireBefore  leiosKeyStatement = "LeiosKeyLatestRetirementExecutedBefore"
)

var statementLabel = regexp.MustCompile(`(?m)^-- name: (\w+)$`)

// classifyLeiosKeyStatement returns the statement's label. A statement without
// one, or with a label this test does not know, fails the test, so a statement
// added to leioskeyepoch cannot go unmeasured.
func classifyLeiosKeyStatement(t *testing.T, query string) leiosKeyStatement {
	t.Helper()
	match := statementLabel.FindStringSubmatch(query)
	require.NotNil(t, match, "unlabeled leioskeyepoch statement:\n%s", query)
	statement := leiosKeyStatement(match[1])
	switch statement {
	case stmtEpochAtSlot, stmtLatestRegistration, stmtLatestRegExecBefore,
		stmtFirstRegistration, stmtSnapshotAtSlot, stmtLatestSnapshot,
		stmtLatestRetirement, stmtLatestRetireBefore:
		return statement
	}
	t.Fatalf("unknown leioskeyepoch statement label %q", statement)
	return ""
}

// stillUnbounded lists the statements whose work is not yet independent of the
// pool's in-epoch certificates.
var stillUnbounded = map[leiosKeyStatement]string{
	// The remaining unbounded lookup: the only index that serves it orders
	// added_slot before certificate_id, so finding the latest snapshot
	// row walks every live row added since the epoch started.
	stmtLatestSnapshot: "snapshot row is found by a walk over live rows",
	// pool_retirement is indexed by pool_id alone, so every statement reads
	// all of the pool's retirements.
	stmtLatestRetirement:   "pool_retirement has no (pool_id, added_slot) index",
	stmtLatestRetireBefore: "pool_retirement has no (pool_id, added_slot) index",
}

type statementWork struct {
	statement leiosKeyStatement
	visited   int64
}

// rowCountingQuerier runs every statement leioskeyepoch issues twice: once with
// a probe that counts the rows it visits, and once unchanged for the result.
type rowCountingQuerier struct {
	t       *testing.T
	db      *sql.DB
	token   int64
	counter *atomic.Int64
	work    []statementWork
}

func newRowCountingQuerier(t *testing.T, db *sql.DB) *rowCountingQuerier {
	t.Helper()
	registerRowProbe()
	q := &rowCountingQuerier{
		t:       t,
		db:      db,
		token:   rowProbeTokens.Add(1),
		counter: &atomic.Int64{},
	}
	rowProbeCounters.Store(q.token, q.counter)
	t.Cleanup(func() { rowProbeCounters.Delete(q.token) })
	return q
}

// instrument adds the probe to every filter that constrains a table by pool
// (or by slot, for the epoch table), which is the filter that the index
// access satisfies row by row.
func (q *rowCountingQuerier) instrument(query string) string {
	probe := fmt.Sprintf(" AND %s(%d) = 1", rowProbeName, q.token)
	out := query
	for _, term := range []string{
		"pool_id = ?", "start_slot <= ?",
	} {
		out = strings.ReplaceAll(out, term, term+probe)
	}
	return out
}

func (q *rowCountingQuerier) plan(query string, args []any) string {
	q.t.Helper()
	rows, err := q.db.QueryContext(
		context.Background(), "EXPLAIN QUERY PLAN "+query, args...,
	)
	require.NoError(q.t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(q.t, rows.Scan(&id, &parent, &unused, &detail))
		lines = append(lines, detail)
	}
	require.NoError(q.t, rows.Err())
	return strings.Join(lines, "\n")
}

func (q *rowCountingQuerier) measure(query string, args []any) {
	q.t.Helper()
	statement := classifyLeiosKeyStatement(q.t, query)
	instrumented := q.instrument(query)
	require.NotEqual(
		q.t, query, instrumented,
		"statement %s has no probe site", statement,
	)
	// A probe that changes the access path measures a different statement.
	require.Equal(
		q.t, q.plan(query, args), q.plan(instrumented, args),
		"probe changed the plan of %s", statement,
	)
	q.counter.Store(0)
	rows, err := q.db.QueryContext(context.Background(), instrumented, args...)
	require.NoError(q.t, err)
	for rows.Next() {
	}
	require.NoError(q.t, rows.Err())
	require.NoError(q.t, rows.Close())
	q.work = append(q.work, statementWork{
		statement: statement,
		visited:   q.counter.Load(),
	})
}

func (q *rowCountingQuerier) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	q.measure(query, args)
	return q.db.QueryContext(ctx, query, args...)
}

func (q *rowCountingQuerier) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	q.measure(query, args)
	return q.db.QueryRowContext(ctx, query, args...)
}

// visitedByStatement sums the rows each kind of statement visited.
func (q *rowCountingQuerier) visitedByStatement() map[leiosKeyStatement]int64 {
	out := make(map[leiosKeyStatement]int64)
	for _, w := range q.work {
		out[w.statement] += w.visited
	}
	return out
}

const (
	rowsEpochLength = 1_000_000
	rowsEpoch       = 5
	rowsEpochStart  = rowsEpoch * rowsEpochLength
)

func rowsEpochs(t *testing.T, store *Store) {
	t.Helper()
	for epoch := range uint64(9) {
		require.NoError(t, store.SetEpoch(
			epoch*rowsEpochLength, epoch, nil, nil, nil, nil,
			6, 1, rowsEpochLength, nil,
		))
	}
}

// seedInEpochRegistrations registers a pool with a key in epoch 2 and then
// re-registers it n times inside epoch 5. With retire set, each
// re-registration follows a retirement naming a later epoch.
func seedInEpochRegistrations(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	n int,
	retire bool,
) (next uint64) {
	t.Helper()
	writeDepositHeldCert(
		t, store, 2_000_050, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500,
	)
	slot := uint64(rowsEpochStart + 100)
	for range n {
		if retire {
			writeDepositHeldCert(
				t, store, slot, 0, depositHeldRetirement(pool, 8), 0,
			)
			slot += 10
		}
		writeDepositHeldCert(
			t, store, slot, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0,
		)
		slot += 10
	}
	return slot
}

// registrationWork derives one more re-registration's epoch and returns the
// rows each statement visited.
func registrationWork(
	t *testing.T,
	n int,
	retire bool,
) map[leiosKeyStatement]int64 {
	t.Helper()
	// Registered before the store opens its connections, which snapshot the
	// functions that exist when they open.
	registerRowProbe()
	store := newDepositHeldStore(t)
	rowsEpochs(t, store)
	pool := depositHeldPoolKey(0xf1)
	at := seedInEpochRegistrations(t, store, pool, n, retire)
	querier := newRowCountingQuerier(t, store.writeDB)
	epoch, ageUnknown, err := leioskeyepoch.Registration(
		context.Background(),
		querier,
		leiosKeyEpochPoolID(t, store, pool),
		leioskeyepoch.Position{Slot: at},
		at,
		bytes.Repeat([]byte{0x11}, 96),
		bytes.Repeat([]byte{0x21}, 48),
	)
	require.NoError(t, err)
	require.False(t, ageUnknown)
	// The key has been active since its registration in epoch 2.
	require.NotNil(t, epoch)
	require.Equal(t, uint64(2), *epoch)
	return querier.visitedByStatement()
}

// The rows a keyed registration reads do not grow with the pool's in-epoch
// registrations, so n re-registrations in one epoch cost n times a constant
// rather than n squared.
func TestPoolLeiosKeyEpochRowsVisitedIndependentOfInEpochRegistrations(t *testing.T) {
	t.Parallel()
	for _, retire := range []bool{false, true} {
		t.Run(fmt.Sprintf("retire=%v", retire), func(t *testing.T) {
			t.Parallel()
			small := registrationWork(t, 10, retire)
			large := registrationWork(t, 300, retire)
			require.Equal(
				t,
				slices.Sorted(maps.Keys(small)),
				slices.Sorted(maps.Keys(large)),
			)
			for statement, visited := range small {
				if reason, ok := stillUnbounded[statement]; ok {
					t.Logf(
						"%s excluded (%s): %d rows at n=10, %d at n=300",
						statement, reason, visited, large[statement],
					)
					continue
				}
				require.Equalf(
					t, visited, large[statement],
					"%s visited %d rows at n=10 and %d at n=300",
					statement, visited, large[statement],
				)
				require.LessOrEqualf(
					t, large[statement], int64(12),
					"%s visited %d rows", statement, large[statement],
				)
			}
		})
	}
}

// writeLeiosClosureCert writes a certificate the way the epoch-boundary
// closure path does: added at slot, executed at the parent's earlier
// contextSlot.
func writeLeiosClosureCert(
	t *testing.T,
	store *Store,
	slot uint64,
	contextSlot uint64,
	cert lcommon.Certificate,
	deposit uint64,
) {
	t.Helper()
	hash := bytes.Repeat([]byte{0xc1}, 32)
	hash[0] = byte(slot)
	hash[1] = byte(slot >> 8)
	hash[2] = byte(slot >> 16)
	tx := mockledger.NewTransactionBuilder().WithCertificates(cert)
	tx.WithId(hash)
	tx.WithValid(true)
	require.NoError(t, store.SetTransactionLeiosClosureInContext(
		tx,
		ocommon.Point{Slot: slot, Hash: hash},
		0,
		map[int]uint64{0: deposit},
		false,
		contextSlot,
		nil, 0,
	))
}

// A closure executed in the previous epoch but certified by the first block of
// the next one is a registration of the previous epoch: it is the state in
// force when the next epoch starts, though it was added after the boundary.
func TestPoolLeiosKeyEpochClosureAcrossBoundaryIsPriorState(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xf7)

	writeDepositHeldCert(t, store, 4_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	// Executed in epoch 4, certified by the first block of epoch 5.
	writeLeiosClosureCert(t, store, 5_005, 4_990, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 5_200, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	// 0x12 was staged in epoch 4 and is the key in force from epoch 5, so
	// repeating it keeps epoch 5 and returning to 0x11 is staged for 6.
	require.Equal(t, []int64{4, 5, 5, 6}, leiosKeyEpochsInOrder(t, store, pool))
}

// A retirement closure executed before the epoch but certified after it is
// likewise part of the prior state.
func TestPoolLeiosKeyEpochRetirementClosureAcrossBoundaryReapsPool(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xf8)

	writeDepositHeldCert(t, store, 4_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeLeiosClosureCert(t, store, 5_005, 4_990, depositHeldRetirement(pool, 5), 0)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_200, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)

	// Reaped at the boundary into 5, then registered afresh in 5, where a
	// different key is staged for 6.
	require.Equal(t, []int64{4, 5, 6}, leiosKeyEpochsInOrder(t, store, pool))
}

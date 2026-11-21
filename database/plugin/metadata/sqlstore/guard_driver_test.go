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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var errGuarded = errors.New("guarded statement")

// refusingGuard refuses any statement containing needle and records every
// statement it was shown.
type refusingGuard struct {
	needle string
	mu     sync.Mutex
	seen   []string
}

func (g *refusingGuard) CheckStatement(query string) error {
	g.mu.Lock()
	g.seen = append(g.seen, query)
	g.mu.Unlock()
	if strings.Contains(query, g.needle) {
		return errGuarded
	}
	return nil
}

var guardDBSequence atomic.Int64

func TestGuardedDBVetsEveryStatementPath(t *testing.T) {
	t.Parallel()
	guard := &refusingGuard{needle: "forbidden"}
	dsn := fmt.Sprintf(
		"file:guard_driver_%d?mode=memory&cache=shared",
		guardDBSequence.Add(1),
	)
	db, err := OpenGuardedDB("sqlite", dsn, "sqlite", false, guard)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()

	_, err = db.ExecContext(ctx, "CREATE TABLE allowed (id INTEGER)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE forbidden (id INTEGER)")
	require.ErrorIs(t, err, errGuarded, "exec")

	_, err = db.QueryContext(ctx, "SELECT id FROM forbidden")
	require.ErrorIs(t, err, errGuarded, "query")

	_, err = db.PrepareContext(ctx, "SELECT id FROM forbidden")
	require.ErrorIs(t, err, errGuarded, "prepare")

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.PrepareContext(ctx, "SELECT id FROM forbidden")
	require.ErrorIs(t, err, errGuarded, "prepare in a transaction")
	_, err = tx.ExecContext(ctx, "DELETE FROM forbidden")
	require.ErrorIs(t, err, errGuarded, "exec in a transaction")
	require.NoError(t, tx.Rollback())

	stmt, err := db.PrepareContext(ctx, "INSERT INTO allowed (id) VALUES (?)")
	require.NoError(t, err)
	_, err = stmt.ExecContext(ctx, 7)
	require.NoError(t, err, "an allowed statement still runs, with arguments")
	require.NoError(t, stmt.Close())
	var id int
	require.NoError(
		t,
		db.QueryRowContext(ctx, "SELECT id FROM allowed").Scan(&id),
	)
	require.Equal(t, 7, id)
}

func TestOpenGuardedDBWithoutAGuardIsPlain(t *testing.T) {
	t.Parallel()
	dsn := fmt.Sprintf(
		"file:guard_driver_%d?mode=memory&cache=shared",
		guardDBSequence.Add(1),
	)
	db, err := OpenGuardedDB("sqlite", dsn, "sqlite", false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("CREATE TABLE forbidden (id INTEGER)")
	require.NoError(t, err)
}

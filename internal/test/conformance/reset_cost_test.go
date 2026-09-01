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

//go:build dingo_extra_plugins

package conformance

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// fakeResetter builds a backendResetter with no real database, recording what
// each injected hook was asked to do.
type fakeResetter struct {
	resetter    *backendResetter
	listCalls   int
	listReturns [][]string
	truncated   [][]string
	dirty       []string
}

func newFakeResetter(t *testing.T, listReturns ...[]string) *fakeResetter {
	t.Helper()
	f := &fakeResetter{listReturns: listReturns}
	f.resetter = &backendResetter{
		listTables: func(
			context.Context,
			*sql.DB,
		) ([]string, error) {
			idx := f.listCalls
			f.listCalls++
			if idx >= len(f.listReturns) {
				idx = len(f.listReturns) - 1
			}
			return f.listReturns[idx], nil
		},
		qualify: func(table string) string { return `"s"."` + table + `"` },
		probeDirty: func(
			_ context.Context,
			_ *sql.DB,
			_ []string,
		) ([]string, error) {
			return f.dirty, nil
		},
		truncate: func(
			_ context.Context,
			_ *sql.DB,
			qualified []string,
		) error {
			f.truncated = append(f.truncated, qualified)
			return nil
		},
	}
	return f
}

// TestBackendResetterCachesTableList proves the information_schema query runs
// once rather than once per Reset. Reset runs once per vector across a
// ~315-vector corpus, so re-deriving a table list that cannot change (it is
// fixed by the migrations that ran at construction) was 315 wasted round trips
// per backend per replay.
func TestBackendResetterCachesTableList(t *testing.T) {
	f := newFakeResetter(t, []string{"a", "b"})
	f.dirty = []string{`"s"."a"`}

	for range 5 {
		require.NoError(t, f.resetter.reset(context.Background()))
	}

	require.Equal(
		t,
		1,
		f.listCalls,
		"table list should be discovered once and cached, not re-queried "+
			"per Reset",
	)
}

// TestBackendResetterDoesNotCacheEmptyDiscovery proves an empty discovery is
// retried rather than remembered. Reset can run before any construction has
// migrated the schema, and caching that would leave the resetter permanently
// convinced there is nothing to truncate -- which would silently leak every
// vector's rows into the next one once migrations did land.
func TestBackendResetterDoesNotCacheEmptyDiscovery(t *testing.T) {
	f := newFakeResetter(t, nil, []string{"a"})
	f.dirty = []string{`"s"."a"`}

	require.NoError(t, f.resetter.reset(context.Background()))
	require.Empty(
		t,
		f.truncated,
		"nothing migrated yet, so nothing should be truncated",
	)

	require.NoError(t, f.resetter.reset(context.Background()))
	require.Equal(t, 2, f.listCalls, "empty discovery must be retried")
	require.Equal(
		t,
		[][]string{{`"s"."a"`}},
		f.truncated,
		"the retried discovery should be used",
	)
}

// TestBackendResetterTruncatesOnlyDirtyTables proves the truncate hook is
// handed exactly the tables holding rows, not the whole schema. This is the
// change that makes reset cost proportional to what a vector wrote: MySQL has
// no multi-table TRUNCATE, so before this it issued one implicit-commit DDL
// statement for all 84 tables on every vector.
func TestBackendResetterTruncatesOnlyDirtyTables(t *testing.T) {
	f := newFakeResetter(t, []string{"a", "b", "c", "d"})
	f.dirty = []string{`"s"."b"`, `"s"."d"`}

	require.NoError(t, f.resetter.reset(context.Background()))

	require.Equal(
		t,
		[][]string{{`"s"."b"`, `"s"."d"`}},
		f.truncated,
		"only the tables reported dirty should be truncated",
	)
}

// TestBackendResetterSkipsTruncateEntirelyWhenClean proves a vector that wrote
// nothing issues no DDL at all, rather than truncating every table to no
// effect.
func TestBackendResetterSkipsTruncateEntirelyWhenClean(t *testing.T) {
	f := newFakeResetter(t, []string{"a", "b"})
	f.dirty = nil

	require.NoError(t, f.resetter.reset(context.Background()))

	require.Empty(
		t,
		f.truncated,
		"no table holds rows, so no TRUNCATE should be issued",
	)
}

// nonEmptyTables' query has to be valid in two dialects at once, and the way
// it can be wrong is a syntax error rather than a wrong answer: the index must
// be a single-quoted SQL string literal, since double quotes are an identifier
// reference in PostgreSQL and would select a column named after the number.
//
// Only a real server catches that, so these run the actual query against each
// backend rather than asserting on the generated string. Reintroducing double
// quotes fails TestNonEmptyTablesPostgres.

// TestNonEmptyTablesPostgres proves the probe returns exactly the tables
// holding rows on PostgreSQL.
func TestNonEmptyTablesPostgres(t *testing.T) {
	skipIfPostgresConformanceNotConfigured(t)

	db, err := sql.Open("pgx", postgresConformanceDSN())
	require.NoError(t, err)
	defer db.Close()

	schema := "probe_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	_, err = db.Exec(`CREATE SCHEMA ` + pgQuoteIdent(schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA ` + pgQuoteIdent(schema) + ` CASCADE`)
	})

	assertProbeFindsOnlyPopulated(t, db, schema, pgQuoteQualified,
		func(qualified string) string {
			return `CREATE TABLE ` + qualified + ` (v integer)`
		},
	)
}

// TestNonEmptyTablesMysql proves the same on MySQL, whose grammar is the
// stricter of the two.
func TestNonEmptyTablesMysql(t *testing.T) {
	skipIfMysqlConformanceNotConfigured(t)

	cfg, err := mysqldriver.ParseDSN(mysqlConformanceRootDSN())
	require.NoError(t, err)
	cfg.DBName = ""
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()

	database := "probe_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	_, err = db.Exec(
		"CREATE DATABASE " + mysqlQuoteIdentifier(database),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(
			"DROP DATABASE " + mysqlQuoteIdentifier(database),
		)
	})

	qualify := func(schema, table string) string {
		return mysqlQuoteIdentifier(schema) + "." +
			mysqlQuoteIdentifier(table)
	}
	assertProbeFindsOnlyPopulated(t, db, database, qualify,
		func(qualified string) string {
			return "CREATE TABLE " + qualified + " (v int)"
		},
	)
}

// assertProbeFindsOnlyPopulated creates three tables, populates the middle
// one, and asserts nonEmptyTables reports that one and only that one. The
// empty-in, empty-out case is asserted first, since "no table holds rows" is
// the case that lets a vector skip DDL entirely.
func assertProbeFindsOnlyPopulated(
	t *testing.T,
	db *sql.DB,
	namespace string,
	qualify func(string, string) string,
	createStmt func(string) string,
) {
	t.Helper()
	ctx := context.Background()

	names := []string{"t_a", "t_b", "t_c"}
	qualified := make([]string, len(names))
	for i, name := range names {
		qualified[i] = qualify(namespace, name)
		_, err := db.ExecContext(ctx, createStmt(qualified[i]))
		require.NoError(t, err, "create %s", qualified[i])
	}

	dirty, err := nonEmptyTables(ctx, db, qualified)
	require.NoError(t, err, "probe over three empty tables")
	require.Empty(t, dirty, "no table holds rows yet")

	_, err = db.ExecContext(
		ctx, "INSERT INTO "+qualified[1]+" (v) VALUES (1)",
	)
	require.NoError(t, err)

	dirty, err = nonEmptyTables(ctx, db, qualified)
	require.NoError(t, err, "probe with one populated table")
	require.Equal(
		t,
		[]string{qualified[1]},
		dirty,
		"probe should report exactly the populated table",
	)

	_, err = db.ExecContext(
		ctx, "INSERT INTO "+qualified[2]+" (v) VALUES (2)",
	)
	require.NoError(t, err)

	dirty, err = nonEmptyTables(ctx, db, qualified)
	require.NoError(t, err, "probe with two populated tables")
	require.ElementsMatch(
		t,
		[]string{qualified[1], qualified[2]},
		dirty,
		"probe should report both populated tables",
	)
}

// TestBackendResetterTruncatesExtraDirtyTables proves a table reported by
// extraDirty is truncated even though it holds no rows.
//
// MySQL's TRUNCATE is what resets AUTO_INCREMENT, so a vector that inserts and
// then deletes rows leaves the table empty with its counter advanced. Skipping
// it because it is empty would carry that counter into the next vector.
func TestBackendResetterTruncatesExtraDirtyTables(t *testing.T) {
	f := newFakeResetter(t, []string{"a", "b"})
	f.dirty = nil
	f.resetter.extraDirty = func(
		context.Context,
		*sql.DB,
		[]string,
	) ([]string, error) {
		return []string{"b"}, nil
	}

	require.NoError(t, f.resetter.reset(context.Background()))

	require.Equal(
		t,
		[][]string{{`"s"."b"`}},
		f.truncated,
		"an empty table reported by extraDirty must still be truncated",
	)
}

// TestBackendResetterDeduplicatesExtraDirtyTables proves a table reported by
// both the row probe and extraDirty is truncated once, not twice.
func TestBackendResetterDeduplicatesExtraDirtyTables(t *testing.T) {
	f := newFakeResetter(t, []string{"a", "b"})
	f.dirty = []string{`"s"."b"`}
	f.resetter.extraDirty = func(
		context.Context,
		*sql.DB,
		[]string,
	) ([]string, error) {
		return []string{"b"}, nil
	}

	require.NoError(t, f.resetter.reset(context.Background()))

	require.Equal(
		t,
		[][]string{{`"s"."b"`}},
		f.truncated,
		"a table reported by both criteria must be truncated once",
	)
}

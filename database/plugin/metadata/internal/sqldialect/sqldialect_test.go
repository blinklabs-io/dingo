// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sqldialect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type namedDialector string

func (d namedDialector) Name() string                                 { return string(d) }
func (namedDialector) Initialize(*gorm.DB) error                      { return nil }
func (namedDialector) Migrator(*gorm.DB) gorm.Migrator                { return nil }
func (namedDialector) DataTypeOf(*schema.Field) string                { return "" }
func (namedDialector) DefaultValueOf(*schema.Field) clause.Expression { return nil }
func (namedDialector) BindVarTo(clause.Writer, *gorm.Statement, any)  {}
func (namedDialector) QuoteTo(clause.Writer, string)                  {}
func (namedDialector) Explain(string, ...any) string                  { return "" }

func dialectDB(name string) *gorm.DB {
	return &gorm.DB{Config: &gorm.Config{Dialector: namedDialector(name)}}
}

func TestExactUint64ArithmeticDialects(t *testing.T) {
	tests := []struct {
		name       string
		castType   string
		arithmetic string
	}{
		{name: "sqlite", castType: "INTEGER", arithmetic: "?"},
		{name: "mysql", castType: "UNSIGNED", arithmetic: "CAST(? AS UNSIGNED)"},
		{name: "postgres", castType: "NUMERIC", arithmetic: "CAST(? AS NUMERIC)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := dialectDB(test.name)
			require.Equal(t, test.castType, IntegerCastType(db))
			require.Equal(t, test.arithmetic, ArithmeticParam(db))
		})
	}
}

// TestTwoColumnRangeConditionDialects pins the per-backend predicate form
// chosen by TwoColumnRangeCondition. This is not a stylistic choice: real
// sqlite/postgres/mysql containers were seeded with a composite index
// matching idx_addr_tx_stake_position and EXPLAIN was run for both the
// row-value and expanded-OR forms during the dingo PR #3016 review.
// sqlite and postgres folded the row-value form into the index range scan
// (postgres: EXPLAIN ANALYZE showed all four columns in "Index Cond" with
// no residual Filter; sqlite: see the accounthistory package's own EXPLAIN
// QUERY PLAN regression tests). mysql did not - EXPLAIN showed the
// row-value form using only the two leading equality columns ("ref"
// access) and applying the range as a residual filter, while the expanded
// OR form restored a real "range" access type over all four index
// columns. If this behavior ever changes (a MySQL fix, a newly added
// backend), this test must be updated deliberately alongside new EXPLAIN
// evidence, not guessed at.
func TestTwoColumnRangeConditionDialects(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		wantCond string
		wantArgs []any
	}{
		{
			name:     "sqlite_from",
			op:       ">=",
			wantCond: "(slot, tx_index) >= (?, ?)",
			wantArgs: []any{100, 5},
		},
		{
			name:     "sqlite_to",
			op:       "<=",
			wantCond: "(slot, tx_index) <= (?, ?)",
			wantArgs: []any{100, 5},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := dialectDB("sqlite")
			cond, args := TwoColumnRangeCondition(
				db, "slot", "tx_index", test.op, 100, 5,
			)
			require.Equal(t, test.wantCond, cond)
			require.Equal(t, test.wantArgs, args)
		})
	}

	t.Run("postgres_uses_row_value_form", func(t *testing.T) {
		db := dialectDB("postgres")
		cond, args := TwoColumnRangeCondition(
			db, "slot", "tx_index", ">=", 100, 5,
		)
		require.Equal(t, "(slot, tx_index) >= (?, ?)", cond)
		require.Equal(t, []any{100, 5}, args)
	})

	t.Run("mysql_from_uses_expanded_or_form", func(t *testing.T) {
		db := dialectDB("mysql")
		cond, args := TwoColumnRangeCondition(
			db, "slot", "tx_index", ">=", 100, 5,
		)
		require.Equal(t, "(slot > ? OR (slot = ? AND tx_index >= ?))", cond)
		require.Equal(t, []any{100, 100, 5}, args)
	})

	t.Run("mysql_to_uses_expanded_or_form", func(t *testing.T) {
		db := dialectDB("mysql")
		cond, args := TwoColumnRangeCondition(
			db, "slot", "tx_index", "<=", 100, 5,
		)
		require.Equal(t, "(slot < ? OR (slot = ? AND tx_index <= ?))", cond)
		require.Equal(t, []any{100, 100, 5}, args)
	})
}

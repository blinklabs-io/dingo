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

// Package sqldialect centralizes SQL-dialect literals shared by metadata query
// packages. Keeping one implementation prevents a corrected or newly added
// backend from silently leaving account-history queries inconsistent with
// stake queries. Every helper is pure and treats a nil db the same as sqlite.
package sqldialect

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// Name returns the lower-cased dialect name for db, or "" for a nil db
// (treated the same as sqlite by every helper below).
func Name(db *gorm.DB) string {
	if db == nil {
		return ""
	}
	return strings.ToLower(db.Name())
}

// TransactionTableName returns the quoted reference to the "transaction"
// table for the backend dialect of db. mysql does not require quoting here
// (TRANSACTION is not a reserved word), but backtick-quoting it is always
// valid there; postgres and sqlite both require double quotes because
// "transaction" collides with the SQL TRANSACTION keyword in unquoted
// position.
func TransactionTableName(db *gorm.DB) string {
	if Name(db) == "mysql" {
		return "`transaction`"
	}
	return `"transaction"`
}

// IntegerCastType returns the backend-native integer type used to CAST the
// text-encoded (types.Uint64) columns before arithmetic or numeric
// comparison. The utxo.amount and account.reward columns used by the metadata
// stake and voting-power queries are stored as decimal-string TEXT (see
// types.Uint64.Value) regardless of backend, so any SUM, arithmetic, or
// ordering comparison touching them must cast first:
// sqlite is loosely typed and tolerates INTEGER; mysql needs UNSIGNED to
// preserve values above math.MaxInt64 exactly (a signed cast would
// overflow); postgres needs NUMERIC and, unlike sqlite/mysql, never
// implicitly coerces TEXT to a numeric type, so postgres queries fail
// outright without the cast.
func IntegerCastType(db *gorm.DB) string {
	switch Name(db) {
	case "postgres":
		return "NUMERIC"
	case "mysql":
		return "UNSIGNED"
	default:
		return "INTEGER"
	}
}

// TextCastType returns the CAST target for the decimal-string representation
// stored in types.Uint64 columns.
func TextCastType(db *gorm.DB) string {
	if Name(db) == "mysql" {
		return "CHAR"
	}
	return "TEXT"
}

// BoolLiteral renders a boolean literal for raw SQL.
func BoolLiteral(db *gorm.DB, value bool) string {
	if Name(db) == "mysql" || Name(db) == "postgres" {
		if value {
			return "TRUE"
		}
		return "FALSE"
	}
	if value {
		return "1"
	}
	return "0"
}

// ArithmeticParam returns the bind fragment for a types.Uint64 operand used
// directly in arithmetic. MySQL and Postgres require an explicit numeric cast
// so a bound decimal string never promotes the expression to floating point.
func ArithmeticParam(db *gorm.DB) string {
	if Name(db) == "postgres" || Name(db) == "mysql" {
		return fmt.Sprintf("CAST(? AS %s)", IntegerCastType(db))
	}
	return "?"
}

// TwoColumnRangeCondition returns the SQL fragment (with "?" placeholders)
// and its ordered bind args for an inclusive two-column range bound against
// (col1, col2), in whichever of the two logically equivalent forms actually
// folds into a composite-index range scan for db's backend dialect. op must
// be ">=" (an inclusive lower/"from" bound) or "<=" (an inclusive
// upper/"to" bound).
//
// This was verified empirically, not assumed: scratch sqlite/postgres/mysql
// databases were seeded with a real composite index and EXPLAIN was run for
// both forms during the dingo PR #3016 review (see
// database/plugin/metadata/internal/accounthistory/transactions_test.go and
// DATABASE.md's GetAddressTransactionsByCredential entry for the recorded
// plans).
//
//   - sqlite and postgres fold the row-value form, "(col1, col2) >= (v1,
//     v2)", directly into an index range scan over both bound columns
//     (confirmed via EXPLAIN QUERY PLAN on sqlite and EXPLAIN ANALYZE on
//     postgres: both show the full two-column bound inside the index
//     condition, not a post-scan filter).
//   - mysql's optimizer does not perform that translation for row
//     constructor inequalities (MySQL Bug #104128, #111952 - Verified/S2,
//     both still open as of 2026-07): EXPLAIN there showed the row-value
//     form using only the index's leading equality columns ("ref" access,
//     2 used_key_parts) and applying the two-column bound as a residual
//     filter, so a page-size request cost work proportional to the row's
//     offset into the credential's full history rather than to the page.
//     The logically equivalent expanded form, "col1 > v1 OR (col1 = v1 AND
//     col2 op v2)", is what MySQL's documentation recommends for exactly
//     this situation, and EXPLAIN confirmed it restores a genuine "range"
//     access type using all four index columns there.
func TwoColumnRangeCondition(
	db *gorm.DB,
	col1, col2, op string,
	v1, v2 any,
) (string, []any) {
	if Name(db) != "mysql" {
		return fmt.Sprintf("(%s, %s) %s (?, ?)", col1, col2, op), []any{v1, v2}
	}
	strictOp := ">"
	if op == "<=" {
		strictOp = "<"
	}
	return fmt.Sprintf(
		"(%s %s ? OR (%s = ? AND %s %s ?))",
		col1, strictOp, col1, col2, op,
	), []any{v1, v1, v2}
}

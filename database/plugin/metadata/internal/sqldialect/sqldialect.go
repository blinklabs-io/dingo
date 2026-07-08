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

// Package sqldialect centralizes the handful of SQL-dialect literals that the
// metadata plugins' shared query packages (internal/rewardstate,
// internal/stakequery, internal/accounthistory) previously each re-derived
// from *gorm.DB.Name(). Keeping one implementation prevents a corrected or
// newly added backend from silently leaving reward queries inconsistent with
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
// comparison. utxo.amount, account.reward, and reward_live_stake's
// utxo_stake/reward_stake/total_stake columns are all stored as
// decimal-string TEXT (see types.Uint64.Value) regardless of backend, so any
// SUM, arithmetic, or ordering comparison touching them must cast first:
// sqlite is loosely typed and tolerates INTEGER; mysql needs UNSIGNED to
// preserve values above math.MaxInt64 exactly (a signed cast would
// overflow); postgres needs BIGINT and, unlike sqlite/mysql, never
// implicitly coerces TEXT to a numeric type, so postgres queries fail
// outright without the cast.
func IntegerCastType(db *gorm.DB) string {
	switch Name(db) {
	case "postgres":
		return "BIGINT"
	case "mysql":
		return "UNSIGNED"
	default:
		return "INTEGER"
	}
}

// TextCastType returns the CAST target that converts an arithmetic result
// back to the decimal-string representation stored in a types.Uint64
// column. mysql's CAST target for a string result is CHAR (TEXT is a column
// type keyword, not a valid CAST target, in mysql); sqlite and postgres
// both accept TEXT as a CAST target.
func TextCastType(db *gorm.DB) string {
	if Name(db) == "mysql" {
		return "CHAR"
	}
	return "TEXT"
}

// BoolLiteral renders a boolean literal for interpolation into a raw-SQL
// fragment. sqlite has no dedicated boolean literal and stores booleans as
// 0/1 integers; mysql and postgres both accept the TRUE/FALSE keywords.
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

// ArithmeticParam returns the SQL fragment used to bind a types.Uint64
// parameter for use directly in arithmetic against a
// CAST(col AS IntegerCastType) operand. types.Uint64 binds as a decimal
// string (driver.Value), which sqlite and mysql both coerce implicitly when
// used in numeric arithmetic. Postgres's extended query protocol requires a
// concrete type for every parameter and cannot infer one for a bare `?`
// used in `<bigint> + ?`, so postgres needs the parameter cast explicitly.
func ArithmeticParam(db *gorm.DB) string {
	if Name(db) == "postgres" {
		return fmt.Sprintf("CAST(? AS %s)", IntegerCastType(db))
	}
	return "?"
}

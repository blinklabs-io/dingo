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

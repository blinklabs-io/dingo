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

// Package rewardstate holds reward_live_stake and reward snapshot metadata
// query logic shared by the sqlite, mysql, and postgres
// metadata plugins. The SQL is identical across backends except for a
// handful of dialect literals (integer cast type, "transaction" table
// quoting, boolean literals, and whether a bound parameter needs an explicit
// cast). Those literals are defined once in internal/sqldialect and exposed
// here through the thin wrappers below, so this package cannot drift from
// internal/stakequery and internal/accounthistory when a backend is added or
// corrected.
package rewardstate

import (
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/sqldialect"
	"gorm.io/gorm"
)

// transactionTableName returns the quoted "transaction" table reference for
// db's backend dialect. See sqldialect.TransactionTableName.
func transactionTableName(db *gorm.DB) string {
	return sqldialect.TransactionTableName(db)
}

// integerCastType returns the backend-native integer CAST target for the
// text-encoded (types.Uint64) columns. See sqldialect.IntegerCastType.
func integerCastType(db *gorm.DB) string {
	return sqldialect.IntegerCastType(db)
}

// textCastType returns the CAST target that converts an arithmetic result
// back to a decimal string. See sqldialect.TextCastType.
func textCastType(db *gorm.DB) string {
	return sqldialect.TextCastType(db)
}

// boolLiteral renders a boolean literal for raw-SQL interpolation.
// See sqldialect.BoolLiteral.
func boolLiteral(db *gorm.DB, value bool) string {
	return sqldialect.BoolLiteral(db, value)
}

// arithmeticParam returns the bind fragment for a types.Uint64 operand used
// directly in arithmetic. See sqldialect.ArithmeticParam.
func arithmeticParam(db *gorm.DB) string {
	return sqldialect.ArithmeticParam(db)
}

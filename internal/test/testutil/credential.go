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

package testutil

import (
	"context"
	"database/sql"
	"testing"
)

// CredentialID returns the dictionary id of a credential hash, adding the
// hash when it is not held yet, for a fixture that inserts utxo or
// address_transaction rows directly. The hash is a []byte or nil; an empty
// one is no credential and yields nil, which binds as NULL.
func CredentialID(tb testing.TB, db Queryer, hash any) any {
	tb.Helper()
	key, _ := hash.([]byte)
	return dictionaryID(tb, db, "credential", "hash", key, "(hash)", "(?)")
}

// DatumID returns the id of the datum row of a datum hash, adding an
// unrevealed row (empty raw_datum) when none exists, for a fixture that
// inserts utxo rows directly. An empty hash yields nil.
func DatumID(tb testing.TB, db Queryer, hash any) any {
	tb.Helper()
	key, _ := hash.([]byte)
	return dictionaryID(
		tb, db, "datum", "hash", key,
		"(hash, raw_datum, added_slot)", "(?, X'', 0)",
	)
}

func dictionaryID(
	tb testing.TB,
	db Queryer,
	table, column string,
	key []byte,
	columns, values string,
) any {
	tb.Helper()
	if len(key) == 0 {
		return nil
	}
	if _, err := db.ExecContext(
		context.Background(),
		"INSERT INTO "+table+" "+columns+" VALUES "+values+
			" ON CONFLICT ("+column+") DO NOTHING", key,
	); err != nil {
		tb.Fatalf("insert %s row: %v", table, err)
	}
	var id int64
	if err := db.QueryRowContext(
		context.Background(),
		"SELECT id FROM "+table+" WHERE "+column+" = ?", key,
	).Scan(&id); err != nil && err != sql.ErrNoRows {
		tb.Fatalf("look up %s id: %v", table, err)
	}
	return id
}

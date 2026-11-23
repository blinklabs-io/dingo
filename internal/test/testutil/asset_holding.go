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
	"database/sql"
	"testing"
)

// SQLExecer is the part of *sql.DB and *sql.Tx InsertAssetHolding needs.
type SQLExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// InsertAssetHolding writes one asset_holding row, creating the asset's
// asset_identity row when it does not exist. A nil name is the empty name. It
// writes through plain SQL so a test can seed a metadata schema without the
// store's write path.
func InsertAssetHolding(
	tb testing.TB,
	db SQLExecer,
	utxoID int64,
	policyID, name []byte,
	amount any,
) {
	tb.Helper()
	if name == nil {
		name = []byte{}
	}
	if _, err := db.Exec(
		`INSERT INTO asset_identity (policy_id, name) VALUES (?, ?)
ON CONFLICT (policy_id, name) DO NOTHING`,
		policyID, name,
	); err != nil {
		tb.Fatalf("insert asset identity: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO asset_holding (utxo_id, asset_id, amount)
SELECT ?, id, ? FROM asset_identity WHERE policy_id = ? AND name = ?`,
		utxoID, amount, policyID, name,
	); err != nil {
		tb.Fatalf("insert asset holding: %v", err)
	}
}

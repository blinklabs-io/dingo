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

package ledger_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/livesettest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
)

func init() {
	ledger.OpenLiveSetForTest = func(t testing.TB, db *database.Database) ledger.UtxoSetWriter {
		t.Helper()
		return livesettest.OpenLedgerDB(t, db)
	}
	ledger.LiveSetOfForTest = func(db *database.Database) (ledger.UtxoSetWriter, bool) {
		set, ok := livesettest.LedgerDBOf(db)
		return set, ok
	}
	ledger.AdoptLiveSetForTest = func(
		t testing.TB, db *database.Database, set ledger.UtxoSetWriter,
	) {
		t.Helper()
		inner, ok := set.(*utxoset.Set)
		if !ok {
			// A test's wrapper around the set it observes or fails.
			inner = set.(interface{ Inner() *utxoset.Set }).Inner()
		}
		livesettest.AdoptLedgerDB(t, db, inner)
	}
	ledger.OpenLiveSetAtForTest = func(
		t testing.TB, db *database.Database, dataDir string,
	) (ledger.UtxoSetWriter, func() error) {
		t.Helper()
		set := livesettest.OpenLedgerDBAt(t, db, dataDir)
		return set, func() error { return livesettest.CloseLedgerDB(db) }
	}
}

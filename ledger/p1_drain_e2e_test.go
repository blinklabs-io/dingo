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
	"bytes"
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func tableRows(t *testing.T, raw *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM `"+table+"`").Scan(&n),
	)
	return n
}

// The startup nonce prune of a drained family checks that the set holds none
// past the ledger tip instead of deleting SQL rows that do not exist.
func TestDrainedNoncePruneChecksTheSetAgainstTheTip(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	cur := scenario.CurrentPoint()
	scenario.WriteNonces([]models.BlockNonce{{
		Slot: cur.Slot + 5, Hash: bytes.Repeat([]byte{9}, 32),
		Nonce: bytes.Repeat([]byte{8}, 32),
	}})
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces),
	)
	ldb := scenario.LedgerDB()
	err = ldb.DeleteBlockNoncesAfterPoint(cur, nil)
	require.ErrorContains(t, err, "past the ledger tip")
	require.NoError(t, ldb.DeleteBlockNoncesAfterPoint(
		ocommon.NewPoint(cur.Slot+5, bytes.Repeat([]byte{9}, 32)), nil,
	))
}

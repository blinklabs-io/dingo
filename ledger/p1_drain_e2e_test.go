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
	"context"
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// drainStore adapts the database to statedrain.Store.
type drainStore struct {
	*database.Database
	publish func(nodesettings.FamilyMask)
}

func (s drainStore) RecordDrainedFamily(
	ctx context.Context,
	f nodesettings.Family,
) error {
	if err := s.Database.RecordDrainedFamily(ctx, f); err != nil {
		return err
	}
	mask, err := s.Database.DrainedFamilies(ctx)
	if err == nil {
		s.publish(mask)
	}
	return err
}

func (s drainStore) SQLTip(context.Context) (ocommon.Point, error) {
	tip, err := s.GetTip(nil)
	return tip.Point, err
}

func (s drainStore) GetSyncState(
	_ context.Context,
	key string,
) (string, error) {
	return s.Database.GetSyncState(key, nil)
}

func (s drainStore) SetSyncState(_ context.Context, key, value string) error {
	return s.Database.SetSyncState(key, value, nil)
}

func (s drainStore) DeleteSyncState(_ context.Context, key string) error {
	return s.Database.DeleteSyncState(key, nil)
}

func (s drainStore) ListSyncStateKeys(
	_ context.Context,
	prefix string,
) ([]string, error) {
	return s.ListSyncStateKeysByPrefix(prefix, nil)
}

// drainTestSource is the statedrain.Source of a harness: the set's markers
// and comparison, with the metadata store's tip as the set's.
type drainTestSource struct {
	set *utxoset.Set
	db  *database.Database
}

func (s drainTestSource) Tip() (ocommon.Point, bool, error) {
	tip, err := s.db.GetTip(nil)
	return tip.Point, true, err
}

func (s drainTestSource) Loaded(f nodesettings.Family) (bool, error) {
	return ledger.FamilyLoaded(f, s.set)
}

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
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))
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

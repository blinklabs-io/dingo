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

package database_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// stagedSource is a statedrain.Source that holds nothing: the test reads only
// the table lists of the graduated and staged entries.
type stagedSource struct{}

func (stagedSource) Tip() (ocommon.Point, bool, error) {
	return ocommon.Point{}, false, nil
}

func (stagedSource) Loaded(nodesettings.Family) (bool, error) {
	return false, nil
}

func (stagedSource) Verify(context.Context, nodesettings.Family) error {
	return nil
}

func armStaged(guard *statedrain.TripGuard, family nodesettings.Family) {
	entries := append(
		statedrain.Graduated(
			stagedSource{},
		),
		statedrain.Staged(stagedSource{})...,
	)
	for _, e := range entries {
		if e.Family != family {
			continue
		}
		names := make([]string, 0, len(e.Tables))
		for _, table := range e.Tables {
			names = append(names, table.Name)
		}
		guard.Arm(family, names)
	}
}

// A rollback of a database whose accounts and dreps families are drained
// restores and deletes nothing in their tables, and a direct read or write of
// them is refused before it reaches the store.
func TestTruncateAfterSlotWritesNothingToDrainedAccountTables(t *testing.T) {
	t.Parallel()
	families := []nodesettings.Family{
		nodesettings.FamilyAccounts, nodesettings.FamilyDReps,
		nodesettings.FamilyMIRGenesis,
	}
	db, guard, point := newDrainedTruncateDB(t, families...)
	for _, f := range families {
		armStaged(guard, f)
	}
	want := bytes.Repeat([]byte{0xc2}, 32)
	require.NoError(
		t,
		db.SetBlockNonce(point.Hash, point.Slot, want, true, nil),
	)

	_, nonce, err := db.TruncateAfterSlot(t.Context(), point, 0, nil)

	require.NoError(t, err)
	require.Empty(t, guard.Trips(), "a drained table was touched")
	require.Equal(t, want, nonce)

	key := bytes.Repeat([]byte{1}, 28)
	_, err = db.GetAccountByCredential(t.Context(), 0, key, true, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	_, err = db.GetDrepByCredential(t.Context(), 0, key, true, nil)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
	require.ErrorIs(
		t, db.RestoreAccountStateAtSlot(t.Context(), 0, nil),
		database.ErrFamilyDrained,
	)
	require.Empty(t, guard.Trips(), "the refusal must not reach the table")
}

// The offline truncate collects the credentials witnessed after its target
// from the database before it rolls back. With the accounts family drained
// the set restores each account's expiration itself, so the collection is
// empty and reads none of the drained tables.
func TestAccountsWitnessedAfterSlotReadsNothingOnceAccountsAreDrained(
	t *testing.T,
) {
	t.Parallel()
	db, guard, _ := newDrainedTruncateDB(t, nodesettings.FamilyAccounts)
	armStaged(guard, nodesettings.FamilyAccounts)

	refs, err := db.AccountsWitnessedAfterSlot(t.Context(), 0, nil)

	require.NoError(t, err)
	require.Empty(t, refs)
	require.Empty(t, guard.Trips(), "a drained table was read")
}

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

package dbadapter_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The adapter's pool reads that go straight to the metadata store refuse once
// the pools-history family is drained, and answer while it is not.
func TestAdapterPoolReadsRefuseOnceTheFamilyIsDrained(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	adapter := dbadapter.New(db)
	pool := lcommon.PoolKeyHash(make([]byte, 28))
	reads := map[string]func() error{
		"list": func() error {
			_, err := adapter.ListPoolKeyHashes(nil)
			return err
		},
		"active at slot": func() error {
			_, err := adapter.GetActivePoolKeyHashesAtSlot(1, nil)
			return err
		},
		"deposits": func() error {
			_, err := adapter.GetPoolDepositsHeld(
				[]lcommon.PoolKeyHash{pool},
				nil,
			)
			return err
		},
		"deposits at slot": func() error {
			_, err := adapter.GetPoolDepositsHeldAtSlot(
				[]lcommon.PoolKeyHash{pool}, 1, nil,
			)
			return err
		},
		"retiring": func() error {
			_, err := adapter.GetRetiringPools(1, nil)
			return err
		},
		"retiring at slot": func() error {
			_, err := adapter.GetRetiringPoolsAtSlot(1, 1, nil)
			return err
		},
		"registrations at slot": func() error {
			_, err := adapter.GetPoolRegistrationsAtSlot(
				[]lcommon.PoolKeyHash{pool}, 1, nil,
			)
			return err
		},
		"effective registrations": func() error {
			_, err := adapter.GetPoolRegistrationsEffectiveForEpoch(
				[]lcommon.PoolKeyHash{pool}, 1, 1, 1, nil,
			)
			return err
		},
		"vrf at slot": func() error {
			_, _, err := adapter.GetPoolVrfKeyHashAtSlot(pool[:], 1, nil)
			return err
		},
		"earliest vrf at slot": func() error {
			_, _, err := adapter.GetPoolEarliestVrfKeyHashAtSlot(
				pool[:],
				1,
				nil,
			)
			return err
		},
		"boundary active pools": func() error {
			_, err := adapter.GetEpochBoundaryActivePoolKeyHashes(1, 1, nil)
			return err
		},
		"retired by epoch": func() error {
			_, err := adapter.GetPoolKeyHashesRetiredByEpoch(1, 1, nil)
			return err
		},
	}
	for name, read := range reads {
		require.NotErrorIs(t, read(), database.ErrFamilyDrained, name)
	}
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyPoolsHistory,
	))
	for name, read := range reads {
		require.ErrorIs(t, read(), database.ErrFamilyDrained, name)
	}
}

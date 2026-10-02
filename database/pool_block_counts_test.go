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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// anchoredBlockCountFixture seeds epochs 1 and 2 (100 slots each, epoch 2 at
// [100, 200)), a Mithril anchor at slot 150, two blocks for the returned pool
// above the anchor, and imported counts for both epochs.
func anchoredBlockCountFixture(
	t *testing.T,
	withImported bool,
) (*database.Database, lcommon.PoolKeyHash) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	for _, epoch := range []struct{ id, start uint64 }{{1, 0}, {2, 100}} {
		require.NoError(t, db.SetEpoch(
			epoch.start, epoch.id, nil, nil, nil, nil,
			0, 1, 100, nil,
		))
	}
	var pool lcommon.PoolKeyHash
	copy(pool[:], bytes.Repeat([]byte{0xa1}, len(pool)))
	meta := db.Metadata()
	require.NoError(t, meta.SetSyncState("mithril_ledger_slot", "150", nil))
	for _, slot := range []uint64{160, 170} {
		require.NoError(t, db.UpdatePoolOpCertSequence(pool, slot, slot, nil))
	}
	if withImported {
		for _, imported := range []struct{ epoch, blocks uint64 }{{1, 4}, {2, 5}} {
			require.NoError(t, meta.SaveImportedPoolBlockCounts(
				[]models.ImportedPoolBlockCount{{
					Epoch:          imported.epoch,
					PoolKeyHash:    pool[:],
					BlocksProduced: imported.blocks,
					CapturedSlot:   150,
				}},
				nil,
			))
			require.NoError(t, meta.SaveImportedEpochBlockTotal(
				imported.epoch, imported.blocks, 150, nil,
			))
		}
	}
	return db, pool
}

// A lifetime count on a bootstrapped node adds the snapshot's counts for the
// anchor's epoch and the one before it to the blocks observed above the anchor.
func TestCountPoolBlocksLifetimeIncludesImportedEpochs(t *testing.T) {
	t.Parallel()

	db, pool := anchoredBlockCountFixture(t, true)
	counts, err := database.CountPoolBlocksLifetime(
		db.Metadata(), nil, []lcommon.PoolKeyHash{pool}, math.MaxInt64,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2+5+4), counts[string(pool[:])])
}

// With no imported counts the lifetime count is the observed tail alone.
func TestCountPoolBlocksLifetimeWithoutImportedCounts(t *testing.T) {
	t.Parallel()

	db, pool := anchoredBlockCountFixture(t, false)
	counts, err := database.CountPoolBlocksLifetime(
		db.Metadata(), nil, []lcommon.PoolKeyHash{pool}, math.MaxInt64,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), counts[string(pool[:])])
}

// An epoch entirely above the anchor is counted from observed blocks only, and
// an anchored epoch with no imported counts is reported as unknown.
func TestMergeImportedPoolBlockCountsEpochCoverage(t *testing.T) {
	t.Parallel()

	db, pool := anchoredBlockCountFixture(t, false)
	observed := map[string]uint64{string(pool[:]): 2}

	counts, total, known, err := database.MergeImportedPoolBlockCounts(
		db.Metadata(), nil, 3, 200, observed, 2,
	)
	require.NoError(t, err)
	assert.True(t, known)
	assert.Equal(t, uint64(2), counts[string(pool[:])])
	assert.Equal(t, uint64(2), total)

	_, _, known, err = database.MergeImportedPoolBlockCounts(
		db.Metadata(), nil, 2, 100, observed, 2,
	)
	require.NoError(t, err)
	assert.False(t, known)
}

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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestMithrilBoundaryOpCertCertifiedBaselineIgnoresStaleHistory(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(make([]byte, 28)))
	const boundarySlot = uint64(100)
	require.NoError(t, db.UpdatePoolOpCertSequence(
		poolKeyHash,
		500,
		boundarySlot-1,
		nil,
	))
	require.NoError(t, db.UpdatePoolOpCertSequence(
		poolKeyHash,
		489,
		boundarySlot,
		nil,
	))
	ledgerState := &LedgerState{db: db, mithrilLedgerSlot: boundarySlot}

	stored, found, err := ledgerState.latestOpCertCounterForValidation(
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(489), stored)
	require.NoError(t, validateOpCertCounter(stored, found, 490, true))
	require.ErrorContains(
		t,
		validateOpCertCounter(stored, found, 491, true),
		"gapped rotation",
	)
	require.ErrorContains(
		t,
		validateOpCertCounter(stored, found, 488, true),
		"stale",
	)
}

func TestMithrilBoundaryOpCertPoolWithoutCertifiedCounterAllowsFirst(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(make([]byte, 28)))
	const boundarySlot = uint64(100)
	require.NoError(t, db.UpdatePoolOpCertSequence(
		poolKeyHash,
		1,
		boundarySlot-1,
		nil,
	))
	ledgerState := &LedgerState{db: db, mithrilLedgerSlot: boundarySlot}

	stored, found, err := ledgerState.latestOpCertCounterForValidation(
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, validateOpCertCounter(stored, found, 490, true))
}

// TestLatestOpCertSequenceRespectsMithrilBoundary pins that
// LatestOpCertSequence -- the entry point startup and forge-loop credential
// checks use through the LedgerView interface -- agrees with what block
// application itself would compute (latestOpCertCounterForValidation),
// rather than a plain MAX that would trust a stale pre-boundary row a
// Mithril import left in the table.
func TestLatestOpCertSequenceRespectsMithrilBoundary(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	var poolID [28]byte
	for i := range poolID {
		poolID[i] = byte(i + 1)
	}
	poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolID[:]))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: poolKeyHash.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: poolKeyHash.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	const boundarySlot = uint64(100)
	require.NoError(t, db.UpdatePoolOpCertSequence(
		poolKeyHash,
		500,
		boundarySlot-1,
		nil,
	))
	require.NoError(t, db.UpdatePoolOpCertSequence(
		poolKeyHash,
		489,
		boundarySlot,
		nil,
	))
	ledgerState := &LedgerState{db: db, mithrilLedgerSlot: boundarySlot}

	sequence, found, err := ledgerState.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(
		t,
		uint64(489),
		sequence,
		"must ignore the pre-boundary row a plain MAX would return",
	)
}

func TestMithrilBoundaryOpCertContiguousRotationIsEnforced(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(make([]byte, 28)))
	const boundarySlot = uint64(100)
	require.NoError(t, db.UpdatePoolOpCertSequence(
		poolKeyHash,
		490,
		boundarySlot+1,
		nil,
	))
	ledgerState := &LedgerState{db: db, mithrilLedgerSlot: boundarySlot}

	stored, found, err := ledgerState.latestOpCertCounterForValidation(
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(490), stored)
	require.NoError(t, validateOpCertCounter(stored, found, 491, true))
	require.ErrorContains(
		t,
		validateOpCertCounter(stored, found, 492, true),
		"gapped rotation",
	)
}

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
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestMithrilBoundaryOpCertGapEstablishesReplayBaseline(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(make([]byte, 28)))
	const boundarySlot = uint64(100)
	require.NoError(t, db.UpdatePoolOpCertSequence(
		poolKeyHash,
		1,
		boundarySlot,
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

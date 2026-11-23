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
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

// errRollbackEpochReloadInjected is the sentinel error the test below injects
// into GetEpochs, so assertions can distinguish it from any other error the
// rollback path might otherwise raise.
var errRollbackEpochReloadInjected = errors.New(
	"injected GetEpochs failure after rollback commit",
)

// TestRollbackWithResyncFailsFastWhenEpochReloadFailsAfterCommit pins the
// fail-fast case for epoch reload after commit: rollbackWithBlocks's
// post-commit reload of epochCache/currentEra/currentPParams can fail after the
// metadata truncation has already committed. Today that failure is logged at
// Warn and the rollback still reports success (nil), leaving those in-memory
// caches at their pre-rollback values even though the database itself was
// truncated -- a later block then validates against stale
// era/epoch/protocol-parameter state. After the fix this must surface as a
// rollbackCommittedError and invoke FatalErrorFunc, so a supervised restart
// reloads the caches from the database before any further block is validated.
func TestRollbackWithResyncFailsFastWhenEpochReloadFailsAfterCommit(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	faults := fxFaultedLedger(t, ls, SetFaultsForTest{})
	faults.AfterRollback = func() {
		faults.Epochs = errRollbackEpochReloadInjected
	}

	fatalCalled := false
	var fatalErr error
	ls.config.FatalErrorFunc = func(err error) {
		fatalCalled = true
		fatalErr = err
	}

	rbErr := ls.rollbackWithBlocksAndIntent(
		context.Background(),
		fixture.ancestorTip.Point,
		nil,
		false,
		false,
		false,
	)

	require.Error(
		t,
		rbErr,
		"a post-commit epoch reload failure must not be reported as a "+
			"successful rollback",
	)
	var committedErr *rollbackCommittedError
	require.ErrorAs(
		t,
		rbErr,
		&committedErr,
		"a post-commit reload failure must be reported as a "+
			"rollbackCommittedError so callers treat the metadata "+
			"rollback as already applied rather than as a refused, "+
			"nothing-happened rollback",
	)
	require.ErrorIs(t, rbErr, errRollbackEpochReloadInjected)

	require.True(
		t,
		fatalCalled,
		"a post-commit epoch reload failure must invoke FatalErrorFunc so "+
			"a supervised restart reloads era/epoch/protocol-parameter "+
			"state from the database instead of continuing to validate "+
			"blocks against stale in-memory state",
	)
	require.ErrorIs(t, fatalErr, errRollbackEpochReloadInjected)

	// The metadata truncation itself committed; the in-memory tip must
	// reflect it even though the epoch/era/pparams reload failed.
	require.Equal(t, fixture.ancestorTip.Point, ls.currentTip.Point)
}

// TestRollbackWithResyncSucceedsWithoutFatalWhenReloadWorks is the negative
// case: an ordinary rollback where every post-commit reload succeeds must not
// report a rollbackCommittedError or invoke FatalErrorFunc.
func TestRollbackWithResyncSucceedsWithoutFatalWhenReloadWorks(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	fatalCalled := false
	ls.config.FatalErrorFunc = func(error) {
		fatalCalled = true
	}

	rbErr := ls.rollbackWithBlocksAndIntent(
		context.Background(),
		fixture.ancestorTip.Point,
		nil,
		false,
		false,
		false,
	)

	require.NoError(t, rbErr)
	require.False(
		t,
		fatalCalled,
		"a rollback whose post-commit reload succeeded must not invoke "+
			"FatalErrorFunc",
	)
}

func TestRollbackWithResyncFailsFastWhenDurableFloorReadFails(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	floorErr := errors.New("injected durable tip floor failure")
	faults := fxFaultedLedger(t, ls, SetFaultsForTest{})
	faults.AfterRollback = func() { faults.LatestBlockNonce = floorErr }

	var fatalErrs []error
	ls.config.FatalErrorFunc = func(err error) {
		fatalErrs = append(fatalErrs, err)
	}

	rbErr := ls.rollbackWithBlocks(
		context.Background(),
		fixture.ancestorTip.Point,
		nil,
		false,
	)

	var committedErr *rollbackCommittedError
	require.ErrorAs(t, rbErr, &committedErr)
	require.ErrorIs(t, rbErr, floorErr)
	require.Len(
		t,
		fatalErrs,
		1,
		"a durable-floor read failure after commit must invoke FatalErrorFunc",
	)
	require.ErrorIs(t, fatalErrs[0], floorErr)
	require.Equal(t, fixture.ancestorTip.Point, ls.currentTip.Point)
}

// TestRollbackWithResyncFailsFastOnEachPostCommitReloadFailure covers every
// post-commit reload failure rollbackWithBlocks reports, one injection each,
// including a failure the durable tip-floor check runs into as well.
func TestRollbackWithResyncFailsFastOnEachPostCommitReloadFailure(
	t *testing.T,
) {
	t.Parallel()

	shelleyEpoch := models.Epoch{
		EpochId:       0,
		EraId:         eras.ShelleyEraDesc.Id,
		SlotLength:    1000,
		LengthInSlots: 100,
	}
	allegraEpoch := models.Epoch{
		EpochId:       1,
		EraId:         eras.AllegraEraDesc.Id,
		StartSlot:     100,
		SlotLength:    1000,
		LengthInSlots: 100,
	}
	errReload := errors.New("injected post-commit reload failure")
	errFloor := errors.New("injected durable tip floor failure")

	shelleyEra := eras.ShelleyEraDesc.Id
	epochsAre := func(epochs ...models.Epoch) func([]models.Epoch) []models.Epoch {
		return func([]models.Epoch) []models.Epoch {
			return slices.Clone(epochs)
		}
	}
	tests := []struct {
		name     string
		faults   SetFaultsForTest
		wantIs   []error
		wantText string
	}{
		{
			name: "unknown era ID",
			faults: SetFaultsForTest{
				TamperEpochs: epochsAre(models.Epoch{EraId: 250}),
			},
			wantText: "unknown era ID 250 after rollback",
		},
		{
			name: "current era protocol parameters",
			faults: SetFaultsForTest{
				TamperEpochs: epochsAre(shelleyEpoch),
				PParams:      errReload,
				PParamsEra:   &shelleyEra,
			},
			wantIs: []error{errReload},
		},
		{
			name: "previous era protocol parameters",
			faults: SetFaultsForTest{
				TamperEpochs: epochsAre(shelleyEpoch, allegraEpoch),
				PParams:      errReload,
				PParamsEra:   &shelleyEra,
			},
			wantIs: []error{errReload},
		},
		{
			name: "synthetic PlutusV2 cost model marker",
			faults: SetFaultsForTest{
				SyncMarker: map[string]error{
					database.SyntheticV2CostModelSyncKey: errReload,
				},
			},
			wantIs: []error{errReload},
		},
		{
			name: "epochs and durable tip floor",
			faults: SetFaultsForTest{
				Epochs:           errReload,
				LatestBlockNonce: errFloor,
			},
			wantIs: []error{errReload, errFloor},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			// The reads fail only once the rollback has committed.
			faults := fxFaultedLedger(t, ls, SetFaultsForTest{})
			faults.AfterRollback = func() {
				armed := tc.faults
				armed.AfterRollback = faults.AfterRollback
				*faults = armed
			}

			var fatalErrs []error
			ls.config.FatalErrorFunc = func(err error) {
				fatalErrs = append(fatalErrs, err)
			}

			rbErr := ls.rollbackWithBlocksAndIntent(
				context.Background(),
				fixture.ancestorTip.Point,
				nil,
				false,
				false,
				false,
			)

			var committedErr *rollbackCommittedError
			require.ErrorAs(t, rbErr, &committedErr)
			require.Len(
				t,
				fatalErrs,
				1,
				"a post-commit reload failure must invoke FatalErrorFunc "+
					"exactly once",
			)
			for _, want := range tc.wantIs {
				require.ErrorIs(t, rbErr, want)
				require.ErrorIs(t, fatalErrs[0], want)
			}
			if tc.wantText != "" {
				require.ErrorContains(t, rbErr, tc.wantText)
				require.ErrorContains(t, fatalErrs[0], tc.wantText)
			}
			require.Equal(t, fixture.ancestorTip.Point, ls.currentTip.Point)
		})
	}
}

// A rollback that deletes an epoch row must also rewind the ledger epoch
// label. Otherwise the first block re-applied in the deleted epoch is seen as
// a non-successor signal, and the epoch's boundary effects never run.
func TestRollbackRewindsLedgerEpochLabel(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	for _, epochID := range []uint64{0, 1} {
		require.NoError(t, seedDB(t, ls.db).SetEpoch(
			epochID*15, epochID, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1000, 15, nil,
		))
	}
	epochs, err := seedDB(t, ls.db).GetEpochs(nil)
	require.NoError(t, err)
	require.Len(t, epochs, 2)
	ls.epochCache = epochs
	ls.currentEpoch = epochs[1]
	ls.currentEra = eras.ShelleyEraDesc
	ls.ledgerEpoch = 1

	require.NoError(t, ls.rollbackWithBlocksAndIntent(
		context.Background(),
		fixture.ancestorTip.Point,
		nil,
		false,
		false,
		false,
	))

	require.Equal(t, uint64(0), ls.currentEpoch.EpochId)
	require.Equal(t, uint64(0), ls.ledgerEpoch,
		"the label must follow the epoch cache so epoch 1 re-runs its boundary effects")
}

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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

// errRollbackEpochReloadInjected is the sentinel error the test below injects
// into GetEpochs, so assertions can distinguish it from any other error the
// rollback path might otherwise raise.
var errRollbackEpochReloadInjected = errors.New(
	"injected GetEpochs failure after rollback commit",
)

// getEpochsFailingMetadataStore fails GetEpochs while leaving every other
// metadata operation -- including the rollback truncation itself -- working,
// so the injected failure lands exactly where rollbackWithResync reloads
// epochCache/currentEra/currentPParams, after the metadata transaction that
// performs the truncation has already committed.
type getEpochsFailingMetadataStore struct {
	metadata.MetadataStore
	err error
}

func (s getEpochsFailingMetadataStore) GetEpochs(
	_ dbtypes.Txn,
) ([]models.Epoch, error) {
	return nil, s.err
}

// TestRollbackWithResyncFailsFastWhenEpochReloadFailsAfterCommit pins case R3
// of blinklabs-io/dingo#1649: rollbackWithResync's post-commit reload of
// epochCache/currentEra/currentPParams can fail after the metadata truncation
// has already committed. Today that failure is logged at Warn and the
// rollback still reports success (nil), leaving those in-memory caches at
// their pre-rollback values even though the database itself was truncated --
// a later block then validates against stale era/epoch/protocol-parameter
// state. After the fix this must surface as a rollbackCommittedError and
// invoke FatalErrorFunc, so a supervised restart reloads the caches from the
// database before any further block is validated.
func TestRollbackWithResyncFailsFastWhenEpochReloadFailsAfterCommit(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	base := ls.db
	failing, err := database.New(
		base.Config(),
		database.Stores{
			Blob: base.Blob(),
			Metadata: getEpochsFailingMetadataStore{
				MetadataStore: base.Metadata(),
				err:           errRollbackEpochReloadInjected,
			},
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, failing.Close()) })
	ls.db = failing

	fatalCalled := false
	var fatalErr error
	ls.config.FatalErrorFunc = func(err error) {
		fatalCalled = true
		fatalErr = err
	}

	rbErr := ls.rollbackWithoutResync(fixture.ancestorTip.Point)

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

	rbErr := ls.rollbackWithoutResync(fixture.ancestorTip.Point)

	require.NoError(t, rbErr)
	require.False(
		t,
		fatalCalled,
		"a rollback whose post-commit reload succeeded must not invoke "+
			"FatalErrorFunc",
	)
}

// epochsOverrideMetadataStore returns a fixed epoch list from the
// transaction-less GetEpochs read rollbackWithResync performs after its
// metadata transaction commits, so a test can steer that reload into a
// specific era without writing epoch rows the truncation would delete.
type epochsOverrideMetadataStore struct {
	metadata.MetadataStore
	epochs []models.Epoch
}

func (s epochsOverrideMetadataStore) GetEpochs(
	txn dbtypes.Txn,
) ([]models.Epoch, error) {
	if txn != nil {
		return s.MetadataStore.GetEpochs(txn)
	}
	return append([]models.Epoch(nil), s.epochs...), nil
}

// pparamsReadFailingMetadataStore fails transaction-less protocol-parameter
// reads for one era, which only the post-commit computePParams reload
// performs.
type pparamsReadFailingMetadataStore struct {
	metadata.MetadataStore
	eraId uint
	err   error
}

func (s pparamsReadFailingMetadataStore) GetPParams(
	epoch uint64,
	eraId uint,
	txn dbtypes.Txn,
) ([]models.PParams, error) {
	if txn == nil && eraId == s.eraId {
		return nil, s.err
	}
	return s.MetadataStore.GetPParams(epoch, eraId, txn)
}

// syncStateReadFailingMetadataStore fails the transaction-less read of one
// sync-state key, leaving the in-transaction marker recompute working.
type syncStateReadFailingMetadataStore struct {
	metadata.MetadataStore
	key string
	err error
}

func (s syncStateReadFailingMetadataStore) GetSyncState(
	key string,
	txn dbtypes.Txn,
) (string, error) {
	if txn == nil && key == s.key {
		return "", s.err
	}
	return s.MetadataStore.GetSyncState(key, txn)
}

// TestRollbackWithResyncFailsFastOnEachPostCommitReloadFailure covers every
// post-commit reload failure rollbackWithResync reports, one injection each,
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

	tests := []struct {
		name     string
		wrap     func(metadata.MetadataStore) metadata.MetadataStore
		wantIs   []error
		wantText string
	}{
		{
			name: "unknown era ID",
			wrap: func(m metadata.MetadataStore) metadata.MetadataStore {
				return epochsOverrideMetadataStore{
					MetadataStore: m,
					epochs:        []models.Epoch{{EraId: 250}},
				}
			},
			wantText: "unknown era ID 250 after rollback",
		},
		{
			name: "current era protocol parameters",
			wrap: func(m metadata.MetadataStore) metadata.MetadataStore {
				return pparamsReadFailingMetadataStore{
					MetadataStore: epochsOverrideMetadataStore{
						MetadataStore: m,
						epochs:        []models.Epoch{shelleyEpoch},
					},
					eraId: eras.ShelleyEraDesc.Id,
					err:   errReload,
				}
			},
			wantIs: []error{errReload},
		},
		{
			name: "previous era protocol parameters",
			wrap: func(m metadata.MetadataStore) metadata.MetadataStore {
				return pparamsReadFailingMetadataStore{
					MetadataStore: epochsOverrideMetadataStore{
						MetadataStore: m,
						epochs: []models.Epoch{
							shelleyEpoch,
							allegraEpoch,
						},
					},
					eraId: eras.ShelleyEraDesc.Id,
					err:   errReload,
				}
			},
			wantIs: []error{errReload},
		},
		{
			name: "synthetic PlutusV2 cost model marker",
			wrap: func(m metadata.MetadataStore) metadata.MetadataStore {
				return syncStateReadFailingMetadataStore{
					MetadataStore: m,
					key:           database.SyntheticV2CostModelSyncKey,
					err:           errReload,
				}
			},
			wantIs: []error{errReload},
		},
		{
			name: "epochs and durable tip floor",
			wrap: func(m metadata.MetadataStore) metadata.MetadataStore {
				return floorLookupFailingMetadataStore{
					MetadataStore: getEpochsFailingMetadataStore{
						MetadataStore: m,
						err:           errReload,
					},
					err: errFloor,
				}
			},
			wantIs: []error{errReload, errFloor},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			base := ls.db
			wrapped, err := database.New(
				base.Config(),
				database.Stores{
					Blob:     base.Blob(),
					Metadata: tc.wrap(base.Metadata()),
				},
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wrapped.Close()) })
			ls.db = wrapped

			var fatalErrs []error
			ls.config.FatalErrorFunc = func(err error) {
				fatalErrs = append(fatalErrs, err)
			}

			rbErr := ls.rollbackWithoutResync(fixture.ancestorTip.Point)

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

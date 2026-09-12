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

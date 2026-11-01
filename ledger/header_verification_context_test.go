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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func TestHeaderVerificationContextTransient(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	_, err := ls.withStableHeaderVerificationContext(func() error {
		t.Fatal("an unpublished context must not be verified")
		return nil
	})
	require.True(t, IsHeaderVerificationContextTransient(err), "an unpublished context must be transient")
	require.ErrorIs(t, err, errHeaderVerificationDeferred)

	ls.publishSnapshotsLocked()
	finish := ls.beginTxValidationTransition()
	_, err = ls.withStableHeaderVerificationContext(func() error {
		t.Fatal("an active transition must not be verified")
		return nil
	})
	finish()
	require.True(t, IsHeaderVerificationContextTransient(err), "an active validation transition must be transient")
	require.ErrorIs(t, err, errHeaderVerificationDeferred)

	_, err = ls.withStableHeaderVerificationContext(func() error {
		ls.publishSnapshotsLocked()
		return nil
	})
	require.True(t, IsHeaderVerificationContextTransient(err), "a concurrent publication must be transient")
	require.ErrorIs(t, err, errHeaderVerificationDeferred)
	require.True(t, IsHeaderVerificationContextTransient(fmt.Errorf("wrapped: %w", err)))

	generation, err := ls.withStableHeaderVerificationContext(func() error { return nil })
	require.NoError(t, err)
	require.NotZero(t, generation)
	for _, cause := range []error{
		nil, errHeaderVerificationDeferred, errEpochNonceUnavailable,
		ErrHeaderBeyondForecastHorizon, errors.New("invalid VRF proof"),
	} {
		require.False(t, IsHeaderVerificationContextTransient(cause), "%v", cause)
	}
}

func TestPeerNonceLookupClassifiesChangedContext(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 2)
	f.stamp(0)
	snapshot := f.ls.loadConsensusSnapshot()
	f.ls.publishSnapshotsLocked()
	_, _, _, err := f.ls.peerChainSelectionEpochNonce(
		ouroboros.ConnectionId{}, f.headers[1], snapshot,
	)
	require.ErrorIs(t, err, errHeaderVerificationDeferred)
	require.True(t, IsHeaderVerificationContextTransient(err),
		"a context replaced before ancestry lookup must retry even a local extension")
}

func TestPeerForkPathClassifiesContextChangedDuringLookup(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 1)
	var finish func()
	f.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		if finish == nil {
			finish = f.ls.beginTxValidationTransition()
			t.Cleanup(finish)
		}
		return database.BlockByHash(context.Background(), f.ls.db, hash)
	}
	_, _, found, err := f.ls.peerHeaderForkPath(ouroboros.ConnectionId{}, f.headers[0])
	require.NotNil(t, finish, "the actual local-anchor lookup must run")
	require.False(t, found)
	require.ErrorIs(t, err, errHeaderVerificationDeferred)
	require.True(t, IsHeaderVerificationContextTransient(err),
		"a context change during local-anchor lookup must retry the header")

	finish()
	_, path, found, err := f.ls.peerHeaderForkPath(ouroboros.ConnectionId{}, f.headers[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, path, 1)
}

func TestHeaderVerificationEpochClassifiesPersistentSnapshotChurn(t *testing.T) {
	t.Parallel()

	newState := func() *LedgerState {
		ls := &LedgerState{
			epochCache: []models.Epoch{{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1_000,
				LengthInSlots: 100,
				Nonce:         make([]byte, 32),
			}},
		}
		ls.publishSnapshotsLocked()
		return ls
	}

	ls := newState()
	lookups := 0
	ls.headerVerificationEpochLookupHook = func() {
		lookups++
		ls.publishSnapshotsLocked()
	}
	_, cache, err := ls.headerVerificationEpochWithCache(
		context.Background(), 10, false,
	)
	require.Equal(t, 3, lookups, "every retry must resolve the epoch")
	require.Nil(t, cache)
	require.ErrorIs(t, err, errHeaderVerificationDeferred)
	require.True(t, IsHeaderVerificationContextTransient(err),
		"a snapshot replaced during every lookup must retry the header")
	require.False(t, headerFailureBlamesPeer(err))

	// Each publication carries a distinct nonce so the result identifies the
	// lookup that produced it: only the third attempt sees a stable snapshot.
	ls = newState()
	lookups = 0
	ls.headerVerificationEpochLookupHook = func() {
		lookups++
		if lookups < 3 {
			nonce := make([]byte, 32)
			nonce[0] = byte(lookups)
			ls.epochCache = []models.Epoch{{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1_000,
				LengthInSlots: 100,
				Nonce:         nonce,
			}}
			ls.publishSnapshotsLocked()
		}
	}
	epoch, cache, err := ls.headerVerificationEpochWithCache(
		context.Background(), 10, false,
	)
	require.NoError(t, err)
	require.Equal(t, 3, lookups)
	require.Equal(t, uint64(0), epoch.EpochId)
	require.Equal(t, byte(2), epoch.Nonce[0],
		"the epoch must come from the lookup that saw a stable snapshot")
	published := ls.loadConsensusSnapshot().epochCache
	require.Len(t, cache, 1)
	require.Same(t, &published[0], &cache[0],
		"the cache must be the snapshot the epoch was resolved from")
	require.Equal(t, epoch.Nonce, cache[0].Nonce)
}

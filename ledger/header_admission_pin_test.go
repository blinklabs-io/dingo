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
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// A verdict that a publication expires between verification and pin is
// verified again and pinned, instead of the header being withheld.
func TestPinHeaderVerdictReverifiesExpiredVerdict(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	stale := f.ls.CurrentHeaderCryptoVerificationGeneration()
	require.NotZero(t, stale)
	f.ls.publishSnapshotsLocked()

	calls := 0
	verify := func() (uint64, error) {
		calls++
		generation := f.ls.CurrentHeaderCryptoVerificationGeneration()
		if calls == 1 {
			f.ls.publishSnapshotsLocked()
		}
		return generation, nil
	}
	generation, release, err := f.ls.pinHeaderVerdict(stale, verify)
	require.NoError(t, err)
	require.NotNil(t, release, "an expired verdict must be verified again")
	defer release()
	require.Equal(t, 2, calls)
	require.Equal(t, f.ls.CurrentHeaderCryptoVerificationGeneration(), generation)
	require.NotEqual(t, stale, generation)
}

// Re-verification is bounded: a verdict that expires on every attempt is
// withheld and never pinned.
func TestPinHeaderVerdictBoundsReverification(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	stale := f.ls.CurrentHeaderCryptoVerificationGeneration()
	f.ls.publishSnapshotsLocked()

	calls := 0
	_, release, err := f.ls.pinHeaderVerdict(stale, func() (uint64, error) {
		calls++
		generation := f.ls.CurrentHeaderCryptoVerificationGeneration()
		f.ls.publishSnapshotsLocked()
		return generation, nil
	})
	require.NoError(t, err)
	require.Nil(t, release)
	require.Equal(t, maxHeaderAdmissionVerifyAttempts, calls)
}

// A header that fails verification again is never pinned, and the failure
// reaches the caller.
func TestPinHeaderVerdictReturnsVerificationFailure(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	stale := f.ls.CurrentHeaderCryptoVerificationGeneration()
	f.ls.publishSnapshotsLocked()

	invalid := errors.New("invalid header")
	_, release, err := f.ls.pinHeaderVerdict(stale, func() (uint64, error) {
		return 0, invalid
	})
	require.ErrorIs(t, err, invalid)
	require.Nil(t, release)

	_, release, err = f.ls.pinHeaderVerdict(stale, nil)
	require.NoError(t, err)
	require.Nil(t, release, "no fresh verdict can replace an expired one")
}

// A header whose expired verdict fails verification again is rejected the way
// a first verification failure is: the error reaches the handler and its
// connection is recycled. A deferral on the second verification only withholds
// the header.
func TestPinChainsyncHeaderVerdictRejectsVerificationFailure(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	f.ls.config.EventBus = bus
	stale := f.ls.CurrentHeaderCryptoVerificationGeneration()
	f.ls.publishSnapshotsLocked()
	e := ChainsyncEvent{ConnectionId: testRecycleConnId()}

	invalid := errors.New("invalid header")
	pending := &pendingPublishes{}
	_, release, err := f.ls.pinChainsyncHeaderVerdict(
		e, pending, stale, func() (uint64, error) { return 0, invalid }, true,
	)
	require.ErrorIs(t, err, invalid)
	require.Nil(t, release)
	require.Len(t, pending.events, 1, "the connection must be recycled")
	recycle, ok := pending.events[0].evt.Data.(ConnectionRecycleRequestedEvent)
	require.True(t, ok)
	require.Equal(t, e.ConnectionId, recycle.ConnectionId)
	require.Equal(t, "header_verification_failure", recycle.Reason)

	pending = &pendingPublishes{}
	_, release, err = f.ls.pinChainsyncHeaderVerdict(
		e, pending, stale, func() (uint64, error) {
			return 0, errHeaderVerificationContextChanged
		}, true,
	)
	require.NoError(t, err, "a deferred verification withholds the header")
	require.Nil(t, release)
	require.Empty(t, pending.events)
}

// Withholding a header leaves a hole in its connection's headers: the next
// one does not fit, the header queue is discarded and the peer re-intersected.
// A header that queue admission does not have to authorize is queued without
// its verdict when a publication expires it between the currency check and
// the pin, as one whose verdict had already expired is. One that must be
// authorized is still withheld.
func TestPinChainsyncHeaderVerdictDropsOptionalExpiredVerdict(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	f.ls.config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	stale := f.ls.CurrentHeaderCryptoVerificationGeneration()
	require.NotZero(t, stale)
	f.ls.publishSnapshotsLocked()
	e := ChainsyncEvent{ConnectionId: testRecycleConnId()}

	pending := &pendingPublishes{}
	generation, release, err := f.ls.pinChainsyncHeaderVerdict(
		e, pending, stale, nil, false,
	)
	require.NoError(t, err)
	require.Nil(t, release)
	require.Zero(t, generation,
		"an optional verdict that expired must be dropped, not the header")
	require.Empty(t, pending.events)

	generation, release, err = f.ls.pinChainsyncHeaderVerdict(
		e, pending, stale, nil, true,
	)
	require.NoError(t, err)
	require.Nil(t, release)
	require.Equal(t, stale, generation,
		"a header that must be authorized is withheld")
}

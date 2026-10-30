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
	"fmt"
	"testing"

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

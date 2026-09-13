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

package ouroboros

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// TestLocalstatequeryServerAcquire_PointAheadOfTip_GracefulFailure is the
// blinklabs-io/dingo#4156 regression. Before this fix, localstatequeryServerAcquire
// unconditionally accepted any AcquireSpecificPoint, deferring the actual
// tip-validity check to the first Query -- and a rejection surfacing there
// has no graceful wire-level reply (unlike a rejection at Acquire time), so
// it propagated as a fatal protocol error and gouroboros tore the whole
// LocalStateQuery connection down instead of just failing one Acquire.
//
// Acquiring a point ahead of this node's own current tip is not a client
// bug: it is the ordinary result of two independently-syncing nodes (this
// node and whatever reference the caller derived the point from) not
// advancing in perfect lockstep, which happens on close to every block at
// real cadence. It must fail gracefully -- an error errors.Is-matching
// gouroboros' own olocalstatequery.ErrAcquireFailurePointNotOnChain, which
// its server (handleAcquire/handleReAcquire) translates into a wire-level
// AcquireFailure reply -- not with an arbitrary error gouroboros has no
// graceful handling for.
func TestLocalstatequeryServerAcquire_PointAheadOfTip_GracefulFailure(t *testing.T) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
	}
	ls, db := newTestLedgerStateWithChain(t, 2)
	o.ledgerState = ls

	tipHash := bytes.Repeat([]byte{1}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))

	aheadHash := bytes.Repeat([]byte{2}, 32)
	connID := ouroboros.ConnectionId{}
	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireSpecificPoint{
			Point: ocommon.NewPoint(2, aheadHash),
		},
		false,
	)
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, olocalstatequery.ErrAcquireFailurePointNotOnChain),
		"expected a gracefully-mapped AcquireFailurePointNotOnChain, got: %v",
		err,
	)

	o.localstatequeryAcquireMutex.Lock()
	_, recorded := o.localstatequeryAcquiredPoints[connID]
	o.localstatequeryAcquireMutex.Unlock()
	require.False(
		t, recorded,
		"a rejected Acquire must not record the unvalidated point",
	)
}

// TestLocalstatequeryServerAcquire_PointOnChain_Succeeds is the companion
// positive case: a specific point genuinely matching this node's chain at
// that slot must still be accepted and recorded, unchanged from before the
// #4156 fix.
func TestLocalstatequeryServerAcquire_PointOnChain_Succeeds(t *testing.T) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
	}
	ls, db := newTestLedgerStateWithChain(t, 2)
	o.ledgerState = ls

	tipHash := bytes.Repeat([]byte{2}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(2, tipHash),
	}, nil))

	connID := ouroboros.ConnectionId{}
	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireSpecificPoint{
			Point: ocommon.NewPoint(2, tipHash),
		},
		false,
	)
	require.NoError(t, err)

	o.localstatequeryAcquireMutex.Lock()
	recorded, ok := o.localstatequeryAcquiredPoints[connID]
	o.localstatequeryAcquireMutex.Unlock()
	require.True(t, ok)
	require.Equal(t, uint64(2), recorded.Slot)
}

// TestLocalstatequeryServerAcquire_VolatileTip_ClearsPoint covers the
// AcquireVolatileTip branch, unchanged by the #4156 fix: it must clear any
// previously-recorded pinned point rather than being validated as a
// specific point.
func TestLocalstatequeryServerAcquire_VolatileTip_ClearsPoint(t *testing.T) {
	o := &Ouroboros{
		localstatequeryAcquiredPoints: make(
			map[ouroboros.ConnectionId]ledger.QueryPoint,
		),
	}
	ls, _ := newTestLedgerStateWithChain(t, 1)
	o.ledgerState = ls

	connID := ouroboros.ConnectionId{}
	o.localstatequeryAcquiredPoints[connID] = ledger.QueryPoint{Slot: 1}

	err := o.localstatequeryServerAcquire(
		olocalstatequery.CallbackContext{ConnectionId: connID},
		olocalstatequery.AcquireVolatileTip{},
		false,
	)
	require.NoError(t, err)

	o.localstatequeryAcquireMutex.Lock()
	_, recorded := o.localstatequeryAcquiredPoints[connID]
	o.localstatequeryAcquireMutex.Unlock()
	require.False(t, recorded)
}

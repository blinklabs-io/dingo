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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueryHardFork_CurrentEra_PinnedPointResolvesEraAtThatPoint is the
// regression test for the gap this session's node-parity --from-genesis
// live validation surfaced (blinklabs-io/dingo#1900): HardForkCurrentEraQuery
// used to always answer with dingo's live era regardless of the pinned
// point, a real point-pinning gap #382's original scope decision left open
// (it covered queryShelleyCurrentProtocolParams and friends, not this
// HardFork-mini-protocol query type). gouroboros's client-side
// GetCurrentProtocolParams queries this first specifically to decide which
// era-shaped struct to decode the *next* query's reply into, so answering
// with the wrong era here breaks decoding a perfectly correct, already
// point-aware queryShelleyCurrentProtocolParams reply for any pinned point
// whose real era differs from dingo's live one -- confirmed live pinning at
// genesis (slot 0, Shelley) against a dingo instance already in a later
// era. Epoch 3 (Shelley) and epoch 6 (Conway, live) are seeded with
// deliberately different eras so a handler that silently fell back to the
// live era would be caught.
func TestQueryHardFork_CurrentEra_PinnedPointResolvesEraAtThatPoint(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, eras.ConwayEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	pinned, err := ls.queryHardFork(
		&olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkCurrentEraQuery{},
		},
		QueryPoint{Slot: 350},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		eras.ShelleyEraDesc.Id,
		pinned,
		"a point pinned in epoch 3 (Shelley) must resolve Shelley, not the live epoch 6 (Conway) era",
	)

	live, err := ls.queryHardFork(
		&olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkCurrentEraQuery{},
		},
		QueryPoint{},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, eras.ConwayEraDesc.Id, live)
}

// TestQueryHardFork_CurrentEra_NoEpochRecordRejected covers a pinned point
// whose epoch has no epoch record at all: the era genuinely cannot be
// resolved, and this must fail with ErrHistoricalStateUnavailable rather
// than silently falling back to the live era (the exact bug this handler
// otherwise reproduces every time) or panicking on a nil era descriptor.
//
// Deliberately seeds an epoch-0 row too (human review, Chris Guiney,
// dingo#4320): resolveAsOfEpoch previously fell back to epoch 0, with no
// way to distinguish "genuinely epoch 0" from "no covering row found," when
// GetEpochBySlot found nothing for the pinned slot. On any genesis-synced
// node an epoch-0 row always exists, so GetEpoch(0) would then succeed and
// silently answer Byron for a point actually in a later era -- this test's
// previous fixture omitted the epoch-0 row entirely, so it passed for the
// wrong reason (both the fallback epoch and the real target epoch were
// missing) without ever exercising that silent-wrong-answer path. Slot 50
// here resolves to neither epoch 0 (slots 0-9) nor epoch 6 (starts at slot
// 600) -- a genuine gap, not the chain's start -- so a correct fix must
// still reject it even with epoch 0 present.
func TestQueryHardFork_CurrentEra_NoEpochRecordRejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	require.NoError(t, ls.db.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.ByronEraDesc.Id, 1, 10, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, eras.ConwayEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	_, err := ls.queryHardFork(
		&olocalstatequery.HardForkQuery{
			Query: &olocalstatequery.HardForkCurrentEraQuery{},
		},
		QueryPoint{Slot: 50},
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

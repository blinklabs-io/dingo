// Copyright 2025 Blink Labs Software
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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newImmutableChainsyncRollbackFixture(
	t *testing.T,
) *chainsyncRollbackFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 21600,
		},
	})
	require.NoError(t, err)
	return newChainsyncRollbackFixtureWithDB(t, db)
}

// A peer rollback below the immutable tip must resync the connection and keep
// the node running, whether the target is deeper than K or still within K of
// the rollback anchor (as after the anchor is reset to the tip).
func TestChainsyncRollbackBelowImmutableTipResyncs(t *testing.T) {
	t.Parallel()
	for name, overK := range map[string]bool{
		"within K": false,
		"beyond K": true,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newImmutableChainsyncRollbackFixture(t)
			if overK {
				putPrimaryChainOnForkBeyondK(t, fixture, "immutable-floor")
			}
			_, err := fixture.ls.db.PromoteThrough(context.Background(), 1)
			require.NoError(t, err)

			bus := newTestEventBus(t, nil)
			fixture.ls.config.EventBus = bus
			resyncCh := subscribeChainsyncResync(t, bus)
			var fatal error
			fixture.ls.config.FatalErrorFunc = func(err error) {
				fatal = err
			}

			var pending pendingPublishes
			fixture.ls.processChainsyncRollback(
				ChainsyncEvent{
					ConnectionId: fixture.connId,
					Point:        ocommon.Point{},
				},
				&pending,
			)
			pending.flush()

			require.NoError(t, fatal, "a peer rollback must not be fatal")
			e := testutil.RequireReceive(
				t,
				resyncCh,
				testutil.AsyncWait,
				"expected a resync for a rollback below the immutable tip",
			)
			require.Equal(t, fixture.connId, e.ConnectionId)
			if overK {
				require.Equal(
					t, event.ChainsyncResyncReasonRollbackExceedsK, e.Reason,
				)
			}
		})
	}
}

// A windowed recovery descent must refuse a target below the immutable tip
// before committing any step: it cannot reach the target, and each committed
// step would shorten the chain for nothing.
func TestWindowedRewindRefusesTargetBelowImmutableTip(t *testing.T) {
	t.Parallel()
	fixture := newImmutableChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "immutable-windowed")
	_, err := fixture.ls.db.PromoteThrough(context.Background(), 2)
	require.NoError(t, err)
	tipBefore := fixture.ls.chain.Tip()

	committed, err := fixture.ls.rollbackPrimaryChainInSecurityParamWindows(
		fixture.ancestorTip.Point,
	)

	require.ErrorIs(t, err, database.ErrRollbackBelowImmutableTip)
	require.False(t, committed)
	require.Equal(t, tipBefore, fixture.ls.chain.Tip())
}

// The promoter keeps the immutable tip at least K below the applied tip, so a
// recovery target below the floor is normally deeper than K and the chain
// reports both refusals. The descent must still refuse it up front: its first
// windows sit above the floor and would commit before the floor stops it.
func TestWindowedRewindRefusesTargetBelowDeepImmutableTip(t *testing.T) {
	t.Parallel()
	const securityParam = 2
	fixture := newImmutableChainsyncRollbackFixture(t)
	fixture.ls.config.CardanoNodeConfig = newTestShelleyGenesisCfgWithK(
		t, securityParam,
	)
	fixture.ls.currentEra = eras.ShelleyEraDesc
	require.Equal(t, securityParam, fixture.ls.SecurityParam())
	require.NoError(
		t,
		fixture.ls.chain.Rollback(
			context.Background(),
			fixture.ancestorTip.Point,
		),
	)
	prevHash := fixture.ancestorTip.Point.Hash
	blocks := make([]chain.RawBlock, 0, 8)
	for idx := range 8 {
		offset := uint64(idx + 1)
		hash := testHashBytes(fmt.Sprintf("immutable-deep-fork-%d", idx))
		blocks = append(blocks, chain.RawBlock{
			Slot:        fixture.currentTip.Point.Slot + offset*5,
			Hash:        hash,
			BlockNumber: fixture.ancestorTip.BlockNumber + offset,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        chainsyncFixtureBlockCbor,
		})
		prevHash = hash
	}
	require.NoError(
		t,
		fixture.ls.chain.AddRawBlocks(context.Background(), blocks),
	)
	// Tip index 9, immutable tip index 4: more than K below the tip.
	_, err := fixture.ls.db.PromoteThrough(context.Background(), 4)
	require.NoError(t, err)
	tipBefore := fixture.ls.chain.Tip()

	committed, err := fixture.ls.rollbackPrimaryChainInSecurityParamWindows(
		fixture.ancestorTip.Point,
	)

	require.ErrorIs(t, err, database.ErrRollbackBelowImmutableTip)
	require.False(t, committed)
	require.Equal(t, tipBefore, fixture.ls.chain.Tip())
}

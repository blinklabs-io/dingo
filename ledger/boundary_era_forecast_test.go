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
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// TestProtocolParamsForSlotForecastsVersionTriggeredEra pins that the forger
// builds the first block of a version-triggered era in that era: the ticked
// ledger state has already crossed the boundary, so a block encoded in the
// ending era would be refused by the boundary check.
func TestProtocolParamsForSlotForecastsVersionTriggeredEra(t *testing.T) {
	t.Parallel()

	// The epoch is 100 slots and the stability window 30, so the voting
	// deadline is slot 40 and k=1 block must lie at or past it.
	const firstSlotOfEpoch1 = 100
	newLedger := func(
		t *testing.T,
		tipIndex int,
		bump bool,
	) (*LedgerState, []ochainsync.Tip) {
		t.Helper()
		cfg := newBabbageQuorum1Cfg(t)
		genesis, err := os.Open("../config/cardano/preview/conway-genesis.json")
		require.NoError(t, err)
		t.Cleanup(func() { _ = genesis.Close() })
		require.NoError(t, cfg.LoadConwayGenesisFromReader(genesis))
		db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
		require.NoError(t, err)
		ls := newQuorumMetVersionBumpLedger(t, cfg, db)
		if !bump {
			require.NoError(t, db.DeletePParamUpdatesAfterSlot(t.Context(), 0, nil))
		}
		testChain, tips := newVersionBumpChain(t, db, 30, 15, 2)
		ls.chain = testChain
		ls.currentTip = tips[tipIndex]
		ls.publishSnapshotsLocked()
		return ls, tips
	}

	t.Run("conway pparams once k blocks follow the voting deadline", func(t *testing.T) {
		t.Parallel()
		ls, _ := newLedger(t, 1, true)
		got := ls.ProtocolParamsForSlot(firstSlotOfEpoch1)
		pparams, ok := got.(*conway.ConwayProtocolParameters)
		require.Truef(t, ok, "forged era must be Conway, got %T", got)
		require.Equal(t, uint(9), pparams.ProtocolVersion.Major)
	})
	t.Run("babbage pparams before k blocks follow the deadline", func(t *testing.T) {
		t.Parallel()
		ls, _ := newLedger(t, 0, true)
		got := ls.ProtocolParamsForSlot(firstSlotOfEpoch1)
		pparams, ok := got.(*babbage.BabbageProtocolParameters)
		require.Truef(t, ok, "forged era must stay Babbage, got %T", got)
		require.Equal(t, uint(9), pparams.ProtocolMajor)
	})
	t.Run("babbage pparams without a pending bump", func(t *testing.T) {
		t.Parallel()
		ls, _ := newLedger(t, 1, false)
		got := ls.ProtocolParamsForSlot(firstSlotOfEpoch1)
		pparams, ok := got.(*babbage.BabbageProtocolParameters)
		require.Truef(t, ok, "forged era must stay Babbage, got %T", got)
		require.Equal(t, uint(8), pparams.ProtocolMajor)
	})
	t.Run("waits out an interrupted publication", func(t *testing.T) {
		t.Parallel()
		// The published state counts no block past the deadline, so its
		// forecast is Babbage. A tip from another generation that does count
		// one, as an older higher tip does across a rollback, would select
		// Conway if the forecast paired it with this state.
		ls, tips := newLedger(t, 0, true)
		matching := ls.loadTipSnapshot()
		ls.tip.Store(&tipSnapshot{
			generation: matching.generation - 1,
			currentTip: tips[1],
		})
		result := make(chan lcommon.ProtocolParameters, 1)
		go func() {
			result <- ls.ProtocolParamsForSlot(firstSlotOfEpoch1)
		}()
		testutil.RequireNoReceive(t, result, 200*time.Millisecond,
			"forecast returned while the publication was interrupted")
		ls.tip.Store(matching)
		got := testutil.RequireReceive(t, result, 5*time.Second,
			"forecast after the publication completed")
		pparams, ok := got.(*babbage.BabbageProtocolParameters)
		require.Truef(t, ok, "forged era must stay Babbage, got %T", got)
		require.Equal(t, uint(9), pparams.ProtocolMajor)
	})
}

// TestForgedBoundaryEraIsAccepted drives the forger's era choice into the
// boundary check: whichever era ProtocolParamsForSlot picks for the first slot
// of the new epoch, a block encoded in it must be accepted, and the other era
// refused. The two decisions share one predicate, so they cannot disagree.
func TestForgedBoundaryEraIsAccepted(t *testing.T) {
	t.Parallel()

	k := uint64(boundaryEraTestSecurityParam)
	cases := []struct {
		name       string
		tc         boundaryEraCase
		wantConway bool
	}{
		{
			name:       "k blocks after the deadline",
			tc:         boundaryEraCase{pparamsMajor: 9, votingBlocks: k},
			wantConway: true,
		},
		{
			name:       "fewer than k blocks after the deadline",
			tc:         boundaryEraCase{pparamsMajor: 9, votingBlocks: k - 1},
			wantConway: false,
		},
		{
			name: "quorum-met classic major bump",
			tc: boundaryEraCase{
				pparamsMajor:       8,
				classicUpdateMajor: 9,
				votingBlocks:       k,
			},
			wantConway: true,
		},
		{
			name: "quorum-met classic major bump with fewer than k blocks",
			tc: boundaryEraCase{
				pparamsMajor:       8,
				classicUpdateMajor: 9,
				votingBlocks:       k - 1,
			},
			wantConway: false,
		},
		{
			name:       "no major bump",
			tc:         boundaryEraCase{pparamsMajor: 8, votingBlocks: k},
			wantConway: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, block := newBoundaryEraLedger(t, tc.tc)
			got := ls.ProtocolParamsForSlot(block.SlotNumber())
			_, forgesConway := got.(*conway.ConwayProtocolParameters)
			require.Equalf(t, tc.wantConway, forgesConway,
				"forger picked %T", got)

			forged := tc.tc
			forged.babbageBody = !forgesConway
			ls, err := runBoundaryEraCase(t, forged)
			require.NoError(t, err,
				"a block in the forger's era must pass the boundary check")
			require.Equal(t, uint64(1), ls.currentEpoch.EpochId)

			other := tc.tc
			other.babbageBody = forgesConway
			_, err = runBoundaryEraCase(t, other)
			require.ErrorIs(t, err, errRestartLedgerPipeline,
				"a block in the other era must be refused")
		})
	}
}

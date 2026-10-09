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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/ledgerstate"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMithrilImportedDonationReachesBoundaryTreasuryAndRatify bootstraps from
// a snapshot anchored mid-epoch whose UTxOState carries a 50 ADA donation, then
// runs the real boundary rollover. Conway's EPOCH rule adds that donation to
// the treasury and counts it in the RATIFY seed, so the boundary treasury is
// 150 ADA and a 120 ADA withdrawal is ratified. Dropping the donation at
// import leaves the treasury at 100 ADA and the withdrawal unratified.
func TestMithrilImportedDonationReachesBoundaryTreasuryAndRatify(
	t *testing.T,
) {
	t.Parallel()

	const ada = uint64(1_000_000)
	f := newTreasuryRolloverFixture(t, 100*ada)
	destination, destinationReturn, _ := f.rewardAddress(t, 0x93)
	withdrawal := f.addProposal(
		t, 0x94, f.currentEpoch.StartSlot+20,
		map[*lcommon.Address]uint64{destination: 120 * ada},
		destinationReturn, 1*ada, false,
	)
	_, reservesBefore, _ := networkState(t, f.db)

	nonce := make([]byte, 32)
	eraBounds := make([]ledgerstate.EraBound, ledgerstate.EraConway+1)
	for i := range eraBounds {
		eraBounds[i] = ledgerstate.EraBound{
			Slot:  f.currentEpoch.StartSlot,
			Epoch: f.currentEpoch.EpochId,
		}
	}
	require.NoError(t, ledgerstate.ImportLedgerState(
		context.Background(),
		ledgerstate.ImportConfig{
			Database: f.db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &ledgerstate.RawLedgerState{
				Epoch:               f.currentEpoch.EpochId,
				Treasury:            100 * ada,
				Reserves:            reservesBefore,
				Donation:            50 * ada,
				EraIndex:            ledgerstate.EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &ledgerstate.SnapshotTip{
					Slot:      f.currentEpoch.StartSlot + 50,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, uint(f.currentEpoch.LengthInSlots), nil
			},
		},
	))

	f.rollover(t, f.currentEpoch, f.currentPParams)
	assert.NotNil(
		t,
		f.proposal(t, withdrawal).RatifiedEpoch,
		"the imported donation counts toward the RATIFY treasury",
	)
	treasury, reserves, _ := networkState(t, f.db)
	assert.Equal(t, 150*ada, treasury,
		"the boundary credits the imported pre-anchor donation")
	assert.Equal(t, reservesBefore, reserves)
}

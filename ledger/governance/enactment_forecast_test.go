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

package governance

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForecastHardForkInitiations pins which proposals the forecast applies:
// the ones ENACT applies at the boundary, in its order, and no others.
func TestForecastHardForkInitiations(t *testing.T) {
	t.Parallel()

	const (
		newEpoch     uint64 = 20
		boundarySlot uint64 = 2_000
	)
	updateFn := func(
		pparams lcommon.ProtocolParameters,
		_ any,
	) (lcommon.ProtocolParameters, error) {
		return pparams, nil
	}
	hardFork := func(seed byte, major uint) *models.GovernanceProposal {
		action := &lcommon.HardForkInitiationGovAction{
			Type: uint(lcommon.GovActionTypeHardForkInitiation),
		}
		action.ProtocolVersion.Major = major
		encoded, err := cbor.Encode(action)
		require.NoError(t, err)
		return &models.GovernanceProposal{
			TxHash:        testBytes(32, seed),
			ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
			GovActionCbor: encoded,
			ExpiresEpoch:  100,
			AnchorURL:     "https://example.invalid/hfi",
			AnchorHash:    testBytes(32, seed+1),
			ReturnAddress: testBytes(29, 0),
			AddedSlot:     1,
		}
	}
	ratified := func(p *models.GovernanceProposal) *models.GovernanceProposal {
		epoch, slot := newEpoch-1, uint64(1_500)
		p.RatifiedEpoch, p.RatifiedSlot = &epoch, &slot
		return p
	}
	enactedHere := func(p *models.GovernanceProposal) *models.GovernanceProposal {
		epoch, slot := newEpoch-1, uint64(1_500)
		p.RatifiedEpoch, p.RatifiedSlot = &epoch, &slot
		enactedEpoch, enactedSlot := newEpoch, boundarySlot
		p.EnactedEpoch, p.EnactedSlot = &enactedEpoch, &enactedSlot
		return p
	}
	run := func(
		t *testing.T,
		pparams lcommon.ProtocolParameters,
		pending []*models.GovernanceProposal,
		stored ...*models.GovernanceProposal,
	) lcommon.ProtocolParameters {
		t.Helper()
		db, _ := newTallyTestDB(t)
		for _, p := range stored {
			require.NoError(t, db.SetGovernanceProposal(
				context.Background(), p, nil,
			))
		}
		got, err := ForecastHardForkInitiations(
			context.Background(), db, nil,
			newEpoch, boundarySlot, pparams, updateFn, pending,
		)
		require.NoError(t, err)
		return got
	}
	major := func(t *testing.T, pparams lcommon.ProtocolParameters) uint {
		t.Helper()
		conwayPParams, ok := pparams.(*conway.ConwayProtocolParameters)
		require.True(t, ok)
		return conwayPParams.ProtocolVersion.Major
	}

	t.Run("a ratified initiation sets the version", func(t *testing.T) {
		t.Parallel()
		input := stabilityConwayPParams(11)
		got := run(t, input, nil, ratified(hardFork(0x10, 12)))
		assert.Equal(t, uint(12), major(t, got))
		assert.Equal(t, uint(11), input.ProtocolVersion.Major,
			"the forecast must not mutate its input")
	})
	t.Run("a proposal that is not ratified is ignored", func(t *testing.T) {
		t.Parallel()
		got := run(t, stabilityConwayPParams(11), nil, hardFork(0x20, 12))
		assert.Equal(t, uint(11), major(t, got))
	})
	t.Run("a proposal already enacted earlier is ignored", func(t *testing.T) {
		t.Parallel()
		earlier := hardFork(0x30, 12)
		epoch, slot := newEpoch-3, uint64(900)
		earlier.RatifiedEpoch, earlier.RatifiedSlot = &epoch, &slot
		earlier.EnactedEpoch, earlier.EnactedSlot = &epoch, &slot
		got := run(t, stabilityConwayPParams(11), nil, earlier)
		assert.Equal(t, uint(11), major(t, got))
	})
	t.Run("an initiation enacted at this boundary is replayed", func(t *testing.T) {
		t.Parallel()
		got := run(t, stabilityConwayPParams(11), nil, enactedHere(hardFork(0x40, 12)))
		assert.Equal(t, uint(12), major(t, got))
	})
	t.Run("a decision not yet written counts after the stored ones", func(t *testing.T) {
		t.Parallel()
		got := run(
			t, stabilityConwayPParams(10),
			[]*models.GovernanceProposal{hardFork(0x51, 12)},
			ratified(hardFork(0x50, 11)),
		)
		assert.Equal(t, uint(12), major(t, got))
	})
	t.Run("an initiation ENACT returns to pending is skipped", func(t *testing.T) {
		t.Parallel()
		unenactable := hardFork(0x60, 12)
		unenactable.Deposit = 1
		unenactable.ReturnAddress = []byte{0x01}
		got := run(t, stabilityConwayPParams(11), nil, ratified(unenactable))
		assert.Equal(t, uint(11), major(t, got))
	})
	t.Run("another action type leaves the version", func(t *testing.T) {
		t.Parallel()
		info := ratified(hardFork(0x70, 12))
		info.ActionType = uint8(lcommon.GovActionTypeInfo)
		got := run(t, stabilityConwayPParams(11), nil, info)
		assert.Equal(t, uint(11), major(t, got))
	})
	t.Run("pre-conway parameters are returned unchanged", func(t *testing.T) {
		t.Parallel()
		db, _ := newTallyTestDB(t)
		input := &babbage.BabbageProtocolParameters{}
		got, err := ForecastHardForkInitiations(
			context.Background(), db, nil,
			newEpoch, boundarySlot, input, updateFn, nil,
		)
		require.NoError(t, err)
		assert.Same(t, input, got)
	})
}

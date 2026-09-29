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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/stretchr/testify/require"
)

// newByronEpochLengthConfig returns a node config whose Byron genesis has
// security parameter k, so the Byron epoch is 10 * k slots.
func newByronEpochLengthConfig(
	t *testing.T,
	k uint64,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(
		t, cfg, strings.NewReader(testByronGenesisJSONForK(k)),
	))
	return cfg
}

func TestByronEpochSlotsFollowsConfiguredSecurityParam(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(30000), byronEpochSlots(
		newByronEpochLengthConfig(t, 3000),
	))
	require.Equal(t, uint64(100), byronEpochSlots(
		newByronEpochLengthConfig(t, 10),
	))
	require.Equal(t, uint64(byron.ByronSlotsPerEpoch), byronEpochSlots(nil))
	require.Equal(
		t,
		uint64(byron.ByronSlotsPerEpoch),
		byronEpochSlots(&cardano.CardanoNodeConfig{}),
	)
}

// TestValidateInboundBlockEnvelopeByronOrderingUsesConfiguredEpochLength
// covers #4408 on a genesis whose epoch is longer than gouroboros' fixed
// 21,600 slots (k = 3000, 30,000 slots). With the fixed length the last slots
// of an epoch number higher than the next epoch's boundary block, so valid
// transitions were rejected.
func TestValidateInboundBlockEnvelopeByronOrderingUsesConfiguredEpochLength(
	t *testing.T,
) {
	t.Parallel()

	nodeConfig := newByronEpochLengthConfig(t, 3000)
	tests := []struct {
		name    string
		block   gledger.Block
		parent  gledger.Block
		wantErr string
	}{
		{
			"regular to EBB after the fixed-length boundary",
			byronOrderingEbb(1, 5),
			byronOrderingMain(0, 25000, 5),
			"",
		},
		{
			"regular to EBB in a later epoch",
			byronOrderingEbb(3, 9),
			byronOrderingMain(2, 29000, 9),
			"",
		},
		{
			"regular to EBB of its own epoch",
			byronOrderingEbb(1, 5),
			byronOrderingMain(1, 5, 5),
			"does not follow parent slot",
		},
		{
			"EBB to regular at the boundary slot",
			byronOrderingMain(1, 0, 6),
			byronOrderingEbb(1, 5),
			"",
		},
		{
			"EBB to EBB across an empty epoch",
			byronOrderingEbb(3, 7),
			byronOrderingEbb(1, 6),
			"",
		},
		{
			"EBB to EBB in the same epoch",
			byronOrderingEbb(1, 7),
			byronOrderingEbb(1, 6),
			"does not follow parent slot",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateInboundBlockEnvelope(
				test.block,
				nil,
				nodeConfig,
				envelopeParentFromBlock(test.parent),
			)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

// TestValidateInboundBlockEnvelopeByronOrderingFromPersistedTip covers the
// parent that only has the slot the chain stored, numbered with the fixed
// epoch length: it is split back into epoch and slot before it is compared
// with a block numbered by the configured length.
func TestValidateInboundBlockEnvelopeByronOrderingFromPersistedTip(
	t *testing.T,
) {
	t.Parallel()

	nodeConfig := newByronEpochLengthConfig(t, 10)
	tip := func(epoch, slot, blockNumber uint64, blockType uint) envelopeParent {
		return envelopeParentFromTip(
			epoch*byron.ByronSlotsPerEpoch+slot,
			blockNumber,
			[]byte{1},
			blockType,
			true,
		)
	}
	tests := []struct {
		name    string
		block   gledger.Block
		parent  envelopeParent
		wantErr string
	}{
		{
			"regular to regular in the same epoch",
			byronOrderingMain(1, 51, 6),
			tip(1, 50, 5, uint(gledger.BlockTypeByronMain)),
			"",
		},
		{
			"regular to regular with an equal slot",
			byronOrderingMain(1, 50, 6),
			tip(1, 50, 5, uint(gledger.BlockTypeByronMain)),
			"does not follow parent slot",
		},
		{
			"regular to EBB at the next boundary",
			byronOrderingEbb(2, 5),
			tip(1, 99, 5, uint(gledger.BlockTypeByronMain)),
			"",
		},
		{
			"EBB to regular at the boundary slot",
			byronOrderingMain(2, 0, 6),
			tip(2, 0, 5, uint(gledger.BlockTypeByronEbb)),
			"",
		},
		{
			"EBB to EBB across an empty epoch",
			byronOrderingEbb(4, 6),
			tip(2, 0, 5, uint(gledger.BlockTypeByronEbb)),
			"",
		},
		{
			"EBB to EBB in the same epoch",
			byronOrderingEbb(2, 6),
			tip(2, 0, 5, uint(gledger.BlockTypeByronEbb)),
			"does not follow parent slot",
		},
		{
			// A stored slot within the epoch beyond the configured length
			// cannot be split back, so the stored slots are compared.
			"parent that cannot be split falls back to stored slots",
			byronOrderingEbb(1, 5),
			tip(0, 20000, 5, uint(gledger.BlockTypeByronMain)),
			"",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateInboundBlockEnvelope(
				test.block, nil, nodeConfig, test.parent,
			)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

// TestValidateByronEbbPlacementRejectsUnrepresentableBoundarySlot covers the
// epoch-boundary-slot check: an epoch whose boundary slot does not fit a
// uint64 at the configured length wrapped around under the fixed length and
// was accepted as slot 0.
func TestValidateByronEbbPlacementRejectsUnrepresentableBoundarySlot(
	t *testing.T,
) {
	t.Parallel()

	nodeConfig := newByronEpochLengthConfig(t, 10)
	ebb := byronOrderingEbb(1<<63, 0)
	require.ErrorContains(
		t,
		validateByronEbbPlacement(ebb, byronEpochSlots(nodeConfig)),
		"overflow",
	)
	require.ErrorContains(
		t,
		validateInboundBlockEnvelope(
			ebb, nil, nodeConfig, envelopeParent{origin: true},
		),
		"overflow",
	)
	require.NoError(t, validateByronEbbPlacement(
		byronOrderingEbb(3, 0), byronEpochSlots(nodeConfig),
	))
}

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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func TestTranslateRatifiedGovActions_ConwayToDijkstra(t *testing.T) {
	db, _ := newTallyTestDB(t)

	fee := uint(1234)
	action := &conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			MinFeeA: &fee,
		},
		PolicyHash: []byte{0xaa, 0xbb},
	}
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)

	ratifiedEpoch := uint64(10)
	ratifiedSlot := uint64(200)
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 0xE1),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch: 9,
		ExpiresEpoch:  20,
		GovActionCbor: actionCbor,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AddedSlot:     100,
		AnchorURL:     "https://example.invalid/translate",
		AnchorHash:    testBytes(32, 0xE2),
		ReturnAddress: testBytes(29, 0xE3),
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))

	require.NoError(t, TranslateRatifiedGovActions(
		db,
		nil,
		conway.EraIdConway,
		gdijkstra.EraIdDijkstra,
	))

	got, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	var translated gdijkstra.DijkstraParameterChangeGovAction
	_, err = cbor.Decode(got.GovActionCbor, &translated)
	require.NoError(t, err)
	require.NotNil(t, translated.ParamUpdate.MinFeeA)
	require.Equal(t, uint(1234), *translated.ParamUpdate.MinFeeA)
	require.Equal(t, []byte{0xaa, 0xbb}, translated.PolicyHash)
}

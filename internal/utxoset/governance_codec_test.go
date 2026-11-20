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

package utxoset

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// A proposal with no parent and a vote with no anchor read back from the set
// exactly as they were staged: absent byte fields are nil, not empty.
// Ratification tells a root proposal from a child by a nil ParentTxHash.
func TestGovernanceRowsDecodeAbsentBytesAsNil(t *testing.T) {
	t.Parallel()
	key := append([]byte{0}, bytes.Repeat([]byte{0x71}, 32)...)
	key = binary.BigEndian.AppendUint32(key, 0)
	staged := models.GovernanceProposal{
		ID:            7,
		TxHash:        bytes.Repeat([]byte{0x71}, 32),
		ActionType:    4,
		ProposedEpoch: 740,
		ExpiresEpoch:  751,
		Deposit:       100,
		AddedSlot:     74_090,
		AnchorURL:     "https://example.invalid/root",
		GovActionCbor: []byte{0x80},
	}
	loaded, err := decodeProposal(key, encodeProposal(staged))
	require.NoError(t, err)
	require.Nil(t, loaded.ParentTxHash)
	require.Nil(t, loaded.ParentActionIdx)
	require.Nil(t, loaded.PolicyHash)
	require.Equal(t, staged, loaded)

	voteKey := append(bytes.Clone(key), 1, 0)
	voteKey = append(voteKey, bytes.Repeat([]byte{0xe1}, 28)...)
	vote, err := decodeVote(voteKey, encodeVote(models.GovernanceVote{
		ID: 3, ProposalID: 7, Vote: 1, AddedSlot: 74_095,
	}))
	require.NoError(t, err)
	require.Nil(t, vote.AnchorHash)
}

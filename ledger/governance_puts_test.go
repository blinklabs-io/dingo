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
	"github.com/stretchr/testify/require"
)

// A write that sets two lifecycle groups at different slots is put once per
// slot, so a rollback between them leaves the earlier group, and a new row
// is first put at its added slot without any lifecycle.
func TestProposalPutsSplitLifecycleBySlot(t *testing.T) {
	t.Parallel()
	u64 := func(v uint64) *uint64 { return &v }
	in := models.GovernanceProposal{
		TxHash: []byte{1}, AddedSlot: 5, ExpiresEpoch: 9,
		RatifiedEpoch: u64(2), RatifiedSlot: u64(10),
		EnactedEpoch: u64(3), EnactedSlot: u64(20),
		GovActionCbor: []byte{0xaa},
	}
	puts := proposalPuts(nil, mergeProposal(nil, in))
	require.Len(t, puts, 3)
	require.Equal(t, uint64(5), puts[0].Slot)
	require.Nil(t, puts[0].Proposal.RatifiedSlot)
	require.Nil(t, puts[0].Proposal.EnactedSlot)
	require.Equal(t, uint64(10), puts[1].Slot)
	require.Equal(t, uint64(10), *puts[1].Proposal.RatifiedSlot)
	require.Nil(t, puts[1].Proposal.EnactedSlot, "the enactment waits for its slot")
	require.Equal(t, uint64(20), puts[2].Slot)
	require.Equal(t, uint64(20), *puts[2].Proposal.EnactedSlot)
	require.Equal(t, []byte{0xaa}, puts[2].Proposal.GovActionCbor)
}

// An existing row keeps its added slot and lifecycle fields the write does
// not set, and an empty action CBOR keeps the stored bytes; a write that
// changes nothing puts nothing.
func TestMergeProposalKeepsWhatTheUpsertKeeps(t *testing.T) {
	t.Parallel()
	u64 := func(v uint64) *uint64 { return &v }
	prev := models.GovernanceProposal{
		TxHash: []byte{1}, AddedSlot: 5, ExpiresEpoch: 9,
		RatifiedEpoch: u64(2), RatifiedSlot: u64(10),
		GovActionCbor: []byte{0xaa},
	}
	in := models.GovernanceProposal{TxHash: []byte{1}, AddedSlot: 99, ExpiresEpoch: 11}
	next := mergeProposal(&prev, in)
	require.Equal(t, uint64(5), next.AddedSlot)
	require.Equal(t, uint64(11), next.ExpiresEpoch)
	require.Equal(t, uint64(10), *next.RatifiedSlot)
	require.Equal(t, []byte{0xaa}, next.GovActionCbor)
	require.Empty(t, proposalPuts(&prev, mergeProposal(&prev, prev)))
}

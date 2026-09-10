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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

func TestRatificationHistorySurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	first, err := NewSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	require.NoError(t, first.Start(t.Context()))

	ratifiedEpoch := uint64(5)
	ratifiedSlot := uint64(550)
	proposal := &models.GovernanceProposal{
		TxHash:        []byte("restart-ratification-history"),
		ActionIndex:   0,
		ActionType:    6,
		ProposedEpoch: 1,
		ExpiresEpoch:  100,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/governance",
		AnchorHash:    []byte("restart-governance-anchor"),
		ReturnAddress: []byte("restart-return-address"),
		GovActionCbor: []byte{0x80},
		AddedSlot:     500,
	}
	write := first.Transaction(t.Context())
	require.NoError(t, first.SetGovernanceProposal(proposal, write))
	require.NoError(t, first.ClearGovernanceProposalRatification(
		proposal.TxHash,
		proposal.ActionIndex,
		600,
		write,
	))
	require.NoError(t, write.Commit())
	require.NoError(t, first.Close())

	second, err := NewSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.Start(t.Context()))

	read := second.ReadTransaction(t.Context())
	got, err := second.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		read,
	)
	require.NoError(t, err)
	require.NoError(t, read.Rollback())
	require.NotNil(t, got)
	require.Nil(t, got.RatifiedEpoch)
	require.Nil(t, got.RatifiedSlot)

	rollback := second.Transaction(t.Context())
	require.NoError(t, second.DeleteGovernanceProposalsAfterSlot(599, rollback))
	require.NoError(t, rollback.Commit())

	read = second.ReadTransaction(t.Context())
	got, err = second.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		read,
	)
	require.NoError(t, err)
	require.NoError(t, read.Rollback())
	require.NotNil(t, got)
	require.NotNil(t, got.RatifiedEpoch)
	require.NotNil(t, got.RatifiedSlot)
	require.Equal(t, uint64(5), *got.RatifiedEpoch)
	require.Equal(t, uint64(550), *got.RatifiedSlot)
}

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

package ledger_test

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// A ledger-state import seeds each per-purpose root as an enacted proposal at
// the snapshot anchor, because the snapshot does not record when the root was
// enacted. The set keeps it enacted for governance, but the export to the
// IndexerDB must not present the anchor as the root's enactment epoch.
func TestSeededRootEnactmentIsNotExported(t *testing.T) {
	t.Parallel()
	h := newSetHarness(t)
	ldb := h.reads()
	anchor := h.scenario.CurrentPoint().Slot
	epoch := uint64(1458)
	const parameterChange = uint8(0)

	rootHash := bytes.Repeat([]byte{0x31}, 32)
	root := &models.GovernanceProposal{
		TxHash: rootHash, ActionType: parameterChange,
		ReturnAddress: make([]byte, 29), AnchorHash: make([]byte, 32),
		EnactedEpoch: &epoch, EnactedSlot: &anchor,
	}
	realHash := bytes.Repeat([]byte{0x32}, 32)
	ratifiedEpoch, ratifiedSlot := epoch-1, anchor-1
	realEpoch, realSlot := epoch, anchor
	real := &models.GovernanceProposal{
		TxHash: realHash, ActionType: parameterChange,
		ProposedEpoch: epoch - 2, ExpiresEpoch: epoch + 4, Deposit: 1000,
		ReturnAddress: bytes.Repeat([]byte{0xe0}, 29),
		AnchorHash: bytes.Repeat(
			[]byte{7},
			32,
		), AnchorURL: "https://example.test/p",
		RatifiedEpoch: &ratifiedEpoch, RatifiedSlot: &ratifiedSlot,
		EnactedEpoch: &realEpoch, EnactedSlot: &realSlot,
		AddedSlot: anchor - 2,
	}
	txn := ldb.Begin(t.Context(), true)
	require.NoError(t, ldb.SetGovernanceProposal(t.Context(), root, txn))
	require.NoError(t, ldb.SetGovernanceProposal(t.Context(), real, txn))
	require.NoError(t, txn.Commit())

	rows := readAllFactRows(
		t, h.scenario.ReadFactRows, "governance_proposal", 0, math.MaxInt64,
	)
	rendered := func(
		hash []byte, ratifiedE, ratifiedS, enactedE, enactedS any,
	) string {
		return fmt.Sprintf(
			"action_index=0 deleted_slot=<nil> enacted_epoch=%v "+
				"enacted_slot=%v expired_epoch=<nil> expired_slot=<nil> "+
				"ratified_epoch=%v ratified_slot=%v tx_hash=%x",
			enactedE, enactedS, ratifiedE, ratifiedS, hash,
		)
	}
	require.Contains(t, rows,
		rendered(realHash, ratifiedEpoch, ratifiedSlot, realEpoch, realSlot),
		"a recorded enactment is exported")
	require.NotContains(t, rows,
		rendered(rootHash, "<nil>", "<nil>", epoch, anchor),
		"the seeded root's anchor is not exported as its enactment")
	require.Contains(t, rows,
		rendered(rootHash, "<nil>", "<nil>", "<nil>", "<nil>"),
		"the seeded root's enactment epoch is unknown")
}

// The seeded root stays enacted for governance: with no later enactment of
// its purpose it is the purpose's current root.
func TestSeededRootStaysTheCurrentRoot(t *testing.T) {
	t.Parallel()
	h := newSetHarness(t)
	ldb := h.reads()
	anchor := h.scenario.CurrentPoint().Slot
	epoch := uint64(1458)
	const hardFork = uint8(1)
	rootHash := bytes.Repeat([]byte{0x41}, 32)
	txn := ldb.Begin(t.Context(), true)
	require.NoError(t, ldb.SetGovernanceProposal(t.Context(),
		&models.GovernanceProposal{
			TxHash: rootHash, ActionType: hardFork,
			ReturnAddress: make([]byte, 29), AnchorHash: make([]byte, 32),
			EnactedEpoch: &epoch, EnactedSlot: &anchor,
		}, txn))
	require.NoError(t, txn.Commit())

	read := ldb.Begin(t.Context(), false)
	defer read.Release()
	current, err := ldb.GetLastEnactedGovernanceProposal(
		t.Context(), []uint8{hardFork}, read,
	)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.Equal(t, rootHash, current.TxHash)
}

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

package ledgerstate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// TestImportGovStateRecordsProposalSubmissionOrder imports two proposals from
// one epoch, which share that epoch's anchor slot, listed in the snapshot's
// submission order with the higher transaction hash first. Each keeps its
// snapshot position, so the imported order is the submission order rather
// than the hash order.
func TestImportGovStateRecordsProposalSubmissionOrder(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	first := bytes.Repeat([]byte{0x92}, 32)
	second := bytes.Repeat([]byte{0x91}, 32)
	proposal := func(txHash []byte) []any {
		return []any{
			[]any{txHash, uint64(0)},
			map[uint64]uint64{},
			map[uint64]uint64{},
			map[uint64]uint64{},
			[]any{
				uint64(100_000_000),
				bytes.Repeat([]byte{0xa1}, 29),
				[]any{uint8(6)},
				[]any{
					"https://example.com/proposal",
					bytes.Repeat([]byte{0xb2}, 32),
				},
			},
			uint64(1275),
			uint64(1280),
		}
	}
	govState := []any{
		[]any{[]any{}, []any{proposal(first), proposal(second)}},
		[]any{},
		[]any{
			[]any{
				"https://example.com/constitution",
				bytes.Repeat([]byte{0xc3}, 32),
			},
			nil,
		},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		drepPulsingStateWithEnactCommittee(t, []any{}),
	}
	govStateData, err := cbor.Encode(govState)
	require.NoError(t, err)
	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         1277,
			EraIndex:      EraConway,
			EraBoundEpoch: 1200,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}
	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	for position, txHash := range [][]byte{first, second} {
		stored, err := db.Metadata().GetGovernanceProposal(txHash, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, stored)
		require.Equal(t, uint64(17_500), stored.AddedSlot)
		require.NotNil(t, stored.TxIndex)
		require.Equal(t, uint32(position), *stored.TxIndex) //nolint:gosec
	}
	active, err := db.GetActiveGovernanceProposals(1277, nil)
	require.NoError(t, err)
	require.Len(t, active, 2)
	require.Equal(t, first, active[0].TxHash)
	require.Equal(t, second, active[1].TxHash)
}

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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// donationTestCurrentEra encodes a minimal Current era wrapper whose
// UTxOState carries the given elements after the UTxO map.
func donationTestCurrentEra(t *testing.T, utxoStateTail ...any) []byte {
	t.Helper()
	emptyMap := cbor.RawMessage{0xa0}
	utxoState := append([]any{emptyMap}, utxoStateTail...)
	epochState := []any{
		[]uint64{1_000, 2_000},
		[]any{[]any{}, utxoState},
		[]any{},
		[]any{},
	}
	newEpochState := []any{uint64(317), emptyMap, emptyMap, epochState}
	tip := []any{[]any{uint64(1_500), uint64(7), make([]byte, 32)}}
	current := []any{
		[]uint64{0, 1_000, 310},
		[]any{tip, newEpochState},
	}
	data, err := cbor.Encode(current)
	require.NoError(t, err)
	return data
}

// UTxOState is [utxo, deposited, fees, govState, instantStake, donation] in
// cardano-ledger's Shelley/LedgerState/Types.hs; the donation is the treasury
// donations collected this epoch up to the anchor.
func TestParseCurrentEraDecodesUTxOStateDonation(t *testing.T) {
	t.Parallel()

	emptyMap := cbor.RawMessage{0xa0}
	state, err := parseCurrentEra(EraConway, donationTestCurrentEra(
		t, uint64(0), uint64(300), []any{}, emptyMap, uint64(6_500_000),
	))
	require.NoError(t, err)
	require.Equal(t, uint64(317), state.Epoch)
	require.Equal(t, uint64(300), state.Fees)
	require.Equal(t, uint64(6_500_000), state.Donation)

	state, err = parseCurrentEra(EraConway, donationTestCurrentEra(
		t, uint64(0), uint64(300), []any{},
	))
	require.NoError(t, err)
	require.Zero(t, state.Donation, "a shorter UTxOState has no donation")

	_, err = parseCurrentEra(EraConway, donationTestCurrentEra(
		t, uint64(0), uint64(300), []any{}, emptyMap, "not a coin",
	))
	require.ErrorContains(t, err, "decoding UTxOState donation")
}

// The snapshot's donation is the anchor epoch's donations up to the anchor
// block. ImportLedgerState must record it under that epoch, where the next
// boundary's treasury credit and RATIFY seed read it, replacing any rows local
// replay wrote for that epoch before a catch-up import and leaving other
// epochs alone.
func TestImportLedgerStateSeedsAnchorEpochDonations(t *testing.T) {
	t.Parallel()

	const (
		epoch      = uint64(317)
		priorSlot  = uint64(300)
		localSlot  = uint64(900)
		anchorSlot = uint64(1_000)
		donation   = uint64(6_500_000)
	)
	tests := []struct {
		name     string
		donation uint64
		want     uint64
	}{
		{name: "nonzero donation", donation: donation, want: donation},
		{name: "zero donation", donation: 0, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := dbtest.NewDatabase(
				t, &database.Config{DataDir: t.TempDir()},
			)
			require.NoError(t, err)
			t.Cleanup(func() { dbtest.CloseDatabase(db) })
			meta := db.Metadata()
			require.NoError(t, meta.AddNetworkDonation(
				priorSlot, epoch-1, 11, nil,
			))
			require.NoError(t, meta.AddNetworkDonation(
				localSlot, epoch, 22, nil,
			))

			nonce := make([]byte, 32)
			state := &RawLedgerState{
				Epoch:               epoch,
				Donation:            tc.donation,
				EraIndex:            EraConway,
				EraBounds:           make([]EraBound, EraConway+1),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      anchorSlot,
					BlockHash: make([]byte, 32),
				},
			}
			for i := range state.EraBounds {
				state.EraBounds[i] = EraBound{
					Slot:  priorSlot + 500,
					Epoch: epoch,
				}
			}
			cfg := ImportConfig{
				Database: db,
				Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
				State:    state,
				EpochLength: func(uint) (uint, uint, error) {
					return 1, 1_000, nil
				},
			}
			// Twice, as a resumed import re-runs the sweep.
			for range 2 {
				require.NoError(t, ImportLedgerState(
					context.Background(), cfg,
				))
				sum, err := meta.SumNetworkDonationsForEpoch(epoch, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, sum,
					"the anchor epoch's donations must be the snapshot's")
				prior, err := meta.SumNetworkDonationsForEpoch(
					epoch-1, nil,
				)
				require.NoError(t, err)
				require.Equal(t, uint64(11), prior,
					"earlier epochs' donations must be kept")
			}
		})
	}
}

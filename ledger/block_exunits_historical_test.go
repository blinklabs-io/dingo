// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// conwayRedeemerBlock wraps one transaction declaring a single redeemer with
// the given execution units. Its input does not exist, so the block only
// applies when per-transaction validation is skipped.
func conwayRedeemerBlock(
	t *testing.T,
	slot uint64,
	memory, steps uint64,
) (*conway.ConwayBlock, *database.BlockIngestionResult) {
	t.Helper()
	witness := map[uint]any{
		5: map[[2]uint][]any{
			{0, 0}: {uint(0), []uint64{memory, steps}},
		},
	}
	txCbor, err := cbor.Encode([]any{
		map[uint]any{
			0: cbor.Tag{
				Number: 258,
				Content: []any{
					[]any{bytes.Repeat([]byte{0xe1}, 32), uint(0)},
				},
			},
			1: []any{},
			2: uint64(0),
		},
		witness,
		true,
		nil,
	})
	require.NoError(t, err)
	return conwayTestBlock(t, txCbor, uint(lcommon.ProtocolVersionPlomin), slot)
}

func TestLedgerProcessBlockEnforcesBlockExUnitsWithoutValidation(
	t *testing.T,
) {
	t.Parallel()

	const slot = uint64(10)
	pparams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
		MaxBlockBodySize:   100_000,
		MaxBlockHeaderSize: 100_000,
		MaxBlockExUnits:    lcommon.ExUnits{Memory: 1_000, Steps: 2_000},
	}
	for _, tc := range []struct {
		name          string
		memory, steps uint64
		trustedReplay bool
		mithrilSlot   uint64
		wantExceeds   bool
	}{
		{name: "memory over limit", memory: 1_001, steps: 1, wantExceeds: true},
		{name: "steps over limit", memory: 1, steps: 2_001, wantExceeds: true},
		{name: "exactly at limit", memory: 1_000, steps: 2_000},
		{
			name:   "over limit but trusted replay",
			memory: 1_001, steps: 1, trustedReplay: true,
		},
		{
			name:   "over limit but covered by mithril",
			memory: 1_001, steps: 1, mithrilSlot: slot,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			block, offsets := conwayRedeemerBlock(t, slot, tc.memory, tc.steps)
			ls := newRequiredDatumLedger(t, db, eras.ConwayEraDesc)
			ls.config.TrustedReplay = tc.trustedReplay
			ls.mithrilLedgerSlot = tc.mithrilSlot
			err := ls.db.Transaction(context.Background(), true).Do(
				func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(
						context.Background(),
						txn,
						ocommon.NewPoint(slot, block.Hash().Bytes()),
						block,
						false,
						false,
						false,
						nil,
						envelopeParent{origin: true},
						offsets,
						eras.ConwayEraDesc,
						pparams,
						nil,
						0,
						0,
						false,
					)
					return err
				},
			)
			if tc.wantExceeds {
				require.ErrorContains(t, err, "exceed maxBlockExUnits")
				return
			}
			require.NoError(t, err)
		})
	}
}

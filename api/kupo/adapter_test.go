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

package kupo

import (
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func TestIsMetadataBlockUnavailable(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]error{
		"block not found":         models.ErrBlockNotFound,
		"wrapped block not found": fmt.Errorf("lookup: %w", models.ErrBlockNotFound),
		"history expired":         types.ErrHistoryExpired,
		"wrapped history expired": errors.Join(errors.New("lookup"), types.ErrHistoryExpired),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if !isMetadataBlockUnavailable(err) {
				t.Fatalf("isMetadataBlockUnavailable(%v) = false", err)
			}
		})
	}

	if isMetadataBlockUnavailable(errors.New("database unavailable")) {
		t.Fatal("unrelated database errors must remain internal errors")
	}
}

func TestMetadataBlockBeforeSlotSkipsExpiredAncestors(t *testing.T) {
	points := []ocommon.Point{
		{Slot: 30, Hash: []byte{30}},
		{Slot: 20, Hash: []byte{20}},
		{Slot: 10, Hash: []byte{10}},
	}
	blocks := map[uint64]struct {
		block models.Block
		err   error
	}{
		30: {err: types.ErrHistoryExpired},
		20: {err: types.ErrHistoryExpired},
		10: {block: models.Block{Slot: 10, Hash: []byte{10}}},
	}
	var requested []uint64
	block, err := metadataBlockBeforeSlot(
		40,
		func(slot uint64) (ocommon.Point, error) {
			requested = append(requested, slot)
			for _, point := range points {
				if point.Slot <= slot {
					return point, nil
				}
			}
			return ocommon.Point{}, models.ErrBlockNotFound
		},
		func(point ocommon.Point) (models.Block, error) {
			entry := blocks[point.Slot]
			return entry.block, entry.err
		},
	)
	if err != nil {
		t.Fatalf("metadataBlockBeforeSlot returned error: %v", err)
	}
	if block.Slot != 10 {
		t.Fatalf("readable ancestor slot = %d, want 10", block.Slot)
	}
	wantRequested := []uint64{39, 29, 19}
	if fmt.Sprint(requested) != fmt.Sprint(wantRequested) {
		t.Fatalf("point lookup slots = %v, want %v", requested, wantRequested)
	}
}

func TestSpendingTransactionDetailsUseCanonicalInputOrder(t *testing.T) {
	inputA1 := models.Utxo{TxId: repeatedByte(0x11), OutputIdx: 1}
	inputA2 := models.Utxo{TxId: repeatedByte(0x11), OutputIdx: 2}
	inputB0 := models.Utxo{TxId: repeatedByte(0x22), OutputIdx: 0}
	details := newSpendingTransactionDetails(&models.Transaction{
		// Metadata hydration orders these by UTxO row ID, which is not the
		// canonical spend-purpose order used by the ledger and Kupo.
		Inputs: []models.Utxo{inputB0, inputA2, inputA1},
		Redeemers: []models.Redeemer{
			{Tag: uint8(lcommon.RedeemerTagMint), Index: 1, Data: []byte{0x01}},
			{
				Tag:   uint8(lcommon.RedeemerTagSpend),
				Index: 1,
				Data:  []byte{0xd8, 0x79, 0x80},
			},
		},
	})

	if got := details.inputIndexes[utxoReference(inputA1)]; got != 0 {
		t.Fatalf("first canonical input index = %d, want 0", got)
	}
	if got := details.inputIndexes[utxoReference(inputA2)]; got != 1 {
		t.Fatalf("second canonical input index = %d, want 1", got)
	}
	if got := details.inputIndexes[utxoReference(inputB0)]; got != 2 {
		t.Fatalf("third canonical input index = %d, want 2", got)
	}
	if got := details.redeemers[1]; got != "d87980" {
		t.Fatalf("spend redeemer = %q, want d87980", got)
	}
	if len(details.redeemers) != 1 {
		t.Fatalf("spend redeemer count = %d, want 1", len(details.redeemers))
	}
}

func repeatedByte(value byte) []byte {
	ret := make([]byte, 32)
	for i := range ret {
		ret[i] = value
	}
	return ret
}

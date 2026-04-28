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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueryHardForkEraHistory_TransitionImpossible_MultiEpochEra reproduces
// the real-world case that the single-epoch TransitionImpossible tests miss:
// the current era has been running for several epochs, and the current
// epoch is well past the first.
//
// dingo sets TransitionImpossible in `evaluateTransitionImpossible` when the
// CURRENT epoch's end is inside the safe-zone horizon. The caller therefore
// expects `queryHardForkEraHistory` to serve the CURRENT epoch's end as
// EraEnd. But if the caller naively forwards `TransitionImpossible` into
// `hardfork.BuildSummary`, BuildSummary's Haskell-aligned semantics apply
// the safe zone from `current.Start` (the *first* epoch of the era) — and
// the resulting EraEnd lags many epochs behind the tip.
func TestQueryHardForkEraHistory_TransitionImpossible_MultiEpochEra(t *testing.T) {
	const (
		slotLenMs = uint(1_000)
		epochLen  = uint(432_000)

		// Conway era has epochs 500, 501, 502 in the DB. Current epoch is 502.
		firstEpochId    = uint64(500)
		firstEpochSlot  = uint64(100_000)
		secondEpochId   = uint64(501)
		secondEpochSlot = uint64(532_000) // 100_000 + 432_000
		thirdEpochId    = uint64(502)
		thirdEpochSlot  = uint64(964_000) // 532_000 + 432_000
		thirdEpochEnd   = uint64(1_396_000)

		// Tip well inside epoch 502.
		tipSlot = uint64(1_100_000)
	)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		firstEpochSlot, firstEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		secondEpochSlot, secondEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		thirdEpochSlot, thirdEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionImpossible(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	eraList := result.(cbor.IndefLengthList)
	lastEra := eraList[len(eraList)-1].([]any)
	eraEnd := lastEra[1].([]any)

	slot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")
	epoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")

	assert.Equal(t, thirdEpochEnd, slot,
		"TransitionImpossible with a multi-epoch era must serve the CURRENT epoch's end "+
			"(slot %d, end of epoch %d), not a safe-zone projection from the era's first epoch",
		thirdEpochEnd, thirdEpochId)
	assert.Equal(t, thirdEpochId+1, epoch,
		"TransitionImpossible EraEnd epoch must be the current epoch + 1 (%d)",
		thirdEpochId+1)
	assert.GreaterOrEqual(t, slot, tipSlot,
		"TransitionImpossible EraEnd (%d) must never lag the tip (%d)",
		slot, tipSlot)
}

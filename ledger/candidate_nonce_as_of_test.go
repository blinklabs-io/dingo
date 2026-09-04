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
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComputeCandidateNonceAsOf_SlowPathStopsAtFoldEnd covers the CBOR-decode
// fallback's iteration bound.
//
// The sibling coverage in queries_chaindepstate_test.go seeds a block_nonce row
// for every block, so it only ever reaches computeCandidateNonceFast. The slow
// path takes the same two bounds and had none.
//
// It is pinned here by what the fold would have to touch rather than by the
// nonce it returns. Blocks inside the fold are Byron, which foldBlockEtaV
// returns unchanged without decoding, so they need no VRF fixture. The block
// past the tip is Conway with a body that cannot be decoded at all. A fold
// bounded at the tip never reaches it and succeeds; one bounded at the epoch's
// end decodes it and fails. That makes the assertion the bound itself, not a
// value derived from it.
//
// Only the iteration bound needs pinning. Given it, the slow path's use of
// candidateBound cannot diverge: iterFn only ever sees blocks below foldEndSlot,
// and for those `block.Slot < min(cutoffSlot, foldEndSlot)` is exactly
// `block.Slot < cutoffSlot`. The two bounds are separable on the fast path,
// where the lookups are independent seeks, which is where that case is covered.
func TestComputeCandidateNonceAsOf_SlowPathStopsAtFoldEnd(t *testing.T) {
	db := newTestDB(t)

	// Conway's window is 4k/f = 4*6/0.4 = 60, so with the epoch running
	// [1000, 2000) the candidate freezes at 1940 -- past every block here, so
	// the fold's end is the binding bound rather than the cutoff.
	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 1000
		earlySlot   uint64 = 1100
		tipSlot     uint64 = 1200
		beyondSlot  uint64 = 1300
	)

	prevEvolving := bytes.Repeat([]byte{0x61}, 32)
	prevCandidate := bytes.Repeat([]byte{0x62}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		// Byron blocks carry no Praos VRF contribution, so foldBlockEtaV
		// returns before touching the body. Their CBOR is never read.
		for _, blk := range []struct {
			slot   uint64
			hash   []byte
			number uint64
		}{
			{earlySlot, bytes.Repeat([]byte{0x63}, 32), 1},
			{tipSlot, bytes.Repeat([]byte{0x64}, 32), 2},
		} {
			if err := db.BlockCreate(models.Block{
				Slot:     blk.slot,
				Hash:     blk.hash,
				PrevHash: bytes.Repeat([]byte{0x65}, 32),
				Cbor:     []byte{0x80},
				Number:   blk.number,
				Type:     byron.BlockTypeByronMain,
			}, txn); err != nil {
				return err
			}
		}
		// The tripwire: past the tip, and a Conway body the decoder cannot
		// read. Reaching it is a decode error, which is the signal that the
		// fold ran past the point it was asked to stop at.
		return db.BlockCreate(models.Block{
			Slot:     beyondSlot,
			Hash:     bytes.Repeat([]byte{0x66}, 32),
			PrevHash: bytes.Repeat([]byte{0x67}, 32),
			Cbor:     []byte{0xff, 0xff, 0xff, 0xff},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn)
	}))

	// No block_nonce rows anywhere, so computeCandidateNonceFast finds the
	// tip's row empty, reports errNoncesMissing, and hands over to the slow
	// path -- which is the path under test.
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: newConwayBootstrapStabilityCfg(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(false).Do(func(txn *database.Txn) error {
		var err error
		candidate, evolving, err = ls.computeCandidateNonceAsOf(
			txn,
			eras.ConwayEraDesc.Id,
			prevEvolving,
			prevCandidate,
			epochStart,
			epochLength,
			foldEndSlotForTip(tipSlot),
		)
		return err
	}), "the fold must stop at the tip; reaching the block stored above it "+
		"means decoding a body that was never applied on this chain")

	// Byron blocks contribute nothing, so both nonces come through as the
	// epoch's carried values. What the test is really asserting is that this
	// returned at all rather than failing on the block past the tip.
	assert.Equal(t, prevEvolving, evolving,
		"blocks in the fold are Byron and fold to nothing, so the evolving "+
			"nonce is the value carried in")
	assert.Equal(t, prevCandidate, candidate,
		"and the candidate likewise stays at the value carried in")
}

// TestFoldEndSlotForTip pins the conversion from a tip slot to the fold's
// exclusive end bound, including the saturating edge.
//
// The edge is unreachable on any real chain, which is exactly why it is worth
// a test: nothing else would notice it becoming a wrap. A wrap to zero is not
// an off-by-one -- it folds no blocks at all, so the reply would carry the
// epoch's opening nonces while claiming to describe the tip.
func TestFoldEndSlotForTip(t *testing.T) {
	assert.Equal(t, uint64(1), foldEndSlotForTip(0),
		"the origin's own block is inside the fold")
	assert.Equal(t, uint64(1201), foldEndSlotForTip(1200),
		"the bound is exclusive, so it sits one past the tip")
	assert.Equal(t, ^uint64(0), foldEndSlotForTip(^uint64(0)),
		"the maximum slot saturates rather than wrapping to zero, which "+
			"would fold nothing and report the epoch's opening values")
}

func TestComputeCandidateNonceFastRejectsMalformedNonceRows(t *testing.T) {
	db := newTestDB(t)
	hash := bytes.Repeat([]byte{0x71}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: 100, Hash: hash, PrevHash: bytes.Repeat([]byte{0x72}, 32),
		Cbor: []byte{0x80}, Number: 1, Type: byron.BlockTypeByronMain,
	}, nil))
	require.NoError(t, db.SetBlockNonce(hash, 100, []byte{0x01}, false, nil))
	ls := &LedgerState{db: db}
	err := db.Transaction(false).Do(func(txn *database.Txn) error {
		_, _, err := ls.computeCandidateNonceFast(txn,
			bytes.Repeat([]byte{0x73}, 32), bytes.Repeat([]byte{0x74}, 32),
			0, 101, 101)
		return err
	})
	require.ErrorIs(t, err, errNoncesMissing)
}

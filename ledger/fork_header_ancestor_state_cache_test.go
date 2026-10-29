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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestPeerForkAncestorStateCheckedOncePerContext(t *testing.T) {
	t.Parallel()

	cfg := newHighFreqShelleyGenesisCfg(t)
	epochNonce := bytes.Repeat([]byte{0x21}, lcommon.Blake2b256Size)
	anchor := createTestBlockWithNonceAtSlots(
		t, [32]byte{90}, epochNonce, 2_010, 2_011,
		make([]byte, lcommon.Blake2b256Size), tamperNone,
	)
	anchorPoint := ocommon.NewPoint(
		anchor.block.SlotNumber(), anchor.block.Hash().Bytes(),
	)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.PrimaryChain().AddLocalBlock(context.Background(), anchor.block),
	)
	anchorNonce, err := eras.BabbageEraDesc.CalculateEtaVFunc(
		cfg, epochNonce, anchor.block,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetBlockNonce(
		anchorPoint.Hash, anchorPoint.Slot, anchorNonce, false, nil,
	))
	epoch := models.Epoch{
		EpochId:        6,
		StartSlot:      2_000,
		SlotLength:     1_000,
		LengthInSlots:  2_000,
		EraId:          eras.BabbageEraDesc.Id,
		Nonce:          epochNonce,
		EvolvingNonce:  epochNonce,
		CandidateNonce: epochNonce,
	}
	ls := &LedgerState{
		db:           db,
		chain:        cm.PrimaryChain(),
		epochCache:   []models.Epoch{epoch},
		currentEpoch: epoch,
		currentEra:   eras.BabbageEraDesc,
		currentTip: ochainsync.Tip{
			Point:       anchorPoint,
			BlockNumber: anchor.block.BlockNumber(),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.slotsPerKESPeriod.Store(anchor.slotsPerKesPeriod)
	ls.publishSnapshotsLocked()

	// Three chained headers; the last is the candidate and is never an
	// "ancestor" of itself.
	number := anchor.block.BlockNumber()
	prev := anchorPoint.Hash
	var blocks []gledger.Block
	for i := range 3 {
		number++
		n := number
		b := createTestBlockWithNonceAtSlotsAndBlockNumber(
			t, [32]byte{byte(91 + i)}, epochNonce,
			uint64(2_200+100*i), uint64(2_201+100*i), prev, &n, tamperNone,
		)
		seedBlockPoolRegistration(t, db, b.block)
		pool := b.block.IssuerVkey().Hash()
		seedPoolStakeSnapshot(t, db, 5, pool[:], 1_000_000_000)
		blocks = append(blocks, b.block)
		prev = b.block.Hash().Bytes()
	}
	byHash := map[string]gledger.Block{}
	for _, b := range blocks {
		byHash[string(b.Hash().Bytes())] = b
	}
	ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId, hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		b, ok := byHash[string(hash)]
		if !ok {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			Point:       ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes()),
			BlockHeader: b.Header(),
		}, anchorPoint.Hash, true
	}
	checks := map[uint64]int{}
	ls.onPeerForkAncestorStateCheck = func(slot uint64) { checks[slot]++ }

	conn := ouroboros.ConnectionId{}
	anchorBlock := models.Block{Slot: anchorPoint.Slot, Hash: anchorPoint.Hash}
	verify := func(snapshot *consensusSnapshot, count int) error {
		headers := make([]gledger.BlockHeader, 0, count)
		for _, b := range blocks[:count] {
			headers = append(headers, b.Header())
		}
		ls.storePeerForkPathPrefix(
			connIdKey(conn), anchorBlock, headers,
			snapshot.generation, snapshot.contextGeneration,
		)
		return ls.verifyPeerForkTargetEpochAncestors(
			conn, headers, epoch, epochNonce, snapshot,
		)
	}

	snapshot := ls.loadConsensusSnapshot()
	require.NoError(t, verify(snapshot, 2))
	require.Equal(t, map[uint64]int{2_200: 1}, checks)
	require.NoError(t, verify(snapshot, 3))
	require.Equal(
		t,
		map[uint64]int{2_200: 1, 2_300: 1},
		checks,
		"an ancestor already state-verified in this context must not be re-verified for a later header",
	)

	// A tip-only publication keeps the context, so nothing is re-verified.
	ls.publishSnapshotsLocked()
	same := ls.loadConsensusSnapshot()
	require.Greater(t, same.generation, snapshot.generation)
	require.Equal(t, snapshot.contextGeneration, same.contextGeneration)
	require.NoError(t, verify(same, 3))
	require.Equal(t, map[uint64]int{2_200: 1, 2_300: 1}, checks)

	// A new consensus context invalidates every verdict.
	next := *same
	next.generation++
	next.contextGeneration = next.generation
	require.NoError(t, verify(&next, 3))
	require.Equal(
		t,
		map[uint64]int{2_200: 2, 2_300: 2},
		checks,
		"a context change must re-run the state verification",
	)
}

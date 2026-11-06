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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// After a local rollback across an epoch boundary the published epoch cache
// ends at the anchor's epoch. A peer's winning fork whose headers reach into
// the next epoch must still be verifiable: its epoch and nonce follow from the
// fork's own ancestry, as in TestValidatePeerChainSelectionHeaderCryptoUsesForkNonce.
// Deferring it ("no cached epoch data") leaves the node unable to ever select
// the fork, because only applying the fork would extend the cache.
func TestValidatePeerChainSelectionHeaderCryptoAfterRollbackAcrossEpochBoundary(
	t *testing.T,
) {
	t.Parallel()

	cfg := newHighFreqShelleyGenesisCfg(t)
	cfg.ShelleyGenesisHash = hex.EncodeToString(bytes.Repeat([]byte{0x07}, 32))
	sourceNonce := bytes.Repeat([]byte{0x21}, lcommon.Blake2b256Size)
	anchor := createTestBlockWithNonceAtSlots(
		t, [32]byte{80}, sourceNonce, 10, 100,
		make([]byte, lcommon.Blake2b256Size), tamperNone,
	)
	anchorPoint := ocommon.NewPoint(
		anchor.block.SlotNumber(), anchor.block.Hash().Bytes(),
	)
	anchorNonce, err := eras.BabbageEraDesc.CalculateEtaVFunc(
		cfg, sourceNonce, anchor.block,
	)
	require.NoError(t, err)
	forkBlockNumber := anchor.block.BlockNumber() + 1
	forkHeader := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t, [32]byte{81}, sourceNonce, 300, 400, anchorPoint.Hash,
		&forkBlockNumber, tamperNone,
	)
	// The epoch is short enough that the fork lies past the candidate-nonce
	// cutoff, so the next epoch's nonce uses the epoch's own candidate.
	forkCandidate := sourceNonce

	const targetEpochStart = uint64(1_000)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.PrimaryChain().AddLocalBlock(context.Background(), anchor.block),
	)
	require.NoError(t, db.SetBlockNonce(
		anchorPoint.Hash, anchorPoint.Slot, anchorNonce, false, nil,
	))

	epoch5 := models.Epoch{
		EpochId:        5,
		StartSlot:      0,
		SlotLength:     1_000,
		LengthInSlots:  uint(targetEpochStart),
		EraId:          eras.BabbageEraDesc.Id,
		Nonce:          sourceNonce,
		EvolvingNonce:  sourceNonce,
		CandidateNonce: sourceNonce,
	}
	// The cache and current epoch end at epoch 5: epoch 6 was rolled back.
	ls := &LedgerState{
		db:           db,
		chain:        cm.PrimaryChain(),
		epochCache:   []models.Epoch{epoch5},
		currentEra:   eras.BabbageEraDesc,
		currentEpoch: epoch5,
		currentTip: ochainsync.Tip{
			Point:       anchorPoint,
			BlockNumber: anchor.block.BlockNumber(),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if !bytes.Equal(hash, forkHeader.block.Hash().Bytes()) {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			Point: ocommon.NewPoint(
				forkHeader.block.SlotNumber(),
				forkHeader.block.Hash().Bytes(),
			),
			BlockHeader: forkHeader.block.Header(),
			HeaderCryptoVerificationGeneration: ls.
				CurrentHeaderCryptoVerificationGeneration(),
		}, anchorPoint.Hash, true
	}
	ls.slotsPerKESPeriod.Store(anchor.slotsPerKesPeriod)
	ls.publishSnapshotsLocked()

	extraEntropy, err := ls.recordedExtraEntropyForEpoch(6, eras.BabbageEraDesc.Id)
	require.NoError(t, err)
	forkEpochNonce, err := assembleEpochNonce(
		forkCandidate, ls.epochCache[0].LastEpochBlockNonce, extraEntropy,
	)
	require.NoError(t, err)
	targetBlockNumber := forkHeader.block.BlockNumber() + 1
	target := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t, [32]byte{82}, forkEpochNonce, targetEpochStart,
		targetEpochStart+100, forkHeader.block.Hash().Bytes(),
		&targetBlockNumber, tamperNone,
	)
	seedBlockPoolRegistration(t, db, forkHeader.block)
	seedPoolStakeSnapshot(
		t, db, 4, ptrHash(forkHeader.block.IssuerVkey().Hash()), 1_000_000_000,
	)
	seedBlockPoolRegistration(t, db, target.block)
	seedPoolStakeSnapshot(
		t, db, 5, ptrHash(target.block.IssuerVkey().Hash()), 1_000_000_000,
	)

	err = ls.ValidatePeerChainSelectionHeaderCrypto(
		ouroboros.ConnectionId{},
		target.block.Header(),
	)
	require.NoError(t, err)
}

func ptrHash(h lcommon.Blake2b224) []byte { return h[:] }

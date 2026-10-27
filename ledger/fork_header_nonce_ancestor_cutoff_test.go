package ledger

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestPeerChainSelectionEpochNonceAncestorAfterCutoffWithoutStoredNonce
// covers a fork intersection at or after the candidate-nonce cutoff whose
// block has no stored nonce (e.g. Mithril-imported); the frozen candidate
// nonce from the epoch cache must be used instead of failing.
func TestPeerChainSelectionEpochNonceAncestorAfterCutoffWithoutStoredNonce(t *testing.T) {
	t.Parallel()

	cfg := newHighFreqShelleyGenesisCfg(t)
	sourceNonce := bytes.Repeat([]byte{0x21}, common.Blake2b256Size)

	// k=6, f=0.4 -> 4k/f = 60. Epoch length 75, cutoff = epochEnd - 60.
	// Epoch 5: [0, 75), cutoff = 15.
	// Ancestor at slot 20 (>= cutoff), no nonce in DB.
	// Fork header at slot 30, target header at slot 100 (epoch 6).

	const (
		epoch5Start  uint64 = 0
		epoch5End    uint64 = 75
		cutoffSlot   uint64 = 15
		ancestorSlot uint64 = 20 // >= cutoff, no nonce in DB
		forkSlot     uint64 = 30
		targetSlot   uint64 = 100 // epoch 6
	)

	// Create ancestor block (no nonce stored)
	ancestorBlock := createTestBlockWithNonceAtSlots(
		t,
		[32]byte{80},
		sourceNonce,
		ancestorSlot,
		ancestorSlot+1,
		make([]byte, common.Blake2b256Size), // no nonce
		tamperNone,
	)
	ancestorPoint := ocommon.NewPoint(
		ancestorBlock.block.SlotNumber(),
		ancestorBlock.block.Hash().Bytes(),
	)

	// Create fork header (child of ancestor)
	forkBlockNumber := ancestorBlock.block.BlockNumber() + 1
	forkHeader := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t,
		[32]byte{81},
		sourceNonce,
		forkSlot,
		forkSlot+1,
		ancestorPoint.Hash,
		&forkBlockNumber,
		tamperNone,
	)

	// Create target header (in epoch 6)
	targetEpochStart := epoch5End
	targetBlockNumber := forkHeader.block.BlockNumber() + 1
	targetHeader := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t,
		[32]byte{82},
		sourceNonce, // nonce doesn't matter, we're testing the fork path
		targetSlot,
		targetSlot+1,
		forkHeader.block.Hash().Bytes(),
		&targetBlockNumber,
		tamperNone,
	)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.PrimaryChain().AddLocalBlock(context.Background(), ancestorBlock.block),
	)

	// Note: we do NOT store a nonce for the ancestor block
	// This simulates a Mithril-imported block where only the snapshot tip has a nonce

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		epochCache: []models.Epoch{
			{
				EpochId:             5,
				StartSlot:           epoch5Start,
				SlotLength:          1_000,
				LengthInSlots:       uint(epoch5End - epoch5Start),
				EraId:               eras.BabbageEraDesc.Id,
				Nonce:               sourceNonce,
				EvolvingNonce:       sourceNonce,
				CandidateNonce:      sourceNonce,
				LastEpochBlockNonce: nil,
			},
			{
				EpochId:        6,
				StartSlot:      targetEpochStart,
				SlotLength:     1_000,
				LengthInSlots:  uint(epoch5End - epoch5Start),
				EraId:          eras.BabbageEraDesc.Id,
				Nonce:          bytes.Repeat([]byte{0xa5}, common.Blake2b256Size),
				CandidateNonce: bytes.Repeat([]byte{0xb6}, common.Blake2b256Size),
			},
		},
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       6,
			StartSlot:     targetEpochStart,
			LengthInSlots: uint(epoch5End - epoch5Start),
			EraId:         eras.BabbageEraDesc.Id,
			Nonce:         bytes.Repeat([]byte{0xa5}, common.Blake2b256Size),
		},
		currentTip: ochainsync.Tip{
			Point:       ancestorPoint,
			BlockNumber: ancestorBlock.block.BlockNumber(),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
		},
	}
	ls.slotsPerKESPeriod.Store(ancestorBlock.slotsPerKesPeriod)
	ls.publishSnapshotsLocked()

	// Verify ancestor has no nonce in DB
	storedNonce, err := ls.db.GetBlockNonce(ancestorPoint, nil)
	require.NoError(t, err)
	require.Empty(t, storedNonce, "ancestor should have no nonce in DB")

	// Set up peer header lookup to return the fork header when asked for ancestor's child
	peerLookup := func(peerHeader *testBlockResult) PeerHeaderLookupFunc {
		point := ocommon.NewPoint(
			peerHeader.block.SlotNumber(),
			peerHeader.block.Hash().Bytes(),
		)
		return func(
			_ ouroboros.ConnectionId,
			hash []byte,
		) (ChainsyncEvent, []byte, bool) {
			if !bytes.Equal(hash, point.Hash) {
				return ChainsyncEvent{}, nil, false
			}
			return ChainsyncEvent{
				Point: ocommon.NewPoint(
					point.Slot,
					point.Hash,
				),
				BlockHeader: peerHeader.block.Header(),
				HeaderCryptoVerificationGeneration: ls.
					CurrentHeaderCryptoVerificationGeneration(),
			}, ancestorPoint.Hash, true
		}
	}
	ls.config.PeerHeaderLookupFunc = peerLookup(forkHeader)

	seedBlockPoolRegistration(t, db, forkHeader.block)
	forkPool := forkHeader.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, forkPool[:], 1_000_000_000)

	nonce, _, _, err := ls.peerChainSelectionEpochNonce(
		ouroboros.ConnectionId{},
		targetHeader.block.Header(),
		ls.loadConsensusSnapshot(),
	)
	require.NoError(t, err)
	require.Len(t, nonce, common.Blake2b256Size)
}

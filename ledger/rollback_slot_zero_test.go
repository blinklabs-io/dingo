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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newSlotZeroRollbackLedger builds a ledger whose primary chain holds a block
// at slot 0 (of the given type) followed by a block at slot 20, with the tip
// at the slot-20 block. Both blocks have stored nonces.
func newSlotZeroRollbackLedger(
	t *testing.T,
	block0Type uint,
) (*LedgerState, ochainsync.Tip, []byte) {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	hash0 := testHashBytes("slot-zero-block")
	hash1 := testHashBytes("slot-twenty-block")
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(), []chain.RawBlock{
		{
			Slot: 0, Hash: hash0, BlockNumber: 0, Type: block0Type,
			Cbor: []byte{0x80},
		},
		{
			Slot: 20, Hash: hash1, BlockNumber: 1, Type: block0Type,
			PrevHash: hash0, Cbor: []byte{0x80},
		},
	}))
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	nonce0 := bytes.Repeat([]byte{0xa0}, 32)
	require.NoError(t, db.SetBlockNonce(hash0, 0, nonce0, true, nil))
	require.NoError(t, db.SetBlockNonce(
		hash1, 20, bytes.Repeat([]byte{0xa1}, 32), false, nil,
	))
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(20, hash1),
		BlockNumber: 1,
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip
	ls.currentTipBlockNonce = bytes.Repeat([]byte{0xa1}, 32)
	ls.publishSnapshotsLocked()
	return ls, ochainsync.Tip{
		Point: ocommon.NewPoint(0, hash0),
	}, nonce0
}

// A rollback to a real slot-0 block (Shelley-at-genesis networks such as
// Preview) must keep that block's nonce as the tip nonce; clearing it makes
// the next block seed its fold from the genesis hash.
func TestRollbackToSlotZeroBlockKeepsTipNonce(t *testing.T) {
	t.Parallel()

	ls, tip0, nonce0 := newSlotZeroRollbackLedger(
		t, conway.BlockTypeConway,
	)
	require.NoError(t, ls.rollback(context.Background(), tip0.Point))
	require.Equal(t, nonce0, ls.currentTipBlockNonce)
	require.Equal(t, tip0.Point, ls.currentTip.Point)
	require.Zero(t, ls.currentTip.BlockNumber)
}

// A rollback to true origin (empty hash) still clears the tip nonce.
func TestRollbackToOriginClearsTipNonce(t *testing.T) {
	t.Parallel()

	ls, _, _ := newSlotZeroRollbackLedger(t, conway.BlockTypeConway)
	require.NoError(t, ls.rollback(context.Background(), ocommon.NewPointOrigin()))
	require.Empty(t, ls.currentTipBlockNonce)
	require.Empty(t, ls.currentTip.Point.Hash)
}

// A rollback to a Byron slot-0 block carries no Praos nonce and must succeed
// with an empty tip nonce.
func TestRollbackToByronSlotZeroBlockHasNoTipNonce(t *testing.T) {
	t.Parallel()

	ls, tip0, _ := newSlotZeroRollbackLedger(t, byron.BlockTypeByronEbb)
	// Byron blocks store no nonce row.
	require.NoError(t, ls.db.DeleteBlockNoncesAfterPoint(
		ocommon.NewPointOrigin(), nil,
	))
	require.NoError(t, ls.rollback(context.Background(), tip0.Point))
	require.Empty(t, ls.currentTipBlockNonce)
	require.Equal(t, tip0.Point, ls.currentTip.Point)
}

// loadTip must read the nonce of a slot-0 block tip; only origin has none.
func TestLoadTipReadsNonceForSlotZeroBlock(t *testing.T) {
	t.Parallel()

	ls, tip0, nonce0 := newSlotZeroRollbackLedger(t, conway.BlockTypeConway)
	require.NoError(t, ls.db.SetTip(tip0, nil))
	ls.currentTipBlockNonce = nil
	require.NoError(t, ls.loadTip(context.Background()))
	require.Equal(t, nonce0, ls.currentTipBlockNonce)
}

// The rollback intent written for a slot-0 block point must load back as
// that point rather than as an invalid origin.
func TestRollbackIntentRoundTripsSlotZeroBlockPoint(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	point := ocommon.NewPoint(0, testHashBytes("slot-zero-block"))
	require.NoError(t, persistRollbackIntent(db, point, []models.Block{
		{Slot: 20, Hash: testHashBytes("b20"), Cbor: []byte{0x80}},
	}))
	got, _, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, point, got)
}

// Heal must not treat a slot-0 block tip as origin: a non-Byron slot-0 tip
// with no nonce and no checkpoint is unrepairable and must fail loudly.
func TestHealTruncateGapBlockNonces_SlotZeroBlockTipIsNotOrigin(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	hash0 := testHashBytes("slot-zero-block")
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: 0, Hash: hash0, Cbor: []byte{0x80}, Number: 0,
		Type: conway.BlockTypeConway,
	}, nil))
	ls := newTruncateGapHealTestLedgerState(t, db, 0, hash0, nil)
	ls.config.CardanoNodeConfig = newConwayBootstrapStabilityCfg(t)
	require.Error(t, ls.healTruncateGapBlockNonces(t.Context()))
}

func TestHealTruncateGapBlockNonces_OriginTipIsNoOp(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	ls := newTruncateGapHealTestLedgerState(t, db, 0, nil, nil)
	require.NoError(t, ls.healTruncateGapBlockNonces(t.Context()))
}

func TestHealTruncateGapBlockNonces_ByronSlotZeroTipIsNoOp(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	hash0 := testHashBytes("byron-slot-zero")
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: 0, Hash: hash0, Cbor: []byte{0x80}, Number: 0,
		Type: byron.BlockTypeByronEbb,
	}, nil))
	ls := newTruncateGapHealTestLedgerState(t, db, 0, hash0, nil)
	require.NoError(t, ls.healTruncateGapBlockNonces(t.Context()))
}

// Recovery of an intent at a slot-0 block that is no longer on the primary
// chain must retire the intent without rewinding, as for any other off-chain
// point, rather than treating the point as origin.
func TestRecoverRollbackIntentSlotZeroBlockOffPrimaryChain(t *testing.T) {
	t.Parallel()

	ls, _, _ := newSlotZeroRollbackLedger(t, conway.BlockTypeConway)
	tipBefore := ls.currentTip
	forked := ocommon.NewPoint(0, testHashBytes("forked-slot-zero-block"))
	require.NoError(t, persistRollbackIntent(ls.db, forked, nil))
	require.NoError(t, ls.recoverRollbackIntent(context.Background()))
	_, _, pending, err := loadRollbackIntent(ls.db)
	require.NoError(t, err)
	require.False(t, pending)
	require.Equal(t, tipBefore, ls.currentTip)
}

// Recovery of an intent at a slot-0 block on the primary chain completes the
// rollback to that block and keeps its nonce.
func TestRecoverRollbackIntentSlotZeroBlockOnPrimaryChain(t *testing.T) {
	t.Parallel()

	ls, tip0, nonce0 := newSlotZeroRollbackLedger(t, conway.BlockTypeConway)
	require.NoError(t, persistRollbackIntent(ls.db, tip0.Point, nil))
	require.NoError(t, ls.recoverRollbackIntent(context.Background()))
	_, _, pending, err := loadRollbackIntent(ls.db)
	require.NoError(t, err)
	require.False(t, pending)
	require.Equal(t, tip0.Point, ls.currentTip.Point)
	require.Equal(t, nonce0, ls.currentTipBlockNonce)
}

// Applying a slot-0 Conway block and its successor, rolling back to the
// slot-0 block, and re-applying the successor must persist the successor's
// nonce folded from the slot-0 block's nonce, not from the genesis hash.
func TestRollbackToSlotZeroBlockReappliesFromItsNonce(t *testing.T) {
	t.Parallel()

	blocks, err := fixtures.GenerateConwayChain(
		0, lcommon.Blake2b256{}, 0, 20, 2,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	block0, block1 := blocks[0], blocks[1]
	require.Zero(t, block0.SlotNumber())

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	rawBlocks := make([]chain.RawBlock, 0, len(blocks))
	for _, blk := range blocks {
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        blk.SlotNumber(),
			Hash:        blk.Hash().Bytes(),
			BlockNumber: blk.BlockNumber(),
			Type:        conway.BlockTypeConway,
			PrevHash:    blk.PrevHash().Bytes(),
			Cbor:        blk.Cbor(),
		})
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(), rawBlocks))

	pparams := epochBoundaryBenchPParams()
	pparams.ProtocolVersion.Major = conway.MaxProtocolVersionConway
	pparams.MaxBlockBodySize = 2_000_000
	pparams.MaxBlockHeaderSize = 100_000
	epoch0 := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: 1_000,
		EraId:         eras.ConwayEraDesc.Id,
	}
	require.NoError(t, db.SetEpoch(
		epoch0.StartSlot, epoch0.EpochId, nil, nil, nil, nil,
		epoch0.EraId, epoch0.SlotLength, epoch0.LengthInSlots, nil,
	))
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesisHash = strings.Repeat("42", 32)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     nodeConfig,
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:          prometheus.NewRegistry(),
		ManualBlockProcessing: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = pparams
	ls.currentEpoch = epoch0
	ls.epochCache = []models.Epoch{epoch0}
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nil
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))

	apply := func(blks ...gledger.Block) {
		t.Helper()
		results := make(chan readChainResult, 1)
		results <- readChainResult{blocks: blks}
		close(results)
		require.NoError(t, ls.ledgerProcessBlocksFromSource(
			context.Background(),
			results,
		))
	}
	point0 := ocommon.NewPoint(block0.SlotNumber(), block0.Hash().Bytes())
	point1 := ocommon.NewPoint(block1.SlotNumber(), block1.Hash().Bytes())

	apply(block0, block1)
	require.Equal(t, point1, ls.currentTip.Point)
	nonce0, err := db.GetBlockNonce(point0, nil)
	require.NoError(t, err)
	require.NotEmpty(t, nonce0)

	require.NoError(t, ls.rollback(context.Background(), point0))
	require.Equal(t, point0, ls.currentTip.Point)

	apply(block1)
	require.Equal(t, point1, ls.currentTip.Point)

	want, err := eras.CalculateEtaVConway(nodeConfig, nonce0, block1)
	require.NoError(t, err)
	fromGenesis, err := eras.CalculateEtaVConway(nodeConfig, nil, block1)
	require.NoError(t, err)
	require.NotEqual(t, fromGenesis, want)
	got, err := db.GetBlockNonce(point1, nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

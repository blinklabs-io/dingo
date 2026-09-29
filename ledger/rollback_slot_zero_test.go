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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	hash0 := testHashBytes("slot-zero-block")
	hash1 := testHashBytes("slot-twenty-block")
	require.NoError(t, cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
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
	require.NoError(t, ls.rollback(tip0.Point))
	require.Equal(t, nonce0, ls.currentTipBlockNonce)
	require.Equal(t, tip0.Point, ls.currentTip.Point)
	require.Zero(t, ls.currentTip.BlockNumber)
}

// A rollback to true origin (empty hash) still clears the tip nonce.
func TestRollbackToOriginClearsTipNonce(t *testing.T) {
	t.Parallel()

	ls, _, _ := newSlotZeroRollbackLedger(t, conway.BlockTypeConway)
	require.NoError(t, ls.rollback(ocommon.NewPointOrigin()))
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
	require.NoError(t, ls.rollback(tip0.Point))
	require.Empty(t, ls.currentTipBlockNonce)
	require.Equal(t, tip0.Point, ls.currentTip.Point)
}

// loadTip must read the nonce of a slot-0 block tip; only origin has none.
func TestLoadTipReadsNonceForSlotZeroBlock(t *testing.T) {
	t.Parallel()

	ls, tip0, nonce0 := newSlotZeroRollbackLedger(t, conway.BlockTypeConway)
	require.NoError(t, ls.db.SetTip(tip0, nil))
	ls.currentTipBlockNonce = nil
	require.NoError(t, ls.loadTip())
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

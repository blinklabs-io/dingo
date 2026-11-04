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
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type verifiedAncestryFixture struct {
	ls     *LedgerState
	blocks []gledger.Block
	raw    map[string]gledger.BlockHeader
}

func ancestryTip(block gledger.Block) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.NewPoint(
			block.SlotNumber(),
			block.Hash().Bytes(),
		), BlockNumber: block.BlockNumber(),
	}
}

func newVerifiedAncestryFixture(
	t *testing.T,
	tamper tamperOption,
	counters []uint64,
) verifiedAncestryFixture {
	t.Helper()
	cfg := newHighFreqShelleyGenesisCfg(t)
	nonce := bytes.Repeat([]byte{0x51}, 32)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	manager, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		manager.SetLedger(testSecurityParamLedger{securityParam: 10}),
	)
	f := verifiedAncestryFixture{raw: make(map[string]gledger.BlockHeader)}
	previous := make([]byte, 32)
	var slotsPerKES uint64
	for index := range 5 {
		number := uint64(index + 1)
		corruption := tamperNone
		if index == 3 {
			corruption = tamper
		}
		sequence := uint64(0)
		if len(counters) > index {
			sequence = counters[index]
		}
		block := createTestBlockWithNonceAtSlotsAndBlockNumber(
			t, [32]byte{121}, nonce, uint64(10+index*100), uint64(30+index*100),
			previous, &number, corruption, sequence,
		)
		slotsPerKES = block.slotsPerKesPeriod
		f.blocks = append(f.blocks, block.block)
		f.raw[string(block.block.Hash().Bytes())] = block.block.Header()
		previous = block.block.Hash().Bytes()
		if index < 3 {
			require.NoError(
				t,
				manager.PrimaryChain().AddLocalBlock(t.Context(), block.block),
			)
		}
	}
	seedBlockPoolRegistration(t, db, f.blocks[0])
	pool := f.blocks[0].IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, pool[:], 1_000_000_000)
	epoch := models.Epoch{
		EpochId: 5, StartSlot: 0, SlotLength: 1_000, LengthInSlots: 2_000,
		EraId: eras.BabbageEraDesc.Id, Nonce: nonce, EvolvingNonce: nonce, CandidateNonce: nonce,
	}
	f.ls = &LedgerState{
		db: db, chain: manager.PrimaryChain(), currentTip: ancestryTip(f.blocks[2]),
		currentEpoch: epoch, currentEra: eras.BabbageEraDesc, epochCache: []models.Epoch{epoch},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	f.ls.slotsPerKESPeriod.Store(slotsPerKES)
	f.ls.config.PeerHeaderLookupFunc = func(
		conn ouroboros.ConnectionId, hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		h, found := f.raw[string(hash)]
		if !found {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			ConnectionId: conn, Point: ocommon.NewPoint(h.SlotNumber(), h.Hash().Bytes()), BlockHeader: h,
		}, h.PrevHash().Bytes(), true
	}
	f.ls.publishSnapshotsLocked()
	return f
}

func TestVerifiedPeerAncestryRepairsAppliedAndUnstampedPrefix(t *testing.T) {
	t.Parallel()
	f := newVerifiedAncestryFixture(t, tamperNone, nil)
	var checked []uint64
	f.ls.onPeerForkAncestorStateCheck = func(slot uint64) { checked = append(checked, slot) }
	proof, err := f.ls.VerifyPeerHeaderAncestry(
		t.Context(),
		ouroboros.ConnectionId{},
		f.blocks[4].Header(),
		ancestryTip(f.blocks[0]),
		5,
	)
	require.NoError(t, err)
	var expected []ochainsync.Tip
	for _, block := range f.blocks {
		expected = append(expected, ancestryTip(block))
	}
	require.Equal(
		t,
		expected,
		proof.Points,
		"a complete proof must bridge old admission through applied and previously unstamped ancestors",
	)
	require.Equal(t, []uint64{f.blocks[3].SlotNumber()}, checked,
		"an unstamped skipped ancestor must pass its state check")
	release, current := f.ls.PinHeaderCryptoVerificationGeneration(
		proof.Generation,
	)
	require.True(t, current)
	require.True(t, proof.Current())
	release()
	f.ls.publishSnapshotsLocked()
	require.False(
		t,
		proof.Current(),
		"a publication must invalidate the old path certificate",
	)
}

func TestVerifiedPeerAncestryRejectsBadAncestorCryptoAndCounters(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"bad ancestor signature", "counter regression"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tamper := tamperNone
			var counters []uint64
			if name == "bad ancestor signature" {
				tamper = tamperKESSig
			} else {
				counters = []uint64{0, 0, 0, 2, 1}
			}
			f := newVerifiedAncestryFixture(t, tamper, counters)
			proof, err := f.ls.VerifyPeerHeaderAncestry(
				t.Context(),
				ouroboros.ConnectionId{},
				f.blocks[4].Header(),
				ancestryTip(f.blocks[0]),
				5,
			)
			require.Error(
				t,
				err,
				"valid target crypto must not certify an invalid skipped ancestor",
			)
			require.Empty(t, proof.Points)
			require.False(t, proof.Current())
			if name == "bad ancestor signature" {
				require.ErrorContains(t, err, "signature")
			} else {
				require.ErrorContains(t, err, "opcert")
			}
		})
	}
}

func TestVerifiedPeerAncestryRejectsMissingAndMismatchedBase(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"missing raw parent", "different base hash", "wrong base height", "bounded length", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newVerifiedAncestryFixture(t, tamperNone, nil)
			base := ancestryTip(f.blocks[0])
			limit := 5
			ctx := t.Context()
			switch name {
			case "missing raw parent":
				delete(f.raw, string(f.blocks[3].Hash().Bytes()))
			case "different base hash":
				base.Point.Hash = bytes.Clone(base.Point.Hash)
				base.Point.Hash[0] ^= 0xff
			case "wrong base height":
				base.BlockNumber++
			case "bounded length":
				limit = 4
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			proof, err := f.ls.VerifyPeerHeaderAncestry(
				ctx,
				ouroboros.ConnectionId{},
				f.blocks[4].Header(),
				base,
				limit,
			)
			require.Error(t, err)
			require.Empty(
				t,
				proof.Points,
				"a failed path must expose no partial certificate",
			)
		})
	}
}

func TestVerifiedPeerAncestryFencesUnpublishedWithdrawal(t *testing.T) {
	t.Parallel()
	f := newVerifiedAncestryFixture(t, tamperNone, nil)
	proof, err := f.ls.VerifyPeerHeaderAncestry(
		t.Context(),
		ouroboros.ConnectionId{},
		f.blocks[4].Header(),
		ancestryTip(f.blocks[0]),
		5,
	)
	require.NoError(t, err)
	require.True(t, proof.Current())
	snapshot := f.ls.loadTipSnapshot()
	require.NoError(
		t,
		f.ls.chain.Rollback(t.Context(), ancestryTip(f.blocks[0]).Point),
	)
	require.Same(
		t,
		snapshot,
		f.ls.loadTipSnapshot(),
		"the ledger has not published the rollback yet",
	)
	require.False(
		t,
		proof.Current(),
		"raw chain withdrawal must invalidate the proof before publication",
	)
}

func TestVerifiedPeerAncestryKeepsBaseOnVerifiedFork(t *testing.T) {
	t.Parallel()
	f := newVerifiedAncestryFixture(t, tamperNone, nil)
	proof, err := f.ls.VerifyPeerHeaderAncestry(
		t.Context(),
		ouroboros.ConnectionId{},
		f.blocks[4].Header(),
		ancestryTip(f.blocks[3]),
		2,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		[]ochainsync.Tip{ancestryTip(f.blocks[3]), ancestryTip(f.blocks[4])},
		proof.Points,
		"a base above the applied anchor must come from the exact authenticated suffix",
	)
	wrong := ancestryTip(f.blocks[3])
	wrong.Point.Hash = bytes.Repeat([]byte{0xff}, 32)
	proof, err = f.ls.VerifyPeerHeaderAncestry(
		t.Context(),
		ouroboros.ConnectionId{},
		f.blocks[4].Header(),
		wrong,
		2,
	)
	require.Error(t, err)
	require.Empty(t, proof.Points)
}

func TestVerifiedPeerAncestryCancelsDuringAncestorVerification(t *testing.T) {
	t.Parallel()
	f := newVerifiedAncestryFixture(t, tamperNone, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	checked := false
	f.ls.onPeerForkAncestorStateCheck = func(uint64) {
		checked = true
		cancel()
	}
	proof, err := f.ls.VerifyPeerHeaderAncestry(ctx, ouroboros.ConnectionId{},
		f.blocks[4].Header(), ancestryTip(f.blocks[0]), 5)
	require.True(
		t,
		checked,
		"cancellation must occur inside the strict ancestor verifier",
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(
		t,
		proof.Points,
		"an interrupted verifier must never expose a partial certificate",
	)
}

func TestVerifiedPeerAncestryCanceledCryptoDoesNotInspectHeader(t *testing.T) {
	t.Parallel()
	f := newVerifiedAncestryFixture(t, tamperKESSig, nil)
	block := headerOnlyBlock{header: f.blocks[3].Header(), peerRelative: true}
	err := f.ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
		t.Context(), block, f.ls.currentEpoch, f.ls.currentEpoch.Nonce,
	)
	require.ErrorContains(
		t,
		err,
		"signature",
		"the control must exercise real failing crypto",
	)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = f.ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
		ctx, block, f.ls.currentEpoch, f.ls.currentEpoch.Nonce,
	)
	require.ErrorIs(t, err, context.Canceled,
		"a canceled repair must stop before inspecting another signed header")
}

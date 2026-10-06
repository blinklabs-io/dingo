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

package node

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ppupSourceEpoch = uint64(300)
	ppupTargetEpoch = uint64(301)
	ppupMaxTxSize   = uint(20000)
)

// ppupGenesisKeys are five distinct genesis delegates, the preview update
// quorum, all proposing the same update.
func ppupGenesisKeys() []lcommon.Blake2b224 {
	keys := make([]lcommon.Blake2b224, 5)
	for i := range keys {
		keys[i] = lcommon.NewBlake2b224(bytes.Repeat([]byte{byte(0xc0 + i)}, 28))
	}
	return keys
}

func ppupUpdateCbor(t *testing.T) []byte {
	t.Helper()
	// Key 3 is maxTxSize in every classic update map.
	updateCbor, err := cbor.Encode(map[uint]uint{3: ppupMaxTxSize})
	require.NoError(t, err)
	return updateCbor
}

func ppupHeaderBody(slot, major uint64) shelley.ShelleyBlockHeaderBody {
	return shelley.ShelleyBlockHeaderBody{
		BlockNumber:       1,
		Slot:              slot,
		VrfKey:            make([]byte, 32),
		NonceVrf:          lcommon.VrfResult{Output: make([]byte, 64), Proof: make([]byte, 80)},
		LeaderVrf:         lcommon.VrfResult{Output: make([]byte, 64), Proof: make([]byte, 80)},
		OpCertHotVkey:     make([]byte, 32),
		OpCertSignature:   make([]byte, 64),
		ProtoMajorVersion: major,
	}
}

// classicUpdateBlock builds a block of a classic era whose one transaction
// carries the update proposals, so replay stores them the way it stores any
// historical proposal. The body is written as raw CBOR because the update
// structs encode every absent field as null, which the decoder rejects.
func classicUpdateBlock(
	t *testing.T,
	slot, epoch uint64,
	protoMajor uint64,
	alonzoBody bool,
	encodeHeader func(shelley.ShelleyBlockHeader) ([]byte, error),
	decode func([]byte) (gledger.Block, error),
) models.Block {
	t.Helper()
	updates := make(map[lcommon.Blake2b224]cbor.RawMessage)
	for _, key := range ppupGenesisKeys() {
		updates[key] = ppupUpdateCbor(t)
	}
	txBody, err := cbor.Encode(map[uint]any{
		0: []any{[]any{bytes.Repeat([]byte{0xb1}, 32), uint(0)}},
		1: []any{},
		2: uint(0),
		3: slot + 1000,
		6: []any{updates, epoch},
	})
	require.NoError(t, err)
	encode := func(v any) []byte {
		t.Helper()
		out, err := cbor.Encode(v)
		require.NoError(t, err)
		return out
	}
	bodyParts := [][]byte{
		encode([]cbor.RawMessage{txBody}),
		encode([]any{map[uint]any{}}),
		encode(map[uint]any{}),
	}
	if alonzoBody {
		bodyParts = append(bodyParts, encode([]uint{}))
	}
	var bodySize uint64
	for _, part := range bodyParts {
		bodySize += uint64(len(part))
	}
	headerBody := ppupHeaderBody(slot, protoMajor)
	headerBody.BlockBodyHash = mockfixtures.ComputeBlockBodyHash(bodyParts...)
	headerBody.BlockBodySize = bodySize
	headerCbor, err := encodeHeader(shelley.ShelleyBlockHeader{
		Body:      headerBody,
		Signature: make([]byte, 64),
	})
	require.NoError(t, err)
	parts := []cbor.RawMessage{headerCbor}
	for _, part := range bodyParts {
		parts = append(parts, part)
	}
	blockCbor := encode(parts)
	decoded, err := decode(blockCbor)
	require.NoError(t, err)
	_, proposals := decoded.Transactions()[0].ProtocolParameterUpdates()
	require.Len(t, proposals, len(ppupGenesisKeys()))
	return models.Block{
		Slot:   slot,
		Hash:   decoded.Hash().Bytes(),
		Number: 1,
		Cbor:   blockCbor,
		Type:   uint(decoded.Type()),
	}
}

func maryUpdateBlock(t *testing.T, slot, epoch uint64) models.Block {
	t.Helper()
	return classicUpdateBlock(
		t, slot, epoch, mary.MinProtocolVersionMary, false,
		func(header shelley.ShelleyBlockHeader) ([]byte, error) {
			return cbor.Encode(&mary.MaryBlockHeader{ShelleyBlockHeader: header})
		},
		func(data []byte) (gledger.Block, error) {
			return mary.NewMaryBlockFromCbor(data)
		},
	)
}

func alonzoUpdateBlock(t *testing.T, slot, epoch uint64) models.Block {
	t.Helper()
	return classicUpdateBlock(
		t, slot, epoch, alonzo.MinProtocolVersionAlonzo, true,
		func(header shelley.ShelleyBlockHeader) ([]byte, error) {
			return cbor.Encode(&alonzo.AlonzoBlockHeader{ShelleyBlockHeader: header})
		},
		func(data []byte) (gledger.Block, error) {
			return alonzo.NewAlonzoBlockFromCbor(data)
		},
	)
}

// TestRun_EnactsPendingUpdateBeforeHardFork covers the classic update system
// at an era boundary. The EPOCH rule enacts the proposals agreed for the
// boundary (UPEC/NEWPP, Cardano.Ledger.Shelley.Rules.Upec and Newpp) before
// the hard-fork combinator translates the ledger state into the new era, so
// the new era's parameters are the translation of the updated ones. Dingo's
// live rollover follows that order (ledger/chainsync.go epoch-rollover steps
// 4 and 9: ComputeAndApplyPParamUpdates, then transitionToEraFrom).
//
// Both shapes a backfill meets are covered: proposals already stored (a
// resumed run) and proposals carried by a replayed block of the submission
// epoch (a fresh run, where nothing is stored until replay writes it).
func TestRun_EnactsPendingUpdateBeforeHardFork(t *testing.T) {
	t.Parallel()
	testRunEnactsPendingUpdateBeforeHardFork(t, newTestDB)
}

func testRunEnactsPendingUpdateBeforeHardFork(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	for _, boundary := range []struct {
		name        string
		source      eras.EraDesc
		target      eras.EraDesc
		updateBlock func(*testing.T, uint64, uint64) models.Block
		targetBlock func(uint64) ([]gledger.Block, error)
		maxTxSize   func(lcommon.ProtocolParameters) (uint, bool)
	}{
		{
			name:        "mary to alonzo",
			source:      eras.MaryEraDesc,
			target:      eras.AlonzoEraDesc,
			updateBlock: maryUpdateBlock,
			targetBlock: func(slot uint64) ([]gledger.Block, error) {
				return mockfixtures.GenerateAlonzoChain(1, lcommon.Blake2b256{}, slot, 1, 3)
			},
			maxTxSize: func(pp lcommon.ProtocolParameters) (uint, bool) {
				p, ok := pp.(*alonzo.AlonzoProtocolParameters)
				if !ok {
					return 0, false
				}
				return p.MaxTxSize, true
			},
		},
		{
			name:        "alonzo to babbage",
			source:      eras.AlonzoEraDesc,
			target:      eras.BabbageEraDesc,
			updateBlock: alonzoUpdateBlock,
			targetBlock: func(slot uint64) ([]gledger.Block, error) {
				return mockfixtures.GenerateBabbageChain(1, lcommon.Blake2b256{}, slot, 1, 3)
			},
			maxTxSize: func(pp lcommon.ProtocolParameters) (uint, bool) {
				p, ok := pp.(*babbage.BabbageProtocolParameters)
				if !ok {
					return 0, false
				}
				return p.MaxTxSize, true
			},
		},
	} {
		for _, replayed := range []bool{false, true} {
			name := boundary.name + "/stored proposals"
			if replayed {
				name = boundary.name + "/replayed proposals"
			}
			// Sequential: on MySQL, concurrent fresh databases contend for
			// the server-wide migration lock.
			t.Run(name, func(t *testing.T) {
				db := newDB(t)
				require.NoError(t, db.SetEpoch(
					0, ppupSourceEpoch, nil, nil, nil, nil,
					boundary.source.Id, 1000, 10, nil,
				))
				require.NoError(t, db.SetEpoch(
					10, ppupTargetEpoch, nil, nil, nil, nil,
					boundary.target.Id, 1000, 10, nil,
				))
				if replayed {
					require.NoError(t, db.BlockCreate(
						boundary.updateBlock(t, 5, ppupSourceEpoch),
						nil,
					))
				} else {
					for _, key := range ppupGenesisKeys() {
						require.NoError(t, db.SetPParamUpdate(
							key.Bytes(), ppupUpdateCbor(t), 5, ppupSourceEpoch, nil,
						))
					}
				}
				targetBlocks, err := boundary.targetBlock(10)
				require.NoError(t, err)
				for _, block := range targetBlocks {
					require.NoError(t, db.BlockCreate(models.Block{
						Slot:   block.SlotNumber(),
						Hash:   block.Hash().Bytes(),
						Number: block.BlockNumber(),
						Cbor:   block.Cbor(),
						Type:   uint(block.Type()),
					}, nil))
				}
				require.NoError(t, db.SetSyncState("mithril_ledger_slot", "12", nil))

				bf := NewBackfill(
					db,
					nodeCfg,
					slog.New(slog.NewTextHandler(io.Discard, nil)),
				)
				bf.DisableNonceComputation()
				require.NoError(t, bf.Run(context.Background()))
				stored, err := db.Metadata().GetPParamUpdates(ppupSourceEpoch, nil)
				require.NoError(t, err)
				require.Len(t, stored, len(ppupGenesisKeys()),
					"the proposals were not stored")

				sourcePP, err := db.GetPParams(
					ppupSourceEpoch,
					boundary.source.Id,
					boundary.source.DecodePParamsFunc,
					nil,
				)
				require.NoError(t, err)
				require.NotNil(t, sourcePP)
				update, err := boundary.source.DecodePParamsUpdateFunc(
					ppupUpdateCbor(t),
				)
				require.NoError(t, err)
				updated, err := boundary.source.PParamsUpdateFunc(sourcePP, update)
				require.NoError(t, err)
				live, err := boundary.target.HardForkFunc(nodeCfg, updated)
				require.NoError(t, err)
				liveCbor, err := cbor.Encode(&live)
				require.NoError(t, err)

				got, err := db.GetPParams(
					ppupTargetEpoch,
					boundary.target.Id,
					boundary.target.DecodePParamsFunc,
					nil,
				)
				require.NoError(t, err)
				require.NotNil(t, got)
				maxTxSize, ok := boundary.maxTxSize(got)
				require.True(t, ok, "target parameters are %T", got)
				assert.Equal(t, ppupMaxTxSize, maxTxSize,
					"pending update was not enacted before the hard fork")
				gotCbor, err := cbor.Encode(&got)
				require.NoError(t, err)
				assert.Equal(t, liveCbor, gotCbor,
					"target parameters differ from the live boundary result")
			})
		}
	}
}

// TestRun_RederivesPParamsBelowTheAnchor covers a row an earlier backfill
// derived for an era-transition epoch without the update enacted at it, as a
// build that resolved every epoch before replay stored any proposal did. Only
// the anchor epoch and the one before carry imported rows, so an older row is
// derived again from the stored proposals rather than kept.
func TestRun_RederivesPParamsBelowTheAnchor(t *testing.T) {
	t.Parallel()
	testRunRederivesPParamsBelowTheAnchor(t, newTestDB)
}

func testRunRederivesPParamsBelowTheAnchor(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	db := newDB(t)
	for i, eraID := range []uint{
		eras.MaryEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		eras.AlonzoEraDesc.Id,
	} {
		require.NoError(t, db.SetEpoch(
			uint64(i)*10, ppupSourceEpoch+uint64(i), nil, nil, nil, nil,
			eraID, 1000, 10, nil,
		))
	}
	for _, key := range ppupGenesisKeys() {
		require.NoError(t, db.SetPParamUpdate(
			key.Bytes(), ppupUpdateCbor(t), 5, ppupSourceEpoch, nil,
		))
	}
	bootstrap := NewBackfill(db, nodeCfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, bootstrap.bootstrapEraChain(eras.MaryEraDesc.Id))
	stale, err := eras.AlonzoEraDesc.HardForkFunc(nodeCfg, bootstrap.currentPParams)
	require.NoError(t, err)
	staleCbor, err := cbor.Encode(&stale)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		staleCbor, 10, ppupTargetEpoch, eras.AlonzoEraDesc.Id, nil,
	))
	for _, slot := range []uint64{10, 30} {
		blocks, err := mockfixtures.GenerateAlonzoChain(1, lcommon.Blake2b256{}, slot, 1, 3)
		require.NoError(t, err)
		for _, block := range blocks {
			require.NoError(t, db.BlockCreate(models.Block{
				Slot:   block.SlotNumber(),
				Hash:   block.Hash().Bytes(),
				Number: block.BlockNumber(),
				Cbor:   block.Cbor(),
				Type:   uint(block.Type()),
			}, nil))
		}
	}
	require.NoError(t, db.SetSyncState("mithril_ledger_slot", "32", nil))

	bf := NewBackfill(db, nodeCfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bf.DisableNonceComputation()
	require.NoError(t, bf.Run(context.Background()))

	got, err := db.GetPParams(
		ppupTargetEpoch,
		eras.AlonzoEraDesc.Id,
		eras.AlonzoEraDesc.DecodePParamsFunc,
		nil,
	)
	require.NoError(t, err)
	alonzoPP, ok := got.(*alonzo.AlonzoProtocolParameters)
	require.True(t, ok, "target parameters are %T", got)
	assert.Equal(t, ppupMaxTxSize, alonzoPP.MaxTxSize,
		"stale derived parameters were kept below the anchor")
}

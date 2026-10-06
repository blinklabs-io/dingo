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

package forging

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	dingoversion "github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testHash32(b byte) []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = b
	}
	return h
}

// newBlockContextTestBuilder returns a builder whose live chain tip is the
// contested block: slot contestedSlot, block number contestedBlockNumber.
func newBlockContextTestBuilder(
	t *testing.T,
	tip ochainsync.Tip,
) *DefaultBlockBuilder {
	t.Helper()
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{
			pparams: &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: 62000000,
					Steps:  20000000000,
				},
			},
		},
		ChainTip:    &mockChainTip{tip: tip},
		EpochNonce:  &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)},
		Credentials: setupTestCredentials(t),
	})
	require.NoError(t, err)
	return builder
}

type alternativeRelationChainTip struct {
	tip         ochainsync.Tip
	predecessor ocommon.Point
}

func (m *alternativeRelationChainTip) Tip() ochainsync.Tip {
	return m.tip
}

func (m *alternativeRelationChainTip) TipRelation(
	point ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	switch {
	case pointsEqual(point, m.tip.Point):
		return m.tip, 0, true, nil
	case pointsEqual(point, m.predecessor):
		return m.tip, 1, true, nil
	default:
		return m.tip, 0, false, nil
	}
}

type alternativeAppliedTipValidator struct {
	mockTxValidator
	tip           ochainsync.Tip
	securityParam int
}

func (v *alternativeAppliedTipValidator) ForgeTipSnapshot() (
	ochainsync.Tip,
	int,
) {
	return v.tip, v.securityParam
}

func configureAlternativeTipRelation(
	builder *DefaultBlockBuilder,
	rival ochainsync.Tip,
	parent ocommon.Point,
) {
	builder.chainTip = &alternativeRelationChainTip{
		tip:         rival,
		predecessor: parent,
	}
	builder.txValidator = &alternativeAppliedTipValidator{
		tip:           rival,
		securityParam: 2,
	}
}

// TestBuildBlockOnContextForgesAnAlternativeToTheTip is the builder half of the
// equal-slot alternative. The live tip is a rival block at the slot being
// forged; the explicit context names the rival's predecessor as parent and the
// rival's own block number, so the two blocks are siblings that chain selection
// arbitrates between. This is ouroboros-consensus mkCurrentBlockContext's EQ
// branch: "forge an alternative to @hdr@: same block no and same predecessor".
func TestBuildBlockOnContextForgesAnAlternativeToTheTip(t *testing.T) {
	const (
		contestedSlot   = uint64(1000)
		parentSlot      = uint64(999)
		rivalBlockNumbr = uint64(100)
	)
	rivalHash := testHash32(0xAA)
	parentHash := testHash32(0xBB)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: rivalHash},
		BlockNumber: rivalBlockNumbr,
	}
	builder := newBlockContextTestBuilder(t, rival)
	parent := ocommon.Point{Slot: parentSlot, Hash: parentHash}
	configureAlternativeTipRelation(builder, rival, parent)

	block, blockCbor, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{
			Parent:      parent,
			BlockNumber: rivalBlockNumbr,
			Rival:       rival,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, block)
	assert.NotEmpty(t, blockCbor)

	assert.Equal(t, contestedSlot, block.SlotNumber())
	// Same block number as the rival, not the rival's plus one: the
	// alternative competes with it, it does not extend it.
	assert.Equal(t, rivalBlockNumbr, block.BlockNumber())
	assert.Equal(
		t,
		parentHash,
		block.PrevHash().Bytes(),
		"alternative must name the rival's predecessor as parent",
	)
	assert.NotEqual(
		t,
		rivalHash,
		block.PrevHash().Bytes(),
		"alternative must not name the rival itself as parent",
	)
}

func TestBuildBlockOnContextRejectsNonPredecessorParent(t *testing.T) {
	const (
		contestedSlot   = uint64(1000)
		rivalBlockNumbr = uint64(100)
	)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: testHash32(0xAA)},
		BlockNumber: rivalBlockNumbr,
	}
	predecessor := ocommon.Point{Slot: 999, Hash: testHash32(0xBB)}
	builder := newBlockContextTestBuilder(t, rival)
	configureAlternativeTipRelation(builder, rival, predecessor)

	block, blockCbor, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{
			Parent:      ocommon.Point{Slot: 998, Hash: testHash32(0xCC)},
			BlockNumber: rivalBlockNumbr,
			Rival:       rival,
		},
	)
	require.ErrorContains(
		t,
		err,
		"alternative forge parent is not the direct chain-tip predecessor",
	)
	assert.Nil(t, block)
	assert.Nil(t, blockCbor)
}

// TestBuildBlockRefusesAParentAtItsOwnSlot is the negative half: the default
// live-tip path must never produce the block the equal-slot case would
// otherwise ask it for. Binding the tip as parent when the tip is already at
// the forged slot yields a block whose parent slot equals its own, which
// ledger.validateBlockOrder rejects ("block slot %d does not follow parent slot
// %d") and every Praos peer rejects for the same reason. The builder refuses
// before signing rather than emitting it.
func TestBuildBlockRefusesAParentAtItsOwnSlot(t *testing.T) {
	const contestedSlot = uint64(1000)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: testHash32(0xAA)},
		BlockNumber: 100,
	}
	builder := newBlockContextTestBuilder(t, rival)

	t.Run("live tip path", func(t *testing.T) {
		block, blockCbor, err := builder.BuildBlock(contestedSlot, 0)
		require.ErrorIs(t, err, errParentSlotNotBelowBlock)
		assert.Nil(t, block)
		assert.Nil(t, blockCbor)
	})

	t.Run("explicit context naming the rival", func(t *testing.T) {
		block, blockCbor, err := builder.BuildBlockOnContext(
			contestedSlot,
			0,
			LeiosBlockData{},
			BlockContext{
				Parent:      rival.Point,
				BlockNumber: rival.BlockNumber,
				Rival:       rival,
			},
		)
		require.ErrorIs(t, err, errParentSlotNotBelowBlock)
		assert.Nil(t, block)
		assert.Nil(t, blockCbor)
	})

	t.Run("explicit context with a parent above the forged slot", func(t *testing.T) {
		block, _, err := builder.BuildBlockOnContext(
			contestedSlot,
			0,
			LeiosBlockData{},
			BlockContext{
				Parent: ocommon.Point{
					Slot: contestedSlot + 1,
					Hash: testHash32(0xBB),
				},
				BlockNumber: rival.BlockNumber,
				Rival:       rival,
			},
		)
		require.ErrorIs(t, err, errParentSlotNotBelowBlock)
		assert.Nil(t, block)
	})
}

// TestBuildBlockOnContextAbandonsAStaleContest pins that a candidate bound to a
// rival which is no longer the chain tip is dropped before any signing work.
// The contest is over: either the rival was rolled back, or the chain moved on
// past it, and in both cases the alternative is meaningless.
func TestBuildBlockOnContextAbandonsAStaleContest(t *testing.T) {
	const contestedSlot = uint64(1000)
	liveTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: contestedSlot,
			Hash: testHash32(0xCC),
		},
		BlockNumber: 100,
	}
	builder := newBlockContextTestBuilder(t, liveTip)

	staleRival := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: contestedSlot,
			Hash: testHash32(0xAA),
		},
		BlockNumber: 100,
	}
	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{
			Parent: ocommon.Point{
				Slot: contestedSlot - 1,
				Hash: testHash32(0xBB),
			},
			BlockNumber: staleRival.BlockNumber,
			Rival:       staleRival,
		},
	)
	require.ErrorIs(t, err, errParentChangedDuringBuild)
	assert.Nil(t, block)
}

// TestBuildBlockOnContextRequiresAResolvedParent pins the fail-closed contract
// shared with chain.TipPredecessor: a context with no parent must not silently
// fall back to a genesis-shaped (null prevHash) block.
func TestBuildBlockOnContextRequiresAResolvedParent(t *testing.T) {
	const contestedSlot = uint64(1000)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: testHash32(0xAA)},
		BlockNumber: 100,
	}
	builder := newBlockContextTestBuilder(t, rival)

	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{BlockNumber: rival.BlockNumber, Rival: rival},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolved parent")
	assert.Nil(t, block)
}

// TestBuildBlockStillExtendsTheLiveTipByDefault guards the unchanged path: an
// uncontested slot builds on the live tip with the tip's block number plus one.
func TestBuildBlockStillExtendsTheLiveTipByDefault(t *testing.T) {
	tipHash := testHash32(0xAA)
	builder := newBlockContextTestBuilder(t, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1000, Hash: tipHash},
		BlockNumber: 100,
	})

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.NotNil(t, block)
	assert.Equal(t, uint64(1001), block.SlotNumber())
	assert.Equal(t, uint64(101), block.BlockNumber())
	assert.Equal(t, tipHash, block.PrevHash().Bytes())
}

// TestBuildBlockOnContextCarriesNoMempoolTransactions is the state-mismatch
// half of the equal-slot alternative. Every transaction validator reachable
// from the builder answers against the ledger's live state, which has the
// rival applied; the alternative is built on the rival's predecessor and
// adoption rolls the rival back before applying it. A mempool transaction
// spending a UTxO the rival created therefore passes validation, gets
// selected, and then fails to apply after adoption -- wedging the node at the
// fork point with neither candidate on the chain.
//
// The validator here accepts everything, exactly as a live-state validator
// would for such a transaction. The live-tip build shows it is admitted there;
// the context build must not consult the validator at all.
func TestBuildBlockOnContextCarriesNoMempoolTransactions(t *testing.T) {
	const (
		contestedSlot   = uint64(1000)
		parentSlot      = uint64(999)
		rivalBlockNumbr = uint64(100)
	)
	rivalHash := testHash32(0xAA)
	parentHash := testHash32(0xBB)
	rival := ochainsync.Tip{
		Point:       ocommon.Point{Slot: contestedSlot, Hash: rivalHash},
		BlockNumber: rivalBlockNumbr,
	}
	// A UTxO that only exists because the rival block created it.
	rivalCreatedTxHash := testHash32(0xCC)
	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "spends_rival_output",
				Cbor: makeMinimalTxCborWithInput(t, rivalCreatedTxHash, 0),
				Type: conway.TxTypeConway,
			},
		},
	}
	validator := &sessionMockTxValidator{}
	builder := newSelectionTestBuilder(
		t,
		mempool,
		&mockChainTip{tip: rival},
		validator,
	)

	// Live-tip control: the validator admits the transaction, so a normal
	// build selects it. This is the state the alternative must not inherit.
	control, _, err := builder.BuildBlock(contestedSlot+1, 0)
	require.NoError(t, err)
	require.Len(
		t,
		control.Transactions(),
		1,
		"control: the live-state validator admits this transaction",
	)
	require.Equal(t, 1, validator.validateCalls)

	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		BlockContext{
			Parent:      ocommon.Point{Slot: parentSlot, Hash: parentHash},
			BlockNumber: rivalBlockNumbr,
			Rival:       rival,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, block)
	assert.Empty(
		t,
		block.Transactions(),
		"an alternative must carry no transaction selected against the rival's state",
	)
	assert.Equal(
		t,
		1,
		validator.validateCalls,
		"no mempool transaction may be offered to a live-state validator for an alternative",
	)
}

// TestBuildBlockOnContextRejectsLeiosData pins the fail-closed guard on the
// exported entrypoint. Leios certificate and announcement data is resolved
// against the live tip -- the rival -- so a block that does not build on the
// rival must not carry it. The forger omits it already; the builder refuses it
// rather than trusting every caller to.
func TestBuildBlockOnContextRejectsLeiosData(t *testing.T) {
	const (
		contestedSlot   = uint64(1000)
		parentSlot      = uint64(999)
		rivalBlockNumbr = uint64(100)
	)
	rival := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: contestedSlot,
			Hash: testHash32(0xAA),
		},
		BlockNumber: rivalBlockNumbr,
	}
	builder := newBlockContextTestBuilder(t, rival)
	blockCtx := BlockContext{
		Parent: ocommon.Point{
			Slot: parentSlot,
			Hash: testHash32(0xBB),
		},
		BlockNumber: rivalBlockNumbr,
		Rival:       rival,
	}

	for name, leios := range map[string]LeiosBlockData{
		"announcement": {
			Announcement: &LeiosEndorserBlockAnnouncement{},
		},
		"certificate": {
			Certificate: &lcommon.LeiosEbCertificate{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			block, blockCbor, err := builder.BuildBlockOnContext(
				contestedSlot,
				0,
				leios,
				blockCtx,
			)
			require.ErrorIs(t, err, errLeiosDataOnAlternative)
			assert.Nil(t, block)
			assert.Nil(t, blockCbor)
		})
	}

	// Empty Leios data on the same context still builds, so the guard is
	// rejecting the data and not the context.
	block, _, err := builder.BuildBlockOnContext(
		contestedSlot,
		0,
		LeiosBlockData{},
		blockCtx,
	)
	require.NoError(t, err)
	require.NotNil(t, block)
}

func bodyBudgetParams(era eraKind, limit uint) lcommon.ProtocolParameters {
	switch era {
	case eraShelley, eraAllegra:
		return &shelley.ShelleyProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit,
			ProtocolMajor: uint(era) + 1,
		}
	case eraMary:
		return &mary.MaryProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit, ProtocolMajor: 4,
		}
	case eraAlonzo:
		return &alonzo.AlonzoProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit, ProtocolMajor: 5,
		}
	case eraBabbage:
		return &babbage.BabbageProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit, ProtocolMajor: 7,
		}
	default:
		return &conway.ConwayProtocolParameters{
			MaxTxSize: 16384, MaxBlockBodySize: limit,
		}
	}
}

func bodyBudgetTransaction(
	t testing.TB,
	era eraKind,
	index int,
	withMetadata bool,
) MempoolTransaction {
	t.Helper()
	var auxiliary any
	if withMetadata {
		metadata := map[uint]any{0: bytes.Repeat([]byte{0xab}, 64)}
		scripts := []any{[]any{uint(0), make([]byte, 28)}}
		switch era {
		case eraShelley:
			auxiliary = metadata
		case eraAllegra, eraMary:
			auxiliary = []any{metadata, scripts}
		default:
			auxiliary = cbor.Tag{
				Number: 259,
				Content: map[uint]any{
					0: metadata, 1: scripts, 2: []any{[]byte{1, 2, 3}},
				},
			}
		}
	}
	body := map[uint]any{
		0: []any{[]any{make([]byte, 32), uint(index)}},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1000000)}},
		2: uint(200000),
		3: uint(200000),
	}
	if era == eraShelley || era == eraAllegra || era == eraMary || era == eraAlonzo {
		body[3] = uint64(1000)
	}
	if auxiliary != nil {
		encoded, err := cbor.Encode(auxiliary)
		require.NoError(t, err)
		body[7] = lcommon.Blake2b256Hash(encoded).Bytes()
	}
	parts := []any{body, map[uint]any{}}
	if era >= eraAlonzo {
		parts = append(parts, true)
	}
	parts = append(parts, auxiliary)
	encoded, err := cbor.Encode(parts)
	require.NoError(t, err)
	transaction := MempoolTransaction{
		Cbor: encoded, Type: uint(era),
	}
	decoded, err := decodeMempoolTx(transaction)
	require.NoError(t, err)
	transaction.Hash = decoded.Hash().String()
	return transaction
}

func bodyBudgetEncodedSize(
	t testing.TB,
	era eraKind,
	transactions []MempoolTransaction,
) uint64 {
	t.Helper()
	bodies := []cbor.RawMessage{}
	witnesses := []cbor.RawMessage{}
	metadata := map[uint]cbor.RawMessage{}
	for index, transaction := range transactions {
		var parts []cbor.RawMessage
		_, err := cbor.Decode(transaction.Cbor, &parts)
		require.NoError(t, err)
		bodies = append(bodies, parts[0])
		witnesses = append(witnesses, parts[1])
		auxiliary := parts[len(parts)-1]
		if !bytes.Equal(auxiliary, []byte{0xf6}) {
			metadata[uint(index)] = auxiliary
		}
	}
	components := []any{bodies, witnesses, metadata}
	if era == eraAlonzo {
		components = append(components, cbor.IndefLengthList{})
	} else if era > eraAlonzo {
		components = append(components, []uint{})
	}
	var size uint64
	for _, component := range components {
		encoded, err := cbor.Encode(component)
		require.NoError(t, err)
		size += uint64(len(encoded))
	}
	return size
}

func TestBuildBlockEncodedBodyBudget(t *testing.T) {
	credentials := setupTestCredentials(t)
	for _, era := range []eraKind{
		eraShelley, eraAllegra, eraMary, eraAlonzo, eraBabbage, eraConway,
	} {
		for _, metadata := range []bool{false, true} {
			for _, count := range []int{1, 3, 24, 25, 256, 257} {
				transactions := make([]MempoolTransaction, count)
				for index := range transactions {
					transactions[index] = bodyBudgetTransaction(
						t, era, index, metadata,
					)
				}
				exactSize := bodyBudgetEncodedSize(t, era, transactions)
				for _, delta := range []int{-1, 0, 1} {
					t.Run(fmt.Sprintf(
						"era=%d/metadata=%t/count=%d/delta=%d",
						era, metadata, count, delta,
					), func(t *testing.T) {
						limit := uint(int(exactSize) + delta)
						builder := setupCredentialValidationBuilder(
							t,
							credentials,
						)
						builder.mempool = &mockMempool{
							transactions: transactions,
						}
						builder.pparamsProvider = &mockPParamsProvider{
							pparams: bodyBudgetParams(era, limit),
						}
						block, encoded, err := builder.BuildBlock(1001, 0)
						require.NoError(
							t,
							err,
							"size overflow must retain the fitting prefix",
						)
						wantCount := count
						if delta < 0 {
							wantCount--
						}
						require.Len(
							t,
							block.Transactions(),
							wantCount,
							"admission must use encoded bytes, not raw transaction sizes",
						)
						for index, transaction := range block.Transactions() {
							require.Equal(t, transactions[index].Hash,
								transaction.Hash().String())
						}
						var fields []cbor.RawMessage
						_, err = cbor.Decode(encoded, &fields)
						require.NoError(t, err)
						var wireSize uint64
						for _, field := range fields[1:] {
							wireSize += uint64(len(field))
						}
						require.Equal(t, bodyBudgetEncodedSize(
							t, era, transactions[:wantCount],
						), wireSize)
						require.Equal(t, wireSize, block.BlockBodySize())
						require.LessOrEqual(t, wireSize, uint64(limit))
						var actualMetadata map[uint]cbor.RawMessage
						_, err = cbor.Decode(fields[3], &actualMetadata)
						require.NoError(t, err)
						wantMetadata := 0
						if metadata {
							wantMetadata = wantCount
						}
						require.Len(t, actualMetadata, wantMetadata)
					})
				}
			}
		}
	}
}

func TestBuildBlockEncodedBudgetRetainsPrefixAfterSkippedTransaction(
	t *testing.T,
) {
	transactions := []MempoolTransaction{
		bodyBudgetTransaction(t, eraConway, 0, false),
		bodyBudgetTransaction(t, eraConway, 1, true),
		bodyBudgetTransaction(t, eraConway, 2, true),
		bodyBudgetTransaction(t, eraConway, 3, true),
	}
	selected := []MempoolTransaction{transactions[0], transactions[2]}
	limit := bodyBudgetEncodedSize(t, eraConway, selected)
	builder := setupCredentialValidationBuilder(t, setupTestCredentials(t))
	builder.mempool = &mockMempool{transactions: transactions}
	builder.pparamsProvider = &mockPParamsProvider{
		pparams: bodyBudgetParams(eraConway, uint(limit)),
	}
	builder.txValidator = &mockTxValidator{
		rejectHashes: map[string]struct{}{transactions[1].Hash: {}},
	}
	block, encoded, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 2)
	require.Equal(t, selected[0].Hash, block.Transactions()[0].Hash().String())
	require.Equal(t, selected[1].Hash, block.Transactions()[1].Hash().String())
	require.Equal(t, limit, block.BlockBodySize())
	var fields []cbor.RawMessage
	_, err = cbor.Decode(encoded, &fields)
	require.NoError(t, err)
	var metadata map[uint]cbor.RawMessage
	_, err = cbor.Decode(fields[3], &metadata)
	require.NoError(t, err)
	require.Len(t, metadata, 1)
	require.Contains(t, metadata, uint(1))
}

// TestBuildBlockSupportsAllEras verifies BuildBlock dispatches by era
// across the full era table — TPraos (Shelley/Allegra/Mary/Alonzo)
// and Praos (Babbage/Conway) — and that the block re-decodes through
// the era-correct constructor. Each subtest also asserts
// the concrete block type so a regression in decodeBlockFromCbor that
// returned the wrong era's struct would fail loudly.
func TestBuildBlockSupportsAllEras(t *testing.T) {
	creds := setupTestCredentials(t)

	const (
		maxTxSize        = uint(16384)
		maxBlockBodySize = uint(90112)
	)
	maxBlockExUnits := lcommon.ExUnits{
		Memory: 62000000,
		Steps:  20000000000,
	}
	maxTxExUnits := lcommon.ExUnits{
		Memory: 14000000,
		Steps:  10000000000,
	}

	// wantBlock is the concrete type the era's decoder must return.
	// Storing it as an any sentinel keeps the table compact.
	cases := []struct {
		name      string
		pparams   lcommon.ProtocolParameters
		wantBlock any
	}{
		{
			name: "shelley",
			pparams: &shelley.ShelleyProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    2,
			},
			wantBlock: (*shelley.ShelleyBlock)(nil),
		},
		{
			name: "allegra",
			pparams: &allegra.AllegraProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    3,
			},
			wantBlock: (*allegra.AllegraBlock)(nil),
		},
		{
			name: "mary",
			pparams: &mary.MaryProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    4,
			},
			wantBlock: (*mary.MaryBlock)(nil),
		},
		{
			name: "alonzo",
			pparams: &alonzo.AlonzoProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    5,
				MaxBlockExUnits:  maxBlockExUnits,
				MaxTxExUnits:     maxTxExUnits,
			},
			wantBlock: (*alonzo.AlonzoBlock)(nil),
		},
		{
			name: "babbage",
			pparams: &babbage.BabbageProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    7,
				MaxBlockExUnits:  maxBlockExUnits,
				MaxTxExUnits:     maxTxExUnits,
			},
			wantBlock: (*babbage.BabbageBlock)(nil),
		},
		{
			name: "conway",
			pparams: &conway.ConwayProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 9,
				},
				MaxBlockExUnits: maxBlockExUnits,
				MaxTxExUnits:    maxTxExUnits,
			},
			wantBlock: (*conway.ConwayBlock)(nil),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool: &mockMempool{
					transactions: []MempoolTransaction{},
				},
				PParamsProvider: &mockPParamsProvider{pparams: tc.pparams},
				ChainTip: &mockChainTip{
					tip: ochainsync.Tip{
						Point: ocommon.Point{
							Slot: 1000,
							Hash: make([]byte, 32),
						},
						BlockNumber: 100,
					},
				},
				EpochNonce: &mockEpochNonceProvider{
					epoch: 1,
					nonce: make([]byte, 32),
				},
				Credentials: creds,
			})
			require.NoError(t, err)

			block, blockCbor, err := builder.BuildBlock(1001, 0)
			require.NoError(
				t,
				err,
				"BuildBlock must succeed in %s era (issue #2124)",
				tc.name,
			)
			require.NotNil(t, block)
			require.NotEmpty(t, blockCbor)

			assert.IsType(
				t,
				tc.wantBlock,
				block,
				"%s era must round-trip through the matching block constructor",
				tc.name,
			)
			assert.Equal(t, uint64(1001), block.SlotNumber())
			assert.Equal(t, uint64(101), block.BlockNumber())
			assert.Equal(t, 0, len(block.Transactions()))
		})
	}
}

// TestBuildBlockSupportsDijkstraEra verifies BuildBlock forges on the
// Dijkstra (Leios) era: a musashi block producer must build a block from
// *dijkstra.DijkstraProtocolParameters instead of failing the forge with
// "unsupported protocol parameter type". Dijkstra shares Conway's Praos
// block/header layout, so the forged block round-trips through the
// Dijkstra block constructor.
func TestBuildBlockSupportsDijkstraEra(t *testing.T) {
	creds := setupTestCredentials(t)

	// DijkstraProtocolParameters embeds ConwayProtocolParameters, so the
	// shared limits are set via the embedded field.
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        16384,
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err, "BuildBlock must succeed in the Dijkstra era")
	require.NotNil(t, block)
	require.NotEmpty(t, blockCbor)

	assert.IsType(
		t,
		(*dijkstra.DijkstraBlock)(nil),
		block,
		"Dijkstra era must round-trip through the Dijkstra block constructor",
	)
	assert.Equal(t, uint64(1001), block.SlotNumber())
	assert.Equal(t, uint64(101), block.BlockNumber())
	assert.Equal(t, 0, len(block.Transactions()))
	var blockItems []cbor.RawMessage
	_, err = cbor.Decode(blockCbor, &blockItems)
	require.NoError(t, err)
	require.Len(t, blockItems, 2)
	var bodyItems []cbor.RawMessage
	_, err = cbor.Decode(blockItems[1], &bodyItems)
	require.NoError(t, err)
	require.Len(
		t,
		bodyItems,
		3,
		"current Dijkstra block bodies omit the obsolete invalid_transactions field",
	)

	// The forged block's body hash must match the header commitment, i.e.
	// the encoded Dijkstra block_body with null certificate fields. A
	// mismatch means the network would reject the block.
	dblock := block.(*dijkstra.DijkstraBlock)
	certified, present := dblock.BlockHeader.LeiosCertified()
	require.True(
		t,
		present,
		"Dijkstra forge must emit the Leios header extension",
	)
	assert.False(t, certified)
	_, _, announced := dblock.BlockHeader.LeiosAnnouncement()
	assert.False(t, announced)
	assert.Equal(
		t,
		dblock.BlockBodyHash(),
		dblock.CalculatedBlockBodyHash(),
		"forged Dijkstra block body hash must match the header commitment",
	)
}

func TestDijkstraBlockTransactionRejectsInvalidMempoolTx(t *testing.T) {
	// Mempool order is [body, witnesses, is_valid, auxiliary_data].
	_, err := dijkstraBlockTransactionCbor(
		[]byte{0x84, 0x80, 0xa0, 0xf4, 0xf6},
	)
	require.ErrorContains(t, err, "is_valid=false")
}

func TestBuildBlockDijkstraAnnouncesLeiosEndorserBlock(t *testing.T) {
	creds := setupTestCredentials(t)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        16384,
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	ebHash := lcommon.NewBlake2b256(make([]byte, lcommon.Blake2b256Size))
	block, _, err := builder.BuildBlockWithLeios(1001, 0, LeiosBlockData{
		Announcement: &LeiosEndorserBlockAnnouncement{
			Hash: ebHash,
			Size: 1234,
		},
	})
	require.NoError(t, err)
	dblock := block.(*dijkstra.DijkstraBlock)

	certified, present := dblock.BlockHeader.LeiosCertified()
	require.True(t, present)
	assert.False(t, certified)
	gotHash, gotSize, ok := dblock.BlockHeader.LeiosAnnouncement()
	require.True(t, ok)
	assert.Equal(t, ebHash, gotHash)
	assert.Equal(t, uint64(1234), gotSize)
	assert.Nil(t, dblock.BlockBody.LeiosCertificate)
	assert.Equal(t, dblock.BlockBodyHash(), dblock.CalculatedBlockBodyHash())
}

func TestBuildBlockDijkstraDoesNotMixAnnouncedEndorserTransactions(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	ebTxCbor := makeMinimalTxCbor(t, 0x31, 0)
	rankingTxCbor := makeMinimalTxCbor(t, 0x32, 0)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        16384,
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 12,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{transactions: []MempoolTransaction{
			{Hash: "eb-tx", Cbor: ebTxCbor, Type: dijkstra.TxTypeDijkstra},
			{
				Hash: "ranking-tx",
				Cbor: rankingTxCbor,
				Type: dijkstra.TxTypeDijkstra,
			},
		}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{tip: ochainsync.Tip{
			Point:       ocommon.Point{Slot: 1000, Hash: make([]byte, 32)},
			BlockNumber: 100,
		}},
		EpochNonce:  &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlockWithLeios(1001, 0, LeiosBlockData{
		Announcement: &LeiosEndorserBlockAnnouncement{
			Hash: lcommon.NewBlake2b256(make([]byte, lcommon.Blake2b256Size)),
			Size: 1234,
		},
	})
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
}

func TestBuildBlockDijkstraRejectsOversizeLeiosAnnouncement(t *testing.T) {
	creds := setupTestCredentials(t)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        16384,
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	ebHash := lcommon.NewBlake2b256(make([]byte, lcommon.Blake2b256Size))
	_, _, err = builder.BuildBlockWithLeios(1001, 0, LeiosBlockData{
		Announcement: &LeiosEndorserBlockAnnouncement{
			Hash: ebHash,
			Size: uint64(math.MaxUint32) + 1,
		},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "leios announcement size exceeds uint32")
}

func TestBuildBlockDijkstraCertifiesAndAnnouncesLeiosEndorserBlocks(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	txCbor := makeMinimalTxCbor(t, 0x01, 0)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	signature := make([]byte, lcommon.LeiosBlsSignatureSize)
	for i := range signature {
		signature[i] = byte(i)
	}
	announcedHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
	block, _, err := builder.BuildBlockWithLeios(1001, 0, LeiosBlockData{
		Announcement: &LeiosEndorserBlockAnnouncement{
			Hash: announcedHash,
			Size: 1234,
		},
		Certificate: &lcommon.LeiosEbCertificate{
			SlotNo: 900,
			EndorserBlockHash: lcommon.NewBlake2b256(
				make([]byte, lcommon.Blake2b256Size),
			),
			Signers:             []byte{0x80},
			AggregatedSignature: signature,
		},
	})
	require.NoError(t, err)
	dblock := block.(*dijkstra.DijkstraBlock)

	certified, present := dblock.BlockHeader.LeiosCertified()
	require.True(t, present)
	assert.True(t, certified)
	gotHash, gotSize, announced := dblock.BlockHeader.LeiosAnnouncement()
	require.True(t, announced)
	assert.Equal(t, announcedHash, gotHash)
	assert.Equal(t, uint64(1234), gotSize)
	require.NotNil(t, dblock.BlockBody.LeiosCertificate)
	assert.Equal(t, []byte{0x80}, dblock.BlockBody.LeiosCertificate.Signers)
	assert.Equal(
		t,
		signature,
		dblock.BlockBody.LeiosCertificate.AggregatedSignature,
	)
	assert.Empty(t, dblock.Transactions())
	assert.Equal(t, dblock.BlockBodyHash(), dblock.CalculatedBlockBodyHash())
}

func TestBuildBlockDijkstraNormalizesAdmittedTxForBlock(t *testing.T) {
	creds := setupTestCredentials(t)
	txCbor := makeMinimalTxCbor(t, 0x01, 0)

	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Len(t, block.Transactions(), 1)

	dblock := block.(*dijkstra.DijkstraBlock)
	assert.Equal(
		t,
		dblock.BlockBodyHash(),
		dblock.CalculatedBlockBodyHash(),
		"forged Dijkstra block body hash must match the normalized body",
	)

	var originalFields []cbor.RawMessage
	_, err = cbor.Decode(txCbor, &originalFields)
	require.NoError(t, err)
	require.Len(t, originalFields, 4)

	var rawBlock []cbor.RawMessage
	_, err = cbor.Decode(blockCbor, &rawBlock)
	require.NoError(t, err)
	require.Len(t, rawBlock, 2)

	var rawBody []cbor.RawMessage
	_, err = cbor.Decode(rawBlock[1], &rawBody)
	require.NoError(t, err)
	require.Len(t, rawBody, 3)

	var rawTxs []cbor.RawMessage
	_, err = cbor.Decode(rawBody[0], &rawTxs)
	require.NoError(t, err)
	require.Len(t, rawTxs, 1)

	var blockTxFields []cbor.RawMessage
	_, err = cbor.Decode(rawTxs[0], &blockTxFields)
	require.NoError(t, err)
	require.Len(t, blockTxFields, 4)
	assert.Equal(t, originalFields[0], blockTxFields[0])
	assert.Equal(t, originalFields[1], blockTxFields[1])
	assert.Equal(t, originalFields[3], blockTxFields[2])
	assert.Equal(t, originalFields[2], blockTxFields[3])
}

func TestBuildBlockDijkstraRespectsActualBlockBodySize(t *testing.T) {
	creds := setupTestCredentials(t)
	txCbor := makeMinimalTxCbor(t, 0x01, 0)

	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: uint(len(txCbor)),
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.NotNil(t, block)

	require.Empty(t, block.Transactions())
	require.LessOrEqual(
		t,
		block.BlockBodySize(),
		uint64(pparams.MaxBlockBodySize),
	)
}

func mempoolOfSize(t *testing.T, count int) *mockMempool {
	t.Helper()
	txs := make([]MempoolTransaction, 0, count)
	for i := range count {
		txs = append(txs, MempoolTransaction{
			Hash: string(rune('a' + i)),
			Cbor: makeMinimalTxCbor(t, byte(i+1), 0),
			Type: conway.TxTypeConway,
		})
	}
	return &mockMempool{transactions: txs}
}

// TestSelectionStopsAtTheSlotDeadline pins the cost bound on transaction
// selection. Every candidate costs a full ledger re-validation, so a large
// mempool makes a selection pass run for seconds -- long past the slot it
// is building for, and long enough for a ledger publication to land in the
// middle of it. Selection must stop at the deadline and forge what it has.
func TestSelectionStopsAtTheSlotDeadline(t *testing.T) {
	const (
		mempoolSize   = 10
		perTxCost     = 10 * time.Millisecond
		selectionTime = 55 * time.Millisecond
	)
	start := time.Now()
	fakeNow := start
	validator := &sessionMockTxValidator{}
	validator.onValidate = func(int) { fakeNow = fakeNow.Add(perTxCost) }

	builder := newSelectionTestBuilder(
		t,
		mempoolOfSize(t, mempoolSize),
		selectionTestChainTip(),
		validator,
	)
	builder.now = func() time.Time { return fakeNow }

	generation := builder.creds.acquireCredentialGeneration()
	defer generation.release()
	block, _, err := builder.buildBlockWithCredentialGeneration(
		1001,
		0,
		LeiosBlockData{},
		generation,
		blockSelectionConstraints{deadline: start.Add(selectionTime)},
		nil,
	)
	require.NoError(t, err)
	// Six candidates are considered before the clock reaches the
	// deadline (checks at 0, 10, 20, 30, 40, 50ms; the check at 60ms
	// stops the pass).
	require.Len(t, block.Transactions(), 6)
	require.Equal(t, 6, validator.validateCalls)
	require.False(
		t,
		fakeNow.After(start.Add(selectionTime).Add(perTxCost)),
		"selection must not run past the deadline by more than one candidate",
	)
}

// TestSelectionAbortsAsSoonAsTheSnapshotChanges is the wasted-work half of
// the lost-slot defect: stillCurrent() was consulted only after the whole
// pass, so a producer kept re-validating transactions against a snapshot
// that had already been superseded before throwing all of it away.
func TestSelectionAbortsAsSoonAsTheSnapshotChanges(t *testing.T) {
	validator := &sessionMockTxValidator{staleAfterCalls: 1}
	builder := newSelectionTestBuilder(
		t,
		mempoolOfSize(t, 10),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.Error(t, err)
	require.Nil(t, block)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(
		t,
		1,
		validator.validateCalls,
		"selection must stop at the first check after the snapshot moved",
	)
}

// TestSelectionSkipsValidatingTransactionsThatCannotFit removes the other
// half of the exposure window: a candidate that cannot fit in the block
// body was still paying for a full ledger re-validation before the size
// check rejected it.
func TestSelectionSkipsValidatingTransactionsThatCannotFit(t *testing.T) {
	txCbor := makeMinimalTxCbor(t, 0x01, 0)
	// MaxBlockBodySize is exactly one transaction, which the encoded
	// Dijkstra block body wrapper always exceeds.
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: uint(len(txCbor)),
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	validator := &sessionMockTxValidator{}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: setupTestCredentials(t),
		TxValidator: validator,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
	require.Zero(
		t,
		validator.validateCalls,
		"a transaction that cannot fit must not be re-validated first",
	)
}

// constraintRecordingBuilder captures the constraints the forge loop hands
// to the builder on the production path.
type constraintRecordingBuilder struct {
	block       ledger.Block
	cbor        []byte
	constraints []blockSelectionConstraints
}

func (b *constraintRecordingBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	return b.block, b.cbor, nil
}

func (b *constraintRecordingBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	_ LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
	_ *BlockContext,
) (ledger.Block, []byte, error) {
	b.constraints = append(b.constraints, constraints)
	return b.block, b.cbor, nil
}

var _ credentialGenerationBlockBuilder = (*constraintRecordingBuilder)(nil)

// TestBuildDingoForgeHandsTheSlotDeadlineToSelection follows the runtime
// composition path a production forge takes -- checkAndForgeProduction ->
// buildBlockForSlot -> buildBlock -> buildBlockWithCredentialGeneration --
// and proves the configured selection deadline actually arrives at the
// builder. A deadline that exists in the config but never reaches selection
// bounds nothing.
func TestBuildDingoForgeHandsTheSlotDeadlineToSelection(t *testing.T) {
	const selectionMargin = 400 * time.Millisecond
	block := newForgerTestBlock(10, 2)
	builder := &constraintRecordingBuilder{block: block, cbor: block.cbor}
	slotEnd := time.Now().Add(2 * time.Second)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           slotEnd,
	}
	forger := newRetryForger(
		t,
		clock,
		builder,
		&forgerTestBroadcaster{},
		withSelectionDeadlineMargin(selectionMargin),
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.constraints, 1)
	require.Equal(
		t,
		slotEnd.Add(-selectionMargin),
		builder.constraints[0].deadline,
		"selection must be bounded by the end of the slot being forged, less the selection-deadline margin",
	)
	require.False(t, builder.constraints[0].emptyBody)
}

// TestForgeDoesNotTruncateSelectionByDefault pins the two budgets apart.
// ForgeSelectionRetryMargin decides whether a second attempt is worth
// starting; it used to double as the instant the first pass was cut short,
// so simply enabling in-slot re-selection also made every producer stop
// selecting 250ms before the end of its slot. How full blocks get is now
// opt-in, and the default is what a producer always did.
func TestForgeDoesNotTruncateSelectionByDefault(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &constraintRecordingBuilder{block: block, cbor: block.cbor}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// Most of the slot remains, so nothing but the margin itself
		// could cut the pass short.
		slotEnd: time.Now().Add(2 * time.Second),
	}
	forger := newRetryForger(t, clock, builder, &forgerTestBroadcaster{})

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.constraints, 1)
	require.True(
		t,
		builder.constraints[0].deadline.IsZero(),
		"the retry margin must not truncate the selection pass",
	)
	require.NotZero(
		t,
		forger.forgeSelectionRetryMargin,
		"the retry bound is still configured; it simply does not truncate",
	)
}

// TestForgeDropsTheSelectionDeadlineWhenTheSlotIsOver is the other half:
// once the slot has passed there is nothing left to protect, and cutting
// selection short would drop transactions without recovering any of it.
func TestForgeDropsTheSelectionDeadlineWhenTheSlotIsOver(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &constraintRecordingBuilder{block: block, cbor: block.cbor}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newRetryForger(
		t,
		clock,
		builder,
		&forgerTestBroadcaster{},
		withSelectionDeadlineMargin(250*time.Millisecond),
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.constraints, 1)
	require.True(t, builder.constraints[0].deadline.IsZero())
}

// TestForgeWithCustomBuilderIgnoresTheSelectionDeadline keeps embedders
// working. A BlockBuilder that predates per-attempt constraints cannot be
// handed a deadline, and an unbounded selection pass is exactly what it
// always did, so the deadline is dropped rather than failing the forge.
// Only the empty-body fallback genuinely needs builder support.
func TestForgeWithCustomBuilderIgnoresTheSelectionDeadline(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
}

// advancingSlotClock crosses a slot boundary between the leader check and
// the deadline computation: the first reading is the slot being forged, and
// every later one is the slot after it. That is the ordinary way a forge
// runs out of slot -- the leader check, the Leios payload and the KES
// update all happen first -- and it is the only way the clock can disagree
// with the slot under construction, because the forge slot is that same
// clock's first answer.
type advancingSlotClock struct {
	slot              uint64
	chainTipSlot      uint64
	slotsPerKESPeriod uint64
	// nextSlotEnd is the boundary the clock reports once it has already
	// left the forged slot: the end of the *following* slot, which is
	// budget this forge does not have.
	nextSlotEnd time.Time
	calls       int
}

func (c *advancingSlotClock) CurrentSlot() (uint64, error) {
	c.calls++
	if c.calls == 1 {
		return c.slot, nil
	}
	return c.slot + 1, nil
}

func (c *advancingSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

// ChainTip and PrimaryChainTip report the same point: this clock exists to
// exercise the slot-boundary arithmetic, not the ledger-apply backlog, so it
// describes a caught-up node whose primary chain tip is the applied tip.
func (c *advancingSlotClock) ChainTip() ocommon.Point {
	return ocommon.Point{Slot: c.chainTipSlot}
}

func (c *advancingSlotClock) PrimaryChainTip() ocommon.Point {
	return ocommon.Point{Slot: c.chainTipSlot}
}

func (c *advancingSlotClock) ForgeTipSnapshot() (ochainsync.Tip, int) {
	return ochainsync.Tip{Point: c.ChainTip()}, 5
}

func (c *advancingSlotClock) PrimaryChainTipRelation(
	point ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	primary := c.PrimaryChainTip()
	depth := uint64(0)
	ancestor := primary.Slot >= point.Slot
	if primary.Slot > point.Slot {
		depth = primary.Slot - point.Slot
	}
	return ochainsync.Tip{Point: primary}, depth, ancestor, nil
}

func (*advancingSlotClock) UpstreamSyncTip() (ochainsync.Tip, bool) {
	return ochainsync.Tip{}, false
}

func (c *advancingSlotClock) NextSlotTime() (time.Time, error) {
	return c.nextSlotEnd, nil
}

func (c *advancingSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *advancingSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

// TestForgeDropsTheSelectionDeadlineWhenTheClockHasLeftTheSlot pins the
// guard that makes the boundary answer trustworthy. NextSlotTime is derived
// from the clock's own current slot, so once the clock has moved on it
// describes the *next* slot's end -- a budget the block being forged does
// not have, and one long enough to keep selection and its retries running
// well past the slot they belong to. Reading the clock slot first and
// treating a mismatch as "the slot is over" is what keeps that from
// happening; without it a late forge is handed a full extra slot.
func TestForgeDropsTheSelectionDeadlineWhenTheClockHasLeftTheSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	builder := &constraintRecordingBuilder{block: block, cbor: block.cbor}
	clock := &advancingSlotClock{
		slot:              10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		nextSlotEnd:       time.Now().Add(time.Hour),
	}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
		// Truncation on, so a zero deadline here is the guard doing its
		// job rather than the feature being off.
		ForgeSelectionDeadlineMargin: 250 * time.Millisecond,
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.constraints, 1)
	require.True(
		t,
		builder.constraints[0].deadline.IsZero(),
		"a slot the clock has already left leaves no selection budget, "+
			"so the next slot's boundary must not become this slot's deadline",
	)
}

// newDijkstraFitBuilder builds a Dijkstra-era builder whose block body
// budget is maxBlockBody bytes, so a test can decide exactly which
// candidates fit.
func newDijkstraFitBuilder(
	t *testing.T,
	mempool *mockMempool,
	validator *sessionMockTxValidator,
	maxTxSize int,
	maxBlockBody int,
) *DefaultBlockBuilder {
	t.Helper()
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(maxTxSize),
			MaxBlockBodySize: uint(maxBlockBody),
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: setupTestCredentials(t),
		TxValidator: validator,
	})
	require.NoError(t, err)
	return builder
}

// makeWideTxCbor builds a valid Conway transaction carrying outputCount
// outputs, which is how a test grows a transaction past a block-body budget
// without producing an address the decoder rejects.
func makeWideTxCbor(t *testing.T, txID byte, outputCount int) []byte {
	t.Helper()
	txHash := make([]byte, 32)
	txHash[0] = txID
	outputs := make([]any, 0, outputCount)
	for range outputCount {
		// 29 bytes: a Shelley enterprise address, header plus key hash.
		addr := make([]byte, 29)
		addr[0] = 0x61
		outputs = append(outputs, []any{addr, uint64(1000000)})
	}
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{txHash, uint64(0)}},
		},
		1: outputs,
		2: uint64(200000),
	}
	txCbor, err := cbor.Encode([]any{bodyMap, map[uint]any{}, true, nil})
	require.NoError(t, err)
	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err, "generated CBOR must decode as a valid Conway tx")
	return txCbor
}

// TestSelectionContinuesPastACandidateThatCannotFit pins the stop rule.
// Running the exact block-body check before re-validation made the size
// break reachable from a candidate that would also have failed validation,
// so a transaction whose inputs were consumed since mempool admission --
// which used to be skipped -- could end the whole selection pass. On a
// mempool re-validating badly that shortens blocks for a reason that has
// nothing to do with fullness.
//
// A candidate that does not fit is now skipped, exactly as the MaxTxSize
// and MaxExUnits checks above it skip theirs, and only a re-validated
// candidate that the block cannot hold ends the pass.
func TestSelectionContinuesPastACandidateThatCannotFit(t *testing.T) {
	// first and last are small; middle is far too large for what remains
	// of the body once first has been selected, and would fail
	// re-validation as well.
	firstCbor := makeMinimalTxCbor(t, 0x01, 0)
	middleCbor := makeWideTxCbor(t, 0x02, 40)
	lastCbor := makeMinimalTxCbor(t, 0x03, 0)
	require.Greater(t, len(middleCbor), len(firstCbor)+len(lastCbor))

	validator := &sessionMockTxValidator{}
	// Enough body for the two small transactions and their wrapper, and
	// nowhere near enough for the large one.
	maxBlockBody := len(firstCbor) + len(lastCbor) + 64
	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: firstCbor,
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx2",
				Cbor: middleCbor,
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx3",
				Cbor: lastCbor,
				Type: dijkstra.TxTypeDijkstra,
			},
		},
	}
	// MaxTxSize admits the large transaction, so the body budget is the
	// only thing that can reject it.
	builder := newDijkstraFitBuilder(
		t,
		mempool,
		validator,
		len(middleCbor)+64,
		maxBlockBody,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(
		t,
		block.Transactions(),
		2,
		"a candidate that cannot fit must not end selection for the ones after it",
	)
	require.Len(
		t,
		validator.validatedHashes,
		2,
		"a candidate that cannot fit must not pay for re-validation",
	)
}

// TestSelectionContinuesPastACandidateThatFailsRevalidation is the plain
// case the hoist must not have disturbed: a transaction the block could
// have carried, rejected by re-validation, is skipped and the ones after it
// are still selected.
func TestSelectionContinuesPastACandidateThatFailsRevalidation(
	t *testing.T,
) {
	offered := 0
	validator := &sessionMockTxValidator{
		validateErr: func(string) error {
			offered++
			if offered == 2 {
				return errors.New("inputs already consumed")
			}
			return nil
		},
	}
	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: makeMinimalTxCbor(t, 0x01, 0),
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx2",
				Cbor: makeMinimalTxCbor(t, 0x02, 0),
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx3",
				Cbor: makeMinimalTxCbor(t, 0x03, 0),
				Type: dijkstra.TxTypeDijkstra,
			},
		},
	}
	builder := newDijkstraFitBuilder(t, mempool, validator, 4096, 16384)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 2)
	require.Len(
		t,
		validator.validatedHashes,
		3,
		"every candidate the block could hold is offered to re-validation",
	)
}

// mockMempool implements MempoolProvider for testing.
type mockMempool struct {
	transactions []MempoolTransaction
	calls        int
}

func (m *mockMempool) Transactions() []MempoolTransaction {
	m.calls++
	return m.transactions
}

// mockPParamsProvider implements ProtocolParamsProvider for testing.
type mockPParamsProvider struct {
	pparams lcommon.ProtocolParameters
}

func (m *mockPParamsProvider) GetCurrentPParams() lcommon.ProtocolParameters {
	return m.pparams
}

func (m *mockPParamsProvider) ProtocolParamsForSlot(
	_ uint64,
) lcommon.ProtocolParameters {
	return m.pparams
}

// mockChainTip implements ChainTipProvider for testing.
type mockChainTip struct {
	tip ochainsync.Tip
}

func (m *mockChainTip) Tip() ochainsync.Tip {
	return m.tip
}

type advancingChainTip struct {
	initial   ochainsync.Tip
	advanced  ochainsync.Tip
	advanceAt int
	calls     int
}

func (m *advancingChainTip) Tip() ochainsync.Tip {
	m.calls++
	if m.calls >= m.advanceAt {
		return m.advanced
	}
	return m.initial
}

type lockedChainTip struct {
	mu              sync.Mutex
	initial         ochainsync.Tip
	advanced        ochainsync.Tip
	withTipCalls    int
	advanceRequest  chan struct{}
	advanceComplete chan struct{}
}

func (m *lockedChainTip) Tip() ochainsync.Tip {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.initial
}

func (m *lockedChainTip) WithTip(fn func(ochainsync.Tip) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.withTipCalls++
	close(m.advanceRequest)
	return fn(m.initial)
}

func (m *lockedChainTip) requestAdvance() {
	<-m.advanceRequest
	m.mu.Lock()
	m.initial = m.advanced
	m.mu.Unlock()
	close(m.advanceComplete)
}

type reentrantChainTip struct {
	tip         ochainsync.Tip
	callback    func() error
	callbackErr error
	called      bool
}

func (m *reentrantChainTip) Tip() ochainsync.Tip {
	if !m.called {
		m.called = true
		m.callbackErr = m.callback()
	}
	return m.tip
}

// mockEpochNonceProvider implements EpochNonceProvider for testing.
type mockEpochNonceProvider struct {
	epoch           uint64
	nonce           []byte
	nonces          map[uint64][]byte
	slotsPerEpoch   uint64
	epochForSlotErr error
	requestedEpochs []uint64
}

func (m *mockEpochNonceProvider) CurrentEpoch() uint64 {
	return m.epoch
}

func (m *mockEpochNonceProvider) EpochForSlot(slot uint64) (uint64, error) {
	if m.epochForSlotErr != nil {
		return 0, m.epochForSlotErr
	}
	if m.slotsPerEpoch == 0 {
		return m.epoch, nil
	}
	return slot / m.slotsPerEpoch, nil
}

func (m *mockEpochNonceProvider) EpochNonce(epoch uint64) []byte {
	m.requestedEpochs = append(m.requestedEpochs, epoch)
	if m.nonces != nil {
		return m.nonces[epoch]
	}
	return m.nonce
}

// setupTestCredentials creates a temporary directory with test key files
// and returns loaded pool credentials for testing.
func setupTestCredentials(t *testing.T) *PoolCredentials {
	t.Helper()
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(100, 62, time.Second, time.Unix(0, 0)),
		0,
	))
	return creds
}

func setupCredentialValidationBuilder(
	t *testing.T,
	creds *PoolCredentials,
) *DefaultBlockBuilder {
	t.Helper()
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{
			pparams: &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: 62000000,
					Steps:  20000000000,
				},
			},
		},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)
	return builder
}

func TestNewDefaultBlockBuilder(t *testing.T) {
	creds := setupTestCredentials(t)

	mempool := &mockMempool{}
	pparams := &mockPParamsProvider{}
	chainTip := &mockChainTip{}
	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	// Test missing mempool
	_, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         nil,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mempool provider is required")

	// Test missing pparams provider
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: nil,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "protocol params provider is required")

	// Test missing chain tip provider
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        nil,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "chain tip provider is required")

	// Test missing epoch nonce provider
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      nil,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "epoch nonce provider is required")

	// Test missing credentials
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     nil,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool credentials are required")

	// Test successful creation
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)
	assert.NotNil(t, builder)
}

func TestExportedBuildersEnforceProtocolKESLifetime(t *testing.T) {
	tests := []struct {
		name        string
		start       uint64
		max         uint64
		period      uint64
		wantErr     string
		wantMempool int
	}{
		{
			name:    "pre-start",
			start:   1,
			max:     2,
			period:  0,
			wantErr: "not valid before",
		},
		{
			name:        "start",
			start:       0,
			max:         2,
			period:      0,
			wantMempool: 1,
		},
		{
			name:        "last-valid",
			start:       0,
			max:         2,
			period:      1,
			wantMempool: 1,
		},
		{
			name:    "expiry",
			start:   0,
			max:     2,
			period:  2,
			wantErr: "operational certificate expired",
		},
	}
	builders := []struct {
		name string
		call func(*DefaultBlockBuilder, uint64) (ledger.Block, []byte, error)
	}{
		{
			name: "BuildBlock",
			call: func(
				builder *DefaultBlockBuilder,
				period uint64,
			) (ledger.Block, []byte, error) {
				return builder.BuildBlock(1001, period)
			},
		},
		{
			name: "BuildBlockWithLeios",
			call: func(
				builder *DefaultBlockBuilder,
				period uint64,
			) (ledger.Block, []byte, error) {
				return builder.BuildBlockWithLeios(
					1001,
					period,
					LeiosBlockData{},
				)
			},
		},
	}

	for _, entrypoint := range builders {
		for _, test := range tests {
			t.Run(entrypoint.name+"/"+test.name, func(t *testing.T) {
				creds := setupTestCredentials(t)
				creds.mu.Lock()
				creds.generation++
				creds.opCertStartKES = test.start
				creds.maxKESEvolutions = test.max
				creds.opCertExpiryKES = test.start + test.max
				creds.opCertValidated = true
				creds.mu.Unlock()

				mempool := &mockMempool{
					transactions: []MempoolTransaction{},
				}
				builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
					Mempool: mempool,
					PParamsProvider: &mockPParamsProvider{
						pparams: &dijkstra.DijkstraProtocolParameters{
							ConwayProtocolParameters: conway.ConwayProtocolParameters{
								MaxTxSize:        16384,
								MaxBlockBodySize: 90112,
								ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
									Major: 10,
								},
							},
						},
					},
					ChainTip: &mockChainTip{
						tip: ochainsync.Tip{
							Point: ocommon.Point{
								Slot: 1000,
								Hash: make([]byte, 32),
							},
							BlockNumber: 100,
						},
					},
					EpochNonce: &mockEpochNonceProvider{
						epoch: 1,
						nonce: make([]byte, 32),
					},
					Credentials: creds,
				})
				require.NoError(t, err)
				if test.wantErr == "" && test.period > 0 {
					require.NoError(t, creds.UpdateKESPeriod(test.period))
				}

				block, blockCbor, err := entrypoint.call(builder, test.period)
				if test.wantErr != "" {
					require.ErrorContains(t, err, test.wantErr)
					require.Nil(t, block)
					require.Nil(t, blockCbor)
				} else {
					require.NoError(t, err)
					require.NotNil(t, block)
					require.NotEmpty(t, blockCbor)
				}
				require.Equal(t, test.wantMempool, mempool.calls)
			})
		}
	}
}

func TestDefaultBuilderRejectsReentrantProviderReload(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))
	genesis := synthGenesis(100, 62, time.Second, time.Unix(0, 0))
	require.NoError(t, creds.ValidateKESPeriod(genesis, 0))

	chainTip := &reentrantChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
		callback: func() error {
			if err := creds.LoadFromFiles(
				vrfPath,
				kesPath,
				opCertPath,
			); err != nil {
				return err
			}
			return creds.ValidateKESPeriod(genesis, 0)
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{
			pparams: &dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					MaxTxSize:        16384,
					MaxBlockBodySize: 90112,
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: 10,
					},
				},
			},
		},
		ChainTip: chainTip,
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	type buildResult struct {
		block ledger.Block
		cbor  []byte
		err   error
	}
	resultCh := make(chan buildResult, 1)
	go func() {
		block, blockCbor, err := builder.BuildBlock(1001, 0)
		resultCh <- buildResult{block: block, cbor: blockCbor, err: err}
	}()
	result := dingotestutil.RequireReceive(
		t,
		resultCh,
		dingotestutil.AsyncWait,
		"reentrant default-builder provider reload completion",
	)
	require.ErrorContains(t, result.err, "credential generation changed")
	require.Nil(t, result.block)
	require.Nil(t, result.cbor)
	require.NoError(t, chainTip.callbackErr)
}

func TestBuildBlockEmptyMempool(t *testing.T) {
	creds := setupTestCredentials(t)

	// Setup mocks
	mempool := &mockMempool{transactions: []MempoolTransaction{}}

	// Create Conway protocol parameters
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	// Build a block with empty mempool
	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	assert.NotNil(t, block)
	assert.NotEmpty(t, blockCbor)

	// Verify block properties
	assert.Equal(t, uint64(1001), block.SlotNumber())
	assert.Equal(t, uint64(101), block.BlockNumber())
	assert.Equal(t, 0, len(block.Transactions()))
}

func TestBuildBlockRejectsTipChangeBeforeSigning(t *testing.T) {
	creds := setupTestCredentials(t)
	initial := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: bytes.Repeat([]byte{0x01}, 32),
		},
		BlockNumber: 100,
	}
	advanced := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 1001,
			Hash: bytes.Repeat([]byte{0x02}, 32),
		},
		BlockNumber: 101,
	}
	chainTip := &advancingChainTip{
		initial:   initial,
		advanced:  advanced,
		advanceAt: 3,
	}
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip:        chainTip,
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.ErrorIs(t, err, errParentChangedDuringBuild)
	assert.Nil(t, block)
	assert.Nil(t, blockCbor)
	assert.GreaterOrEqual(t, chainTip.calls, 3)
}

func TestBuildBlockBindsSigningToTipLock(t *testing.T) {
	creds := setupTestCredentials(t)
	initial := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: bytes.Repeat([]byte{0x01}, 32),
		},
		BlockNumber: 100,
	}
	chainTip := &lockedChainTip{
		initial: initial,
		advanced: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1001,
				Hash: bytes.Repeat([]byte{0x02}, 32),
			},
			BlockNumber: 101,
		},
		advanceRequest:  make(chan struct{}),
		advanceComplete: make(chan struct{}),
	}
	go chainTip.requestAdvance()
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip:        chainTip,
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.NotEmpty(t, blockCbor)
	assert.Equal(t, 1, chainTip.withTipCalls)
	select {
	case <-chainTip.advanceComplete:
	case <-time.After(time.Second):
		t.Fatal("tip advance did not wait for signing critical section")
	}
}

func TestBuildBlockRequiresLiveTipParentBelowBlockSlot(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		slot                        uint64
		genesis, withLeios, wantErr bool
	}{
		{name: "same slot", slot: 1000, wantErr: true},
		{name: "earlier slot", slot: 999, wantErr: true},
		{name: "later slot", slot: 1001},
		{name: "genesis origin", slot: 0, genesis: true},
		{name: "later slot through Leios entrypoint", slot: 1001, withLeios: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creds := setupTestCredentials(t)
			pparams := &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: 62000000,
					Steps:  20000000000,
				},
			}
			hash := bytes.Repeat([]byte{1}, 32)
			if tc.genesis {
				hash = nil
			}
			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool: &mockMempool{},
				PParamsProvider: &mockPParamsProvider{
					pparams: pparams,
				},
				ChainTip: &mockChainTip{
					tip: ochainsync.Tip{
						Point:       ocommon.Point{Slot: 1000, Hash: hash},
						BlockNumber: 100,
					},
				},
				EpochNonce: &mockEpochNonceProvider{
					epoch: 1,
					nonce: make([]byte, 32),
				},
				Credentials: creds,
			})
			require.NoError(t, err)
			var block ledger.Block
			var blockCbor []byte
			if tc.withLeios {
				block, blockCbor, err = builder.BuildBlockWithLeios(
					tc.slot,
					0,
					LeiosBlockData{},
				)
			} else {
				block, blockCbor, err = builder.BuildBlock(tc.slot, 0)
			}
			if tc.wantErr {
				require.ErrorIs(t, err, errParentSlotNotBelowBlock)
				assert.Nil(t, block)
				assert.Nil(t, blockCbor)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, block)
			assert.NotEmpty(t, blockCbor)
			if tc.genesis {
				assert.Equal(t, uint64(0), block.BlockNumber())
				assert.Empty(t, block.PrevHash())
			} else {
				assert.Equal(t, uint64(101), block.BlockNumber())
				assert.Greater(t, block.SlotNumber(), uint64(1000))
			}
		})
	}
}

func TestBuildBlockUsesSlotEpochForVRFNonce(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	epochNonce := &mockEpochNonceProvider{
		epoch:         10, // ledger state has not rolled over yet
		slotsPerEpoch: 100,
		nonces: map[uint64][]byte{
			10: bytes.Repeat([]byte{0x10}, 32),
			11: bytes.Repeat([]byte{0x11}, 32),
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1099,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce:  epochNonce,
		Credentials: creds,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1100, 0)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Equal(t, []uint64{11}, epochNonce.requestedEpochs)
}

func TestBuildBlockPropagatesEpochForSlotError(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	sentinelErr := errors.New("epoch resolution failed")
	epochNonce := &mockEpochNonceProvider{
		epoch:           10,
		nonce:           make([]byte, 32),
		epochForSlotErr: sentinelErr,
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce:  epochNonce,
		Credentials: creds,
	})
	require.NoError(t, err)

	_, _, err = builder.BuildBlock(1001, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinelErr)
	require.Empty(t, epochNonce.requestedEpochs,
		"EpochNonce must not be queried when EpochForSlot fails")
}

func TestBuildBlockUsesDingoProtocolMinor(t *testing.T) {
	creds := setupTestCredentials(t)

	tests := []struct {
		name         string
		major        uint
		pparamMinor  uint
		expectedSlot uint64
	}{
		{
			name:         "overwrites zero pparam minor",
			major:        9,
			pparamMinor:  0,
			expectedSlot: 1001,
		},
		{
			name:         "overwrites non-zero pparam minor",
			major:        9,
			pparamMinor:  5,
			expectedSlot: 1002,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pparams := &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: tt.major,
					Minor: tt.pparamMinor,
				},
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: 62000000,
					Steps:  20000000000,
				},
			}
			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool: &mockMempool{
					transactions: []MempoolTransaction{},
				},
				PParamsProvider: &mockPParamsProvider{pparams: pparams},
				ChainTip: &mockChainTip{
					tip: ochainsync.Tip{
						Point: ocommon.Point{
							Slot: 1000,
							Hash: make([]byte, 32),
						},
						BlockNumber: 100,
					},
				},
				EpochNonce: &mockEpochNonceProvider{
					epoch: 1,
					nonce: make([]byte, 32),
				},
				Credentials: creds,
			})
			require.NoError(t, err)

			_, blockCbor, err := builder.BuildBlock(tt.expectedSlot, 0)
			require.NoError(t, err)

			decodedBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
			require.NoError(t, err)

			protoVersion := decodedBlock.BlockHeader.Body.ProtoVersion
			assert.Equal(t, uint64(tt.major), protoVersion.Major)
			assert.Equal(
				t,
				dingoversion.BlockHeaderProtocolMinor,
				protoVersion.Minor,
			)
		})
	}
}

func TestBuildBlockMissingVRFKey(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.mu.Lock()
	creds.generation++
	creds.vrfVKey = nil
	creds.mu.Unlock()
	builder := setupCredentialValidationBuilder(t, creds)

	_, _, err := builder.BuildBlock(1001, 0)
	require.ErrorContains(t, err, "VRF verification key not loaded")
}

func TestBuildBlockInvalidColdVKeySize(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.mu.Lock()
	creds.generation++
	creds.opCert.ColdVKey = make([]byte, 16)
	creds.mu.Unlock()
	builder := setupCredentialValidationBuilder(t, creds)

	_, _, err := builder.BuildBlock(1001, 0)
	require.ErrorContains(t, err, "invalid cold verification key size")
}

func TestBuildBlockInvalidVRFVKeySize(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.mu.Lock()
	creds.generation++
	creds.vrfVKey = make([]byte, 16)
	creds.mu.Unlock()
	builder := setupCredentialValidationBuilder(t, creds)

	_, _, err := builder.BuildBlock(1001, 0)
	require.ErrorContains(t, err, "invalid VRF verification key size")
}

func TestBuildBlockTxExceedsMaxSize(t *testing.T) {
	creds := setupTestCredentials(t)

	// Create a transaction that's larger than MaxTxSize
	largeTxCbor := make([]byte, 20000) // 20KB, exceeds 16KB MaxTxSize

	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "large_tx",
				Cbor: largeTxCbor,
				Type: conway.TxTypeConway,
			},
		},
	}

	// Set small MaxTxSize to force skipping
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	// Build block - oversized tx should be skipped
	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	// Block should have no transactions (oversized tx was skipped)
	assert.Equal(t, 0, len(block.Transactions()))
}

func TestBuildBlockNonConwayParams(t *testing.T) {
	creds := setupTestCredentials(t)

	// Setup mocks with nil pparams (simulating error)
	mempool := &mockMempool{transactions: []MempoolTransaction{}}
	pparamsProvider := &mockPParamsProvider{pparams: nil}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	// Build should fail with nil pparams
	_, _, err = builder.BuildBlock(1001, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get protocol parameters")
}

func TestBuildBlockCborRoundTrip(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	t.Run("empty mempool", func(t *testing.T) {
		mempool := &mockMempool{transactions: []MempoolTransaction{}}

		builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
			Mempool:         mempool,
			PParamsProvider: pparamsProvider,
			ChainTip:        chainTip,
			EpochNonce:      epochNonce,
			Credentials:     creds,
		})
		require.NoError(t, err)

		block, blockCbor, err := builder.BuildBlock(1001, 0)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.NotEmpty(t, blockCbor)

		decodedBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
		require.NoError(t, err)

		assert.Equal(t, block.SlotNumber(), decodedBlock.SlotNumber())
		assert.Equal(t, block.BlockNumber(), decodedBlock.BlockNumber())
		assert.Equal(t, 0, len(decodedBlock.Transactions()))

		reencodedCbor := decodedBlock.Cbor()
		assert.Equal(
			t,
			blockCbor,
			reencodedCbor,
			"CBOR round-trip should produce identical bytes",
		)
	})

	t.Run("with transactions", func(t *testing.T) {
		txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
		txCbor2 := makeMinimalTxCbor(t, 0x02, 0)

		mempool := &mockMempool{
			transactions: []MempoolTransaction{
				{Hash: "tx1", Cbor: txCbor1, Type: conway.TxTypeConway},
				{Hash: "tx2", Cbor: txCbor2, Type: conway.TxTypeConway},
			},
		}

		builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
			Mempool:         mempool,
			PParamsProvider: pparamsProvider,
			ChainTip:        chainTip,
			EpochNonce:      epochNonce,
			Credentials:     creds,
		})
		require.NoError(t, err)

		block, blockCbor, err := builder.BuildBlock(1001, 0)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.NotEmpty(t, blockCbor)

		assert.Equal(t, 2, len(block.Transactions()))

		decodedBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
		require.NoError(t, err)

		assert.Equal(t, block.SlotNumber(), decodedBlock.SlotNumber())
		assert.Equal(t, block.BlockNumber(), decodedBlock.BlockNumber())
		assert.Equal(t, 2, len(decodedBlock.Transactions()))

		reencodedCbor := decodedBlock.Cbor()
		assert.Equal(
			t,
			blockCbor,
			reencodedCbor,
			"CBOR round-trip should produce identical bytes",
		)
	})
}

// makeMinimalTxCbor creates a minimal valid Conway transaction CBOR.
// The txID byte distinguishes transactions. The padding parameter adds
// extra bytes to the transaction body via a metadata field, allowing
// size-based tests to control transaction size.
func makeMinimalTxCbor(t *testing.T, txID byte, padding int) []byte {
	t.Helper()

	txHash := make([]byte, 32)
	txHash[0] = txID

	// Conway transaction body: {0: inputs, 1: outputs, 2: fee}
	// Inputs are encoded as a tagged set (tag 258)
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{txHash, uint64(0)}},
		},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1000000)}},
		2: uint64(200000),
	}

	// Add padding via an output with a large address if needed
	if padding > 0 {
		addr := make([]byte, max(padding, 29))
		addr[0] = 0x61 // Shelley enterprise address header byte
		bodyMap[1] = []any{
			[]any{addr, uint64(1000000)},
		}
	}

	// Full Conway tx: [body, witnesses, isValid, auxData]
	txArr := []any{bodyMap, map[uint]any{}, true, nil}
	txCbor, err := cbor.Encode(txArr)
	require.NoError(t, err)

	// Verify it actually decodes
	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err, "generated CBOR must decode as a valid Conway tx")

	return txCbor
}

// TestComputeConwayBlockBodyHashProducesValidatingBlock exercises the
// dev-mode forging path in ledger/state.go: a block assembled from typed
// ConwayTransactionBody/ConwayTransactionWitnessSet values (rather than
// through Builder) must carry a body hash that gouroboros's own decode
// path accepts, or every dev-mode forged block fails immediately with a
// body-hash mismatch.
func TestComputeConwayBlockBodyHashProducesValidatingBlock(t *testing.T) {
	txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
	txCbor2 := makeMinimalTxCbor(t, 0x02, 0)

	tx1, err := conway.NewConwayTransactionFromCbor(txCbor1)
	require.NoError(t, err)
	tx2, err := conway.NewConwayTransactionFromCbor(txCbor2)
	require.NoError(t, err)

	txBodies := []conway.ConwayTransactionBody{tx1.Body, tx2.Body}
	witnessSets := []conway.ConwayTransactionWitnessSet{
		tx1.WitnessSet,
		tx2.WitnessSet,
	}
	var metadataSet lcommon.TransactionMetadataSet

	bodyHash, bodySize, err := ComputeConwayBlockBodyHash(
		txBodies,
		witnessSets,
		metadataSet,
	)
	require.NoError(t, err)
	assert.NotZero(t, bodySize)
	assert.NotEqual(
		t,
		lcommon.Blake2b256{},
		bodyHash,
		"must not be the zero placeholder",
	)

	header := &conway.ConwayBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber: 101,
				Slot:        1001,
				PrevHash:    lcommon.NewBlake2b256(make([]byte, 32)),
				IssuerVkey:  lcommon.IssuerVkey{},
				VrfKey:      []byte{},
				VrfResult: lcommon.VrfResult{
					Output: lcommon.Blake2b256{}.Bytes(),
				},
				BlockBodySize: bodySize,
				BlockBodyHash: bodyHash,
				OpCert:        babbage.BabbageOpCert{},
				ProtoVersion:  babbage.BabbageProtoVersion{Major: 10},
			},
			Signature: []byte{},
		},
	}
	block := &conway.ConwayBlock{
		BlockHeader:            header,
		TransactionBodies:      txBodies,
		TransactionWitnessSets: witnessSets,
		TransactionMetadataSet: metadataSet,
		InvalidTransactions:    []uint{},
	}

	blockCbor, err := cbor.Encode(block)
	require.NoError(t, err)

	// This is exactly the round-trip that fails with "body hash
	// mismatch" if BlockBodyHash is the zero placeholder instead of a
	// real computed hash.
	decoded, err := conway.NewConwayBlockFromCbor(blockCbor)
	require.NoError(t, err)
	assert.Equal(t, 2, len(decoded.Transactions()))
}

func TestBuildBlockBlockSizeLimit(t *testing.T) {
	creds := setupTestCredentials(t)

	// Create valid transactions using minimal CBOR
	txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
	txCbor2 := makeMinimalTxCbor(t, 0x02, 0)
	txCbor3 := makeMinimalTxCbor(t, 0x03, 0)
	txSize := len(txCbor1)

	t.Logf(
		"tx CBOR size: %d bytes, hex: %s...",
		txSize,
		hex.EncodeToString(txCbor1[:min(32, len(txCbor1))]),
	)

	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{Hash: "tx1", Cbor: txCbor1, Type: conway.TxTypeConway},
			{Hash: "tx2", Cbor: txCbor2, Type: conway.TxTypeConway},
			{Hash: "tx3", Cbor: txCbor3, Type: conway.TxTypeConway},
		},
	}

	// MaxBlockBodySize allows 2 transactions but not 3
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        uint(txSize * 2),
		MaxBlockBodySize: uint(txSize*2 + txSize/2), // 2.5x tx size
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	// Block should include exactly 2 transactions (third excluded by size limit)
	assert.Equal(
		t, 2, len(block.Transactions()),
		"block should include 2 txs and exclude the 3rd due to size limit",
	)
}

// TestBuildBlockExcludesTransactionWhoseExactAssembledBodyExceedsLimit
// exercises the boundary a raw-CBOR-size approximation cannot see: a single
// minimal Conway tx's raw CBOR is 52 bytes, but the assembled block body
// re-wraps decoded fields into separate transaction-body/witness-set arrays,
// which is one byte larger (53) and not the same size as the raw tx CBOR. A
// MaxBlockBodySize set to exactly the raw size would pass a raw-sum
// approximation, but the build loop's segmented body-size accounting
// (segmentedBodySize) tracks the real assembled size per candidate
// transaction, so the transaction is excluded from the block before it is
// ever added rather than only being caught by the final assembled-size
// safety net after the whole block is built.
func TestBuildBlockExcludesTransactionWhoseExactAssembledBodyExceedsLimit(
	t *testing.T,
) {
	creds := setupTestCredentials(t)

	txCbor := makeMinimalTxCbor(t, 0x01, 0)
	rawSize := uint(len(txCbor))

	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{Hash: "tx1", Cbor: txCbor, Type: conway.TxTypeConway},
		},
	}

	// MaxBlockBodySize equals the transaction's raw CBOR length exactly, so
	// a raw-sum approximation would admit it, but the exact assembled body
	// is one byte larger and must be excluded.
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        rawSize,
		MaxBlockBodySize: rawSize,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	assert.Empty(
		t,
		block.Transactions(),
		"the only mempool transaction's exact assembled body exceeds MaxBlockBodySize and must be excluded, not silently included",
	)
}

// mockTxValidator implements TxValidator for testing. It rejects
// transactions whose hashes appear in the rejectHashes set.
type mockTxValidator struct {
	rejectHashes map[string]struct{}
}

func (v *mockTxValidator) ValidateTx(tx ledger.Transaction) error {
	if _, reject := v.rejectHashes[tx.Hash().String()]; reject {
		return errors.New("transaction no longer valid")
	}
	return nil
}

func (v *mockTxValidator) ValidateTxWithOverlay(
	tx ledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
	_ *utxoref.StateOverlay,
) error {
	return v.ValidateTx(tx)
}

// makeMinimalTxCborWithInput creates a minimal Conway transaction
// CBOR that spends a specific input (inputHash, inputIndex). This
// allows tests to construct double-spend scenarios.
func makeMinimalTxCborWithInput(
	t *testing.T,
	inputHash []byte,
	inputIndex uint64,
) []byte {
	t.Helper()

	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{inputHash, inputIndex}},
		},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1000000)}},
		2: uint64(200000),
	}

	txArr := []any{bodyMap, map[uint]any{}, true, nil}
	txCbor, err := cbor.Encode(txArr)
	require.NoError(t, err)

	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(
		t,
		err,
		"generated CBOR must decode as a valid Conway tx",
	)

	return txCbor
}

func TestBuildBlockRevalidation(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{
		epoch: 1,
		nonce: make([]byte, 32),
	}

	t.Run(
		"rejects invalid transactions",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
			txCbor2 := makeMinimalTxCbor(t, 0x02, 0)
			txCbor3 := makeMinimalTxCbor(t, 0x03, 0)

			// Decode tx2 to get its hash for the reject list
			decodedTx2, err := conway.NewConwayTransactionFromCbor(
				txCbor2,
			)
			require.NoError(t, err)
			tx2Hash := decodedTx2.Hash().String()

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx3",
						Cbor: txCbor3,
						Type: conway.TxTypeConway,
					},
				},
			}

			// Validator rejects tx2
			validator := &mockTxValidator{
				rejectHashes: map[string]struct{}{
					tx2Hash: {},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
				TxValidator:     validator,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			// tx2 should be excluded; tx1 and tx3 included
			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"block should include 2 txs (tx2 rejected by validator)",
			)
		},
	)

	t.Run(
		"rejects all invalid transactions",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
			txCbor2 := makeMinimalTxCbor(t, 0x02, 0)

			decodedTx1, err := conway.NewConwayTransactionFromCbor(
				txCbor1,
			)
			require.NoError(t, err)
			decodedTx2, err := conway.NewConwayTransactionFromCbor(
				txCbor2,
			)
			require.NoError(t, err)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			// Reject all
			validator := &mockTxValidator{
				rejectHashes: map[string]struct{}{
					decodedTx1.Hash().String(): {},
					decodedTx2.Hash().String(): {},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
				TxValidator:     validator,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				0,
				len(block.Transactions()),
				"block should have no txs when all fail re-validation",
			)
		},
	)

	t.Run(
		"nil validator skips re-validation",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCbor(t, 0x01, 0)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
				},
			}

			// No validator - should include all transactions
			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
				TxValidator:     nil,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"block should include tx when no validator is set",
			)
		},
	)
}

func TestBuildBlockDoubleSpendDetection(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{
		epoch: 1,
		nonce: make([]byte, 32),
	}

	t.Run(
		"detects same input in two transactions",
		func(t *testing.T) {
			// Both transactions spend the same UTxO (same input
			// hash and index). The second should be excluded.
			sharedInputHash := make([]byte, 32)
			sharedInputHash[0] = 0xAA
			txCbor1 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)
			txCbor2 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"block should include only the first tx; second is "+
					"a double-spend",
			)
		},
	)

	t.Run(
		"allows different inputs",
		func(t *testing.T) {
			// Two transactions spending different UTxOs should
			// both be included.
			input1 := make([]byte, 32)
			input1[0] = 0x01
			input2 := make([]byte, 32)
			input2[0] = 0x02
			txCbor1 := makeMinimalTxCborWithInput(t, input1, 0)
			txCbor2 := makeMinimalTxCborWithInput(t, input2, 0)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"block should include both txs with different inputs",
			)
		},
	)

	t.Run(
		"same hash different index is not a double-spend",
		func(t *testing.T) {
			// Two transactions spending the same tx hash but
			// different output indexes are valid.
			sharedHash := make([]byte, 32)
			sharedHash[0] = 0xBB
			txCbor1 := makeMinimalTxCborWithInput(
				t,
				sharedHash,
				0,
			)
			txCbor2 := makeMinimalTxCborWithInput(
				t,
				sharedHash,
				1,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"block should include both txs spending different "+
					"outputs of the same tx",
			)
		},
	)

	t.Run(
		"three-way double-spend keeps only first",
		func(t *testing.T) {
			// Three transactions all spending the same UTxO.
			// Only the first should be included.
			sharedInputHash := make([]byte, 32)
			sharedInputHash[0] = 0xCC
			txCbor1 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)
			txCbor2 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)
			txCbor3 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx3",
						Cbor: txCbor3,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"block should include only the first tx; second "+
					"and third are double-spends",
			)
		},
	)
}

// makeMinimalTxCborWithExUnits creates a minimal valid Conway
// transaction CBOR that includes a redeemer with the given ExUnits.
// This allows tests to control the execution units declared in
// the transaction's witness set.
func makeMinimalTxCborWithExUnits(
	t *testing.T,
	txID byte,
	memory, steps int64,
) []byte {
	t.Helper()

	txHash := make([]byte, 32)
	txHash[0] = txID

	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{txHash, uint64(0)}},
		},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1000000)}},
		2: uint64(200000),
	}

	// Build the witness set with redeemers using raw CBOR.
	// Conway redeemers (witness set key 5) use a map format:
	//   { [tag, index] => [data, [memory, steps]] }
	// We encode the redeemer key [tag=0, index=0] and value
	// [data=0x40(empty bytes), [memory, steps]] as CBOR,
	// then construct the witness set map around it.
	redeemerKeyCbor, err := cbor.Encode(
		[]uint64{0, 0},
	)
	require.NoError(t, err)

	redeemerValCbor, err := cbor.Encode(
		[]any{
			[]byte{},
			[]any{uint64(memory), uint64(steps)},
		},
	)
	require.NoError(t, err)

	// Build the redeemer map CBOR manually: a1 = map(1)
	redeemerMapCbor := []byte{0xa1} // CBOR map with 1 entry
	redeemerMapCbor = append(
		redeemerMapCbor,
		redeemerKeyCbor...,
	)
	redeemerMapCbor = append(
		redeemerMapCbor,
		redeemerValCbor...,
	)

	// Build the witness set: {5: redeemerMap}
	// CBOR: a1 (map 1) 05 (key 5) <redeemerMapCbor>
	witnessSetCbor := []byte{0xa1, 0x05}
	witnessSetCbor = append(witnessSetCbor, redeemerMapCbor...)

	// Encode the body and combine into full tx array
	bodyCbor, err := cbor.Encode(bodyMap)
	require.NoError(t, err)

	// Full Conway tx: [body, witnesses, isValid, auxData]
	// CBOR: 84 (array 4) <body> <witnesses> F5 (true) F6 (null)
	txCbor := []byte{0x84}
	txCbor = append(txCbor, bodyCbor...)
	txCbor = append(txCbor, witnessSetCbor...)
	txCbor = append(txCbor, 0xF5) // true (isValid)
	txCbor = append(txCbor, 0xF6) // null (auxData)

	// Verify it actually decodes and has the expected ExUnits
	decoded, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err, "generated CBOR must decode as a valid Conway tx")

	var totalMem, totalSteps int64
	for _, redeemer := range decoded.WitnessSet.Redeemers().Iter() {
		totalMem += redeemer.ExUnits.Memory
		totalSteps += redeemer.ExUnits.Steps
	}
	require.Equal(t, memory, totalMem, "decoded memory should match")
	require.Equal(t, steps, totalSteps, "decoded steps should match")

	return txCbor
}

func TestBuildBlockExUnitsLimit(t *testing.T) {
	creds := setupTestCredentials(t)

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{
		epoch: 1,
		nonce: make([]byte, 32),
	}

	t.Run(
		"normal accumulation under limit",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCborWithExUnits(
				t, 0x01, 1000000, 50000000,
			)
			txCbor2 := makeMinimalTxCborWithExUnits(
				t, 0x02, 2000000, 80000000,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			pparams := &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: 62000000,
					Steps:  20000000000,
				},
			}

			builder, err := NewDefaultBlockBuilder(
				BlockBuilderConfig{
					Mempool:         mempool,
					PParamsProvider: &mockPParamsProvider{pparams: pparams},
					ChainTip:        chainTip,
					EpochNonce:      epochNonce,
					Credentials:     creds,
				},
			)
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"both txs should be included when under limit",
			)
		},
	)

	t.Run(
		"overflow memory at int64 boundary",
		func(t *testing.T) {
			// First tx uses nearly max int64 memory.
			// Second tx would overflow int64 if added.
			txCbor1 := makeMinimalTxCborWithExUnits(
				t, 0x01, math.MaxInt64-100, 100,
			)
			txCbor2 := makeMinimalTxCborWithExUnits(
				t, 0x02, 200, 100,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			pparams := &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: math.MaxInt64,
					Steps:  math.MaxInt64,
				},
			}

			builder, err := NewDefaultBlockBuilder(
				BlockBuilderConfig{
					Mempool:         mempool,
					PParamsProvider: &mockPParamsProvider{pparams: pparams},
					ChainTip:        chainTip,
					EpochNonce:      epochNonce,
					Credentials:     creds,
				},
			)
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			// tx1 fits (near max), tx2 would overflow so skipped
			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"second tx should be skipped to prevent "+
					"int64 memory overflow",
			)
		},
	)

	t.Run(
		"overflow steps at int64 boundary",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCborWithExUnits(
				t, 0x01, 100, math.MaxInt64-100,
			)
			txCbor2 := makeMinimalTxCborWithExUnits(
				t, 0x02, 100, 200,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			pparams := &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: math.MaxInt64,
					Steps:  math.MaxInt64,
				},
			}

			builder, err := NewDefaultBlockBuilder(
				BlockBuilderConfig{
					Mempool:         mempool,
					PParamsProvider: &mockPParamsProvider{pparams: pparams},
					ChainTip:        chainTip,
					EpochNonce:      epochNonce,
					Credentials:     creds,
				},
			)
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"second tx should be skipped to prevent "+
					"int64 steps overflow",
			)
		},
	)

	t.Run(
		"mixed valid and overflow transactions",
		func(t *testing.T) {
			// tx1: normal (included)
			// tx2: would overflow memory when combined with
			//      tx1 (skipped)
			// tx3: normal, fits after skipping tx2 (included)
			txCbor1 := makeMinimalTxCborWithExUnits(
				t, 0x01, math.MaxInt64-100, 100,
			)
			txCbor2 := makeMinimalTxCborWithExUnits(
				t, 0x02, 200, 100,
			)
			txCbor3 := makeMinimalTxCborWithExUnits(
				t, 0x03, 50, 100,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx3",
						Cbor: txCbor3,
						Type: conway.TxTypeConway,
					},
				},
			}

			pparams := &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: math.MaxInt64,
					Steps:  math.MaxInt64,
				},
			}

			builder, err := NewDefaultBlockBuilder(
				BlockBuilderConfig{
					Mempool:         mempool,
					PParamsProvider: &mockPParamsProvider{pparams: pparams},
					ChainTip:        chainTip,
					EpochNonce:      epochNonce,
					Credentials:     creds,
				},
			)
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			// tx1 included, tx2 skipped (overflow), tx3
			// included (fits)
			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"tx2 should be skipped (overflow) but tx3 "+
					"should still be included",
			)
		},
	)

	t.Run(
		"exceeds max block ex units limit",
		func(t *testing.T) {
			// Two transactions that individually fit but
			// together exceed the block ExUnits limit.
			txCbor1 := makeMinimalTxCborWithExUnits(
				t, 0x01, 40000000, 12000000000,
			)
			txCbor2 := makeMinimalTxCborWithExUnits(
				t, 0x02, 30000000, 10000000000,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			pparams := &conway.ConwayProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				MaxBlockExUnits: lcommon.ExUnits{
					Memory: 62000000,
					Steps:  20000000000,
				},
			}

			builder, err := NewDefaultBlockBuilder(
				BlockBuilderConfig{
					Mempool:         mempool,
					PParamsProvider: &mockPParamsProvider{pparams: pparams},
					ChainTip:        chainTip,
					EpochNonce:      epochNonce,
					Credentials:     creds,
				},
			)
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			// tx1 fits (40M < 62M, 12B < 20B), tx2 would
			// push total to 70M > 62M max memory
			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"second tx should be skipped because "+
					"combined ExUnits exceed block limit",
			)
		},
	)
}

func TestBuildBlockRevalidationAndDoubleSpend(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{
		epoch: 1,
		nonce: make([]byte, 32),
	}

	// Scenario: tx1 fails re-validation, tx2 and tx3 share an
	// input. Expected result: tx1 excluded by validator, tx2
	// included, tx3 excluded by double-spend detection.
	sharedInputHash := make([]byte, 32)
	sharedInputHash[0] = 0xDD
	differentInputHash := make([]byte, 32)
	differentInputHash[0] = 0xEE

	txCbor1 := makeMinimalTxCborWithInput(t, differentInputHash, 0)
	txCbor2 := makeMinimalTxCborWithInput(t, sharedInputHash, 0)
	txCbor3 := makeMinimalTxCborWithInput(t, sharedInputHash, 0)

	decodedTx1, err := conway.NewConwayTransactionFromCbor(txCbor1)
	require.NoError(t, err)

	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: txCbor1,
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx2",
				Cbor: txCbor2,
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx3",
				Cbor: txCbor3,
				Type: conway.TxTypeConway,
			},
		},
	}

	validator := &mockTxValidator{
		rejectHashes: map[string]struct{}{
			decodedTx1.Hash().String(): {},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
		TxValidator:     validator,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	// tx1 rejected by validator, tx3 rejected as double-spend of tx2
	assert.Equal(
		t,
		1,
		len(block.Transactions()),
		"block should include only tx2 (tx1 failed validation, "+
			"tx3 is a double-spend of tx2)",
	)
}

// sessionMockTxValidator implements both TxValidator and
// TxValidationSessionProvider, so it exercises the same
// withTxValidationSession path DefaultBlockBuilder.buildBlock uses with a
// real LedgerState. It lets tests observe how many sessions were opened for
// one BuildBlock call, and lets a hook run synchronously inside a validate
// call to simulate a concurrent ledger or chain-tip mutation landing
// mid-selection.
type sessionMockTxValidator struct {
	sessions      int
	validateCalls int
	// staleAfterCalls, when non-zero, makes stillCurrent() report false
	// once validateCalls reaches this count. This simulates a ledger
	// publication (new block, rollback, epoch transition, or protocol
	// parameter change) landing while transaction selection is still in
	// progress.
	staleAfterCalls int
	// alwaysStale makes stillCurrent() report false from the moment the
	// session opens, so a test can prove a candidate that validates
	// nothing never consults it.
	alwaysStale bool
	// onValidate, when set, runs synchronously inside each validate call
	// with the 1-indexed call number. Tests use it to mutate shared state
	// (e.g. the chain tip) partway through selection, deterministically,
	// rather than racing real goroutines against a sleep.
	onValidate func(callNumber int)
	// validateErr, when set, decides the result of each validate call by
	// transaction hash, so a test can reject one candidate and accept the
	// rest the way a UTxO consumed since mempool admission does.
	validateErr func(txHash string) error
	// validatedHashes records the hash of every transaction actually
	// re-validated, so a test can prove which candidates paid for it.
	validatedHashes []string
}

func (v *sessionMockTxValidator) ValidateTx(tx ledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil, nil)
}

// ValidateTxWithOverlay is only reached if the builder fails to discover the
// TxValidationSessionProvider capability and falls back to unpinned,
// per-transaction validation. TestBuildBlockPinsOneValidationSessionPerBlock
// asserts that does not happen.
func (v *sessionMockTxValidator) ValidateTxWithOverlay(
	_ ledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
	_ *utxoref.StateOverlay,
) error {
	return nil
}

func (v *sessionMockTxValidator) WithTxValidationSession(
	fn func(
		validate func(
			tx ledger.Transaction,
			consumed map[utxoref.Key]struct{},
			created map[utxoref.Key]lcommon.Utxo,
			accounts *utxoref.StateOverlay,
		) error,
		stillCurrent func() bool,
		_ func(func() error) (bool, error),
	) error,
) error {
	v.sessions++
	stale := v.alwaysStale
	validate := func(
		tx ledger.Transaction,
		_ map[utxoref.Key]struct{},
		_ map[utxoref.Key]lcommon.Utxo,
		_ *utxoref.StateOverlay,
	) error {
		v.validateCalls++
		if v.onValidate != nil {
			v.onValidate(v.validateCalls)
		}
		if v.staleAfterCalls > 0 && v.validateCalls >= v.staleAfterCalls {
			stale = true
		}
		if tx != nil {
			v.validatedHashes = append(
				v.validatedHashes,
				tx.Hash().String(),
			)
		}
		if v.validateErr != nil && tx != nil {
			return v.validateErr(tx.Hash().String())
		}
		return nil
	}
	stillCurrent := func() bool { return !stale }
	commitIfCurrent := func(commit func() error) (bool, error) {
		if !stillCurrent() {
			return false, nil
		}
		return true, commit()
	}
	return fn(validate, stillCurrent, commitIfCurrent)
}

var (
	_ TxValidator                 = (*sessionMockTxValidator)(nil)
	_ TxValidationSessionProvider = (*sessionMockTxValidator)(nil)
)

func threeTxMempoolForSelection(t *testing.T) *mockMempool {
	t.Helper()
	return &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: makeMinimalTxCbor(t, 0x01, 0),
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx2",
				Cbor: makeMinimalTxCbor(t, 0x02, 0),
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx3",
				Cbor: makeMinimalTxCbor(t, 0x03, 0),
				Type: conway.TxTypeConway,
			},
		},
	}
}

func selectionTestChainTip() *mockChainTip {
	return &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: bytes.Repeat([]byte{0xAA}, 32),
			},
			BlockNumber: 100,
		},
	}
}

func newSelectionTestBuilder(
	t *testing.T,
	mempool *mockMempool,
	chainTip *mockChainTip,
	validator TxValidator,
) *DefaultBlockBuilder {
	t.Helper()
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip:        chainTip,
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: setupTestCredentials(t),
		TxValidator: validator,
	})
	require.NoError(t, err)
	return builder
}

// TestBuildBlockPinsOneValidationSessionPerBlock verifies that a whole
// block's transaction selection runs inside a single validation session
// (the LedgerState-backed equivalent pins one ledger snapshot and one
// repeatable-read database transaction for it), rather than opening a fresh
// session per transaction. Before this, each transaction's
// ValidateTxWithOverlay call could observe a different ledger generation
// mid-selection.
func TestBuildBlockPinsOneValidationSessionPerBlock(t *testing.T) {
	validator := &sessionMockTxValidator{}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 3)
	require.Equal(
		t,
		1,
		validator.sessions,
		"all transactions in one block must share a single pinned validation session",
	)
	require.Equal(t, 3, validator.validateCalls)
}

// TestBuildBlockRejectsWhenValidationSnapshotGoesStale verifies that a
// ledger publication observed partway through transaction selection (a new
// block, rollback, epoch transition, or protocol parameter change) rejects
// the whole candidate block instead of silently returning one assembled
// from transactions checked against different generations.
func TestBuildBlockRejectsWhenValidationSnapshotGoesStale(t *testing.T) {
	validator := &sessionMockTxValidator{staleAfterCalls: 1}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.Error(t, err)
	require.Nil(t, block)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
}

// TestBuildBlockRejectsWhenSnapshotGoesStaleOnTheFinalCandidate covers the
// publication that lands while the last mempool transaction is being
// re-validated. stillCurrent() is consulted before each candidate, so no
// later iteration exists to observe it: only the check after the selection
// loop stands between that publication and a block returned from a
// superseded snapshot, bypassing the forge loop's in-slot retry. The
// candidate's own outcome must not matter -- a final candidate rejected by
// re-validation ends the pass the same way an accepted one does.
func TestBuildBlockRejectsWhenSnapshotGoesStaleOnTheFinalCandidate(
	t *testing.T,
) {
	for _, tc := range []struct {
		name          string
		rejectFinalTx bool
	}{
		{name: "final candidate accepted"},
		{name: "final candidate rejected", rejectFinalTx: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mempool := threeTxMempoolForSelection(t)
			validator := &sessionMockTxValidator{
				staleAfterCalls: len(mempool.transactions),
			}
			if tc.rejectFinalTx {
				validator.validateErr = func(string) error {
					if validator.validateCalls ==
						len(mempool.transactions) {
						return errors.New("input already spent")
					}
					return nil
				}
			}
			builder := newSelectionTestBuilder(
				t,
				mempool,
				selectionTestChainTip(),
				validator,
			)

			block, _, err := builder.BuildBlock(1001, 0)
			require.ErrorIs(t, err, errTxValidationSnapshotChanged)
			require.Nil(t, block)
			require.Equal(
				t,
				len(mempool.transactions),
				validator.validateCalls,
				"every candidate is validated before the publication is observed",
			)
			require.Equal(t, 1, validator.sessions)
		})
	}
}

// TestBuildBlockRejectsWhenParentChangesDuringSelection simulates a peer
// block advancing the primary chain tip while a locally-forged block is
// still selecting mempool transactions against the previously-current
// parent. The builder must reject the stale candidate itself rather than
// binding VRF/KES signing to a parent that chain adoption will refuse
// anyway once the tip has moved.
func TestBuildBlockRejectsWhenParentChangesDuringSelection(t *testing.T) {
	chainTip := selectionTestChainTip()
	validator := &sessionMockTxValidator{}
	validator.onValidate = func(callNumber int) {
		if callNumber != 1 {
			return
		}
		// A concurrent peer block lands on the chain mid-selection: the
		// tip this forge attempt already committed to as its parent is
		// no longer current.
		chainTip.tip = ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1001,
				Hash: bytes.Repeat([]byte{0xBB}, 32),
			},
			BlockNumber: 101,
		}
	}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		chainTip,
		validator,
	)

	block, _, err := builder.BuildBlock(1002, 0)
	require.Error(t, err)
	require.Nil(t, block)
	require.ErrorIs(t, err, errParentChangedDuringBuild)
}

// TestBuildBlockAcceptsStableParentAcrossSelection is the negative case for
// TestBuildBlockRejectsWhenParentChangesDuringSelection: when nothing moves
// the tip during selection, the recheck must not itself reject a
// legitimately unchanged parent.
func TestBuildBlockAcceptsStableParentAcrossSelection(t *testing.T) {
	validator := &sessionMockTxValidator{}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 3)
}

type ancestryTestTipValidator struct {
	sessionMockTxValidator
	appliedTip ochainsync.Tip
	k          int
}

func (v *ancestryTestTipValidator) ForgeTipSnapshot() (ochainsync.Tip, int) {
	return v.appliedTip, v.k
}

type ancestryTestChainTip struct {
	tip      ochainsync.Tip
	depth    uint64
	ancestor bool
}

func (c ancestryTestChainTip) Tip() ochainsync.Tip { return c.tip }

func (c ancestryTestChainTip) TipRelation(
	ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	return c.tip, c.depth, c.ancestor, nil
}

func TestBuilderRequiresAppliedAncestorWithinLocalBlockLimit(t *testing.T) {
	applied := ochainsync.Tip{
		Point:       ocommon.NewPoint(10, []byte("applied")),
		BlockNumber: 10,
	}
	primary := ochainsync.Tip{
		Point:       ocommon.NewPoint(12, []byte("primary")),
		BlockNumber: 12,
	}
	builder := &DefaultBlockBuilder{
		chainTip:    ancestryTestChainTip{tip: primary, depth: 2, ancestor: false},
		txValidator: &ancestryTestTipValidator{appliedTip: applied, k: 5},
	}

	err := builder.checkAppliedTipRelation(primary, primary.Point, false)
	require.ErrorContains(t, err, "exceeds the maximum unapplied block depth")

	builder.chainTip = ancestryTestChainTip{
		tip: primary, depth: 2, ancestor: true,
	}
	require.NoError(t, builder.checkAppliedTipRelation(primary, applied.Point, false))

	builder.chainTip = ancestryTestChainTip{
		tip: primary, depth: 6, ancestor: true,
	}
	require.ErrorContains(t,
		builder.checkAppliedTipRelation(primary, primary.Point, false),
		"exceeds the maximum unapplied block depth",
	)
}

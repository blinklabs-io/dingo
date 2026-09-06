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
	"encoding/binary"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leiosAuditAnnouncingBlockCbor builds the wire CBOR of a Dijkstra ranking
// block that announces an endorser block. The announcement lives in the Leios
// header extension, and DijkstraBlockHeader.MarshalCBOR only reproduces an
// extension it decoded, so the header body array has to be assembled by hand:
// the ten Babbage fields followed by leios_certified and leios_announcement.
func leiosAuditAnnouncingBlockCbor(
	t *testing.T,
	slot uint64,
	prevHash lcommon.Blake2b256,
	ebHash lcommon.Blake2b256,
	ebSize uint64,
) []byte {
	t.Helper()
	headerBody := babbage.BabbageBlockHeaderBody{
		BlockNumber: 3,
		Slot:        slot,
		PrevHash:    prevHash,
	}
	babbageBodyCbor, err := cbor.Encode(&headerBody)
	require.NoError(t, err)
	var bodyElems []cbor.RawMessage
	_, err = cbor.Decode(babbageBodyCbor, &bodyElems)
	require.NoError(t, err)
	certified, err := cbor.Encode(false)
	require.NoError(t, err)
	announcement, err := cbor.Encode([]any{ebHash.Bytes(), ebSize})
	require.NoError(t, err)
	bodyElems = append(
		bodyElems,
		cbor.RawMessage(certified),
		cbor.RawMessage(announcement),
	)
	extendedBody, err := cbor.Encode(bodyElems)
	require.NoError(t, err)
	headerCbor, err := cbor.Encode([]any{
		cbor.RawMessage(extendedBody),
		[]byte("leios-audit-signature"),
	})
	require.NoError(t, err)
	emptyBody, err := cbor.Encode(dijkstra.DijkstraBlockBody{})
	require.NoError(t, err)
	blockCbor, err := cbor.Encode([]any{
		cbor.RawMessage(headerCbor),
		cbor.RawMessage(emptyBody),
	})
	require.NoError(t, err)
	return blockCbor
}

// leiosAuditCertifyingBlock builds the 71-byte-body shape the Musashi
// prototype produces: a ranking block whose header certifies the endorser
// block its parent announced and whose own body carries no transactions.
func leiosAuditCertifyingBlock(
	t *testing.T,
	slot uint64,
	prevHash lcommon.Blake2b256,
) *dijkstra.DijkstraBlock {
	t.Helper()
	certified, err := cbor.Encode(true)
	require.NoError(t, err)
	return &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 4,
					Slot:        slot,
					PrevHash:    prevHash,
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{certified},
		},
	}
}

// leiosAuditFixture wires the fleet's cert-driven Leios shape onto the shared
// rollback fixture: an announcing ranking block on the chain, a certifying
// ranking block with an empty body, and an endorser-block provider holding the
// certified closure.
type leiosAuditFixture struct {
	*chainsyncRollbackFixture
	certRB     *dijkstra.DijkstraBlock
	certPoint  ocommon.Point
	ebTx       lcommon.Transaction
	providerOK *bool
	logs       *strings.Builder
}

func newLeiosAuditFixture(t *testing.T) *leiosAuditFixture {
	t.Helper()
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	logs := &strings.Builder{}
	// Debug level: the inconclusive verdict is deliberately quiet.
	ls.config.Logger = slog.New(slog.NewJSONHandler(
		logs,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))

	ebHash := lcommon.NewBlake2b256(testHashBytes("leios-audit-eb"))
	const announceSlot = 30
	announceCbor := leiosAuditAnnouncingBlockCbor(
		t,
		announceSlot,
		lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		ebHash,
		4096,
	)
	var announcing dijkstra.DijkstraBlock
	_, err := cbor.Decode(announceCbor, &announcing)
	require.NoError(t, err)
	// The announcement must survive the round trip, otherwise the fixture
	// would prove nothing about the certified closure.
	gotHash, _, ok := announcing.BlockHeader.LeiosAnnouncement()
	require.True(t, ok, "fixture parent must announce an endorser block")
	require.Equal(t, ebHash, gotHash)

	require.NoError(t, ls.chain.AddRawBlocks([]chain.RawBlock{{
		Slot:        announceSlot,
		Hash:        announcing.Hash().Bytes(),
		BlockNumber: 3,
		Type:        dijkstra.BlockTypeDijkstra,
		PrevHash:    fixture.currentTip.Point.Hash,
		Cbor:        announceCbor,
	}}))

	certRB := leiosAuditCertifyingBlock(t, 40, announcing.Hash())
	require.Empty(
		t,
		certRB.Transactions(),
		"a certifying ranking block carries no transactions of its own",
	)

	rawTx, _, ebTx := leiosApplyTestTx(t, 0x5A)
	providerOK := true
	ls.config.EndorserBlockProvider = func(
		hash []byte,
		slot uint64,
	) ([]cbor.RawMessage, bool) {
		if !providerOK {
			return nil, false
		}
		if string(hash) != string(ebHash.Bytes()) || slot != announceSlot {
			return nil, false
		}
		return []cbor.RawMessage{rawTx}, true
	}

	return &leiosAuditFixture{
		chainsyncRollbackFixture: fixture,
		certRB:                   certRB,
		certPoint: ocommon.NewPoint(
			certRB.SlotNumber(),
			certRB.Hash().Bytes(),
		),
		ebTx:       ebTx,
		providerOK: &providerOK,
		logs:       logs,
	}
}

// spenderBlockFor builds an ordinary ranking block that spends the first
// output of the endorser-block transaction.
func (f *leiosAuditFixture) spenderBlock(t *testing.T) BlockfetchEvent {
	t.Helper()
	body := &spliceAuditBlock{
		slot: 50,
		hash: lcommon.NewBlake2b256(testHashBytes("leios-audit-spender")),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes("leios-audit-spender-parent"),
		),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("leios-audit-spender-tx"),
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, f.ebTx.Hash().Bytes(), 0),
				},
			),
		},
	}
	return BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        body,
		Point:        ocommon.NewPoint(body.slot, body.hash.Bytes()),
	}
}

// TestContinuationAuditAcceptsEndorserBlockProducer is the regression for the
// false positive this package reported on every Leios cert-driven fork window.
//
// A certifying ranking block's body is empty: its transactions live in the
// endorser block it certifies, which LedgerState.applyEndorserBlock applies at
// ledger-apply time, long after blockfetch time when the audit runs. Building
// the in-window producer set from e.Block.Transactions() alone therefore never
// records an endorser-block transaction, and the ledger fallbacks miss too
// because the endorser block has not been applied yet. Every later ranking
// block spending an endorser-resident output was reported as having no
// producer on the local applied chain, on a node whose UTxO set was correct.
func TestContinuationAuditAcceptsEndorserBlockProducer(t *testing.T) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)
	ls.auditContinuationBlock(f.spenderBlock(t), true)

	assert.NotContains(
		t,
		f.logs.String(),
		"no producer on the local applied chain",
		"an endorser-block-resident producer certified in this window must not be reported",
	)
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	assert.Equal(t, 2, window.blocksSeen)
	// The input must resolve because the producer was recorded, not because
	// the window gave up: an audit that silently declares itself inconclusive
	// is also silent about a real splice.
	assert.False(
		t,
		window.endorserProducersPending,
		"the certified endorser block was cached, so the producer set is complete",
	)
	assert.Contains(
		t,
		window.producedTxs,
		string(f.ebTx.Hash().Bytes()),
		"the certified endorser block's transaction must be an in-window producer",
	)
}

// TestContinuationAuditTreatsPendingEndorserBlockAsInconclusive covers the
// other half: when the certified endorser block has not been fetched yet the
// audit cannot know the producer set, so it must say so at Debug rather than
// assert ledger corruption at Error.
func TestContinuationAuditTreatsPendingEndorserBlockAsInconclusive(
	t *testing.T,
) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	*f.providerOK = false
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)
	ls.auditContinuationBlock(f.spenderBlock(t), true)

	assert.NotContains(
		t,
		f.logs.String(),
		"no producer on the local applied chain",
		"an unresolvable producer set must not be reported as a missing producer",
	)
	record := findLogRecord(
		t,
		f.logs.String(),
		"cross-fork continuation audit inconclusive: certified endorser block not fetched yet",
	)
	assert.Equal(t, float64(50), record["block_slot"])
}

// TestContinuationAuditStillReportsMissingProducerOnLeios guards against the
// fix muting the diagnostic it was written for: with the certified closure
// resolved, an input whose producer is in neither the ranking block bodies,
// the endorser block, nor the ledger is still reported at Error.
func TestContinuationAuditStillReportsMissingProducerOnLeios(t *testing.T) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)

	missing := mustSpliceAuditInput(t, testHashBytes("leios-absent-producer"), 0)
	body := &spliceAuditBlock{
		slot: 50,
		hash: lcommon.NewBlake2b256(testHashBytes("leios-audit-splice")),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes("leios-audit-splice-parent"),
		),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("leios-audit-splice-tx"),
				[]lcommon.TransactionInput{missing},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        body,
		Point:        ocommon.NewPoint(body.slot, body.hash.Bytes()),
	}, true)

	report := findLogRecord(
		t,
		f.logs.String(),
		"continuation block spends an input with no producer on the local applied chain",
	)
	assert.Equal(t, missing.String(), report["input"])
}

// TestEndorserBlockTxIdsMatchDecodedTransactionHashes pins the audit's cheap
// producer-id derivation to the one the apply path produces. The audit hashes
// the transaction body straight out of the envelope instead of decoding every
// endorser transaction, which is only correct while a transaction id is
// blake2b-256 over its body CBOR. If that ever diverges, the audit would stop
// recognising endorser producers and the false positive would return, so the
// two derivations are compared directly.
func TestEndorserBlockTxIdsMatchDecodedTransactionHashes(t *testing.T) {
	rawTxs := make([]cbor.RawMessage, 0, 3)
	want := make([][]byte, 0, 3)
	for _, seed := range []byte{0x01, 0x02, 0x03} {
		raw, _, tx := leiosApplyTestTx(t, seed)
		rawTxs = append(rawTxs, raw)
		want = append(want, tx.Hash().Bytes())
	}
	// leios-fetch delivers each transaction CBOR-in-CBOR; the third entry
	// carries that wrapping so both envelope shapes are covered.
	wrapped, err := cbor.Encode([]byte(rawTxs[2]))
	require.NoError(t, err)
	rawTxs[2] = cbor.RawMessage(wrapped)

	got, err := endorserBlockTxIds(rawTxs)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestContinuationAuditOutcomesAreCounted covers the observability the
// diagnostic was missing: each audited input lands in exactly one labelled
// bucket, so "this node is not being audited" (inconclusive) is distinguishable
// from "this node is clean" and from "this node is spending unknown inputs".
func TestContinuationAuditOutcomesAreCounted(t *testing.T) {
	outcome := func(ls *LedgerState, result string) float64 {
		t.Helper()
		return promtestutil.ToFloat64(
			ls.metrics.continuationAuditOutcomes.WithLabelValues(result),
		)
	}

	t.Run("clean", func(t *testing.T) {
		f := newLeiosAuditFixture(t)
		ls := f.ls
		ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
		ls.auditContinuationBlock(BlockfetchEvent{
			ConnectionId: f.connId,
			Block:        f.certRB,
			Point:        f.certPoint,
		}, true)
		ls.auditContinuationBlock(f.spenderBlock(t), true)
		assert.Equal(t, float64(1), outcome(ls, "clean"))
		assert.Equal(t, float64(0), outcome(ls, "missing_producer"))
		assert.Equal(t, float64(0), outcome(ls, "inconclusive_eb_pending"))
	})

	t.Run("inconclusive", func(t *testing.T) {
		f := newLeiosAuditFixture(t)
		ls := f.ls
		*f.providerOK = false
		ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
		ls.auditContinuationBlock(BlockfetchEvent{
			ConnectionId: f.connId,
			Block:        f.certRB,
			Point:        f.certPoint,
		}, true)
		ls.auditContinuationBlock(f.spenderBlock(t), true)
		assert.Equal(
			t,
			float64(1),
			outcome(ls, "inconclusive_eb_pending"),
		)
		assert.Equal(t, float64(0), outcome(ls, "missing_producer"))
		assert.Equal(
			t,
			float64(0),
			promtestutil.ToFloat64(ls.metrics.continuationInputUnresolved),
		)
	})

	t.Run("missing_producer", func(t *testing.T) {
		f := newLeiosAuditFixture(t)
		ls := f.ls
		ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
		missing := mustSpliceAuditInput(
			t,
			testHashBytes("counted-absent-producer"),
			0,
		)
		body := &spliceAuditBlock{
			slot: 50,
			hash: lcommon.NewBlake2b256(testHashBytes("counted-splice")),
			txs: []lcommon.Transaction{
				mustSpliceAuditTx(
					t,
					testHashBytes("counted-splice-tx"),
					[]lcommon.TransactionInput{missing},
				),
			},
		}
		ls.auditContinuationBlock(BlockfetchEvent{
			ConnectionId: f.connId,
			Block:        body,
			Point:        ocommon.NewPoint(body.slot, body.hash.Bytes()),
		}, true)
		assert.Equal(t, float64(1), outcome(ls, "missing_producer"))
		assert.Equal(
			t,
			float64(1),
			promtestutil.ToFloat64(ls.metrics.continuationInputUnresolved),
		)
	})
}

// TestContinuationAuditCapDisarmIsExplicit covers the other half of the cap
// review: including endorser-block transactions in the producer set makes a
// busy Leios window reach continuationAuditMaxProducedTxs, so the disarm must
// be visible — a Warn line and a counted outcome — instead of the audit going
// quiet with no way to tell it apart from a clean node.
func TestContinuationAuditCapDisarmIsExplicit(t *testing.T) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	filler := make([]byte, 8)
	for i := range continuationAuditMaxProducedTxs {
		binary.BigEndian.PutUint64(filler, uint64(i))
		window.producedTxs[string(filler)] = struct{}{}
	}

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)

	assert.Equal(t, 0, window.remaining, "the window must disarm at the cap")
	record := findLogRecord(
		t,
		f.logs.String(),
		"disarming cross-fork continuation audit: producer set at capacity",
	)
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			ls.metrics.continuationAuditOutcomes.WithLabelValues(
				"disarmed_cap",
			),
		),
	)
}

// TestContinuationAuditAcceptsAnnouncedEndorserBlockProducer covers the
// forward/CIP path, where a ranking block applies the endorser block it
// announces itself rather than one its parent announced. The audit resolves
// the reference through the same leiosEndorserBlockForApply the apply path
// uses, so both Leios shapes are covered by one change; this pins the CIP half
// so a future divergence in that selector cannot silently reintroduce the
// false positive on the conformant path.
func TestContinuationAuditAcceptsAnnouncedEndorserBlockProducer(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	logs := &strings.Builder{}
	ls.config.Logger = slog.New(slog.NewJSONHandler(
		logs,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	ls.config.LeiosApplyEndorserBlockTxs = true

	ebHash := lcommon.NewBlake2b256(testHashBytes("cip-audit-eb"))
	certified, err := cbor.Encode(false)
	require.NoError(t, err)
	announcement, err := cbor.Encode([]any{ebHash.Bytes(), uint64(4096)})
	require.NoError(t, err)
	announcing := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 3,
					Slot:        30,
					PrevHash: lcommon.NewBlake2b256(
						fixture.currentTip.Point.Hash,
					),
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				cbor.RawMessage(certified),
				cbor.RawMessage(announcement),
			},
		},
	}

	rawTx, _, ebTx := leiosApplyTestTx(t, 0x7C)
	ls.config.EndorserBlockProvider = func(
		hash []byte,
		slot uint64,
	) ([]cbor.RawMessage, bool) {
		// On the CIP path the endorser block is bound to the announcing
		// block's own slot, not a parent's.
		if string(hash) != string(ebHash.Bytes()) ||
			slot != announcing.SlotNumber() {
			return nil, false
		}
		return []cbor.RawMessage{rawTx}, true
	}

	ls.armContinuationAudit(fixture.ancestorTip.Point, "test rollback")
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        announcing,
		Point: ocommon.NewPoint(
			announcing.SlotNumber(),
			announcing.Hash().Bytes(),
		),
	}, true)

	spender := &spliceAuditBlock{
		slot: 50,
		hash: lcommon.NewBlake2b256(testHashBytes("cip-audit-spender")),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("cip-audit-spender-tx"),
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, ebTx.Hash().Bytes(), 0),
				},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        spender,
		Point:        ocommon.NewPoint(spender.slot, spender.hash.Bytes()),
	}, true)

	assert.NotContains(
		t,
		logs.String(),
		"no producer on the local applied chain",
		"a producer in the endorser block this window announced must not be reported",
	)
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	assert.False(t, window.endorserProducersPending)
	assert.Contains(
		t,
		window.producedTxs,
		string(ebTx.Hash().Bytes()),
		"the announced endorser block's transaction must be an in-window producer",
	)
}

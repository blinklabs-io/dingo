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
	"strconv"
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
	certRB        *dijkstra.DijkstraBlock
	certPoint     ocommon.Point
	announceHash  lcommon.Blake2b256
	ebTx          lcommon.Transaction
	ebRepeatTx    lcommon.Transaction
	providerOK    *bool
	providerCalls *int
	logs          *strings.Builder
	// announceRaw is the ranking block whose announcement the certifying
	// block certifies, as it would be stored. Tests that exercise a parent
	// the block store cannot resolve yet add it partway through.
	announceRaw chain.RawBlock
}

func (f *leiosAuditFixture) addAnnouncingBlock(t *testing.T) {
	t.Helper()
	require.NoError(t, f.ls.chain.AddRawBlocks(
		[]chain.RawBlock{f.announceRaw},
	))
}

func newLeiosAuditFixture(t *testing.T) *leiosAuditFixture {
	t.Helper()
	return newLeiosAuditFixtureOpts(t, true)
}

// newLeiosAuditFixtureOpts optionally withholds the announcing ranking block
// from the block store, which is how a test reproduces a certifying block whose
// parent the audit cannot resolve at the moment it first tries.
func newLeiosAuditFixtureOpts(
	t *testing.T,
	insertAnnouncingBlock bool,
) *leiosAuditFixture {
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

	announceRaw := chain.RawBlock{
		Slot:        announceSlot,
		Hash:        announcing.Hash().Bytes(),
		BlockNumber: 3,
		Type:        dijkstra.BlockTypeDijkstra,
		PrevHash:    fixture.currentTip.Point.Hash,
		Cbor:        announceCbor,
	}
	if insertAnnouncingBlock {
		require.NoError(
			t,
			ls.chain.AddRawBlocks([]chain.RawBlock{announceRaw}),
		)
	}

	certRB := leiosAuditCertifyingBlock(t, 40, announcing.Hash())
	require.Empty(
		t,
		certRB.Transactions(),
		"a certifying ranking block carries no transactions of its own",
	)

	// Two transactions, so a cap test can offer the window one id it already
	// holds alongside one it does not.
	rawTx, _, ebTx := leiosApplyTestTx(t, 0x5A)
	rawRepeatTx, _, ebRepeatTx := leiosApplyTestTx(t, 0x5B)
	providerOK := true
	providerCalls := 0
	ls.config.EndorserBlockProvider = func(
		hash []byte,
		slot uint64,
	) ([]cbor.RawMessage, bool) {
		providerCalls++
		if !providerOK {
			return nil, false
		}
		if string(hash) != string(ebHash.Bytes()) || slot != announceSlot {
			return nil, false
		}
		return []cbor.RawMessage{rawTx, rawRepeatTx}, true
	}

	return &leiosAuditFixture{
		chainsyncRollbackFixture: fixture,
		certRB:                   certRB,
		certPoint: ocommon.NewPoint(
			certRB.SlotNumber(),
			certRB.Hash().Bytes(),
		),
		announceHash:  announcing.Hash(),
		ebTx:          ebTx,
		ebRepeatTx:    ebRepeatTx,
		providerOK:    &providerOK,
		providerCalls: &providerCalls,
		logs:          logs,
		announceRaw:   announceRaw,
	}
}

// spenderBlockFor builds an ordinary ranking block that spends the first
// output of the endorser-block transaction.
func (f *leiosAuditFixture) spenderBlock(t *testing.T) BlockfetchEvent {
	t.Helper()
	return f.spenderBlockAt(t, 50, "leios-audit-spender")
}

func (f *leiosAuditFixture) spenderBlockAt(
	t *testing.T,
	slot uint64,
	seed string,
) BlockfetchEvent {
	t.Helper()
	body := &spliceAuditBlock{
		slot: slot,
		hash: lcommon.NewBlake2b256(testHashBytes(seed)),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes(seed + "-parent"),
		),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes(seed+"-tx"),
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
		window.endorserProducersIncomplete(),
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

// continuationAuditFillProducers seeds the window's producer set with n
// synthetic ids, so a cap test does not have to audit a quarter of a million
// real transactions to reach the boundary.
func continuationAuditFillProducers(
	window *continuationAuditWindow,
	n int,
) {
	filler := make([]byte, 8)
	for i := range n {
		binary.BigEndian.PutUint64(filler, uint64(i))
		window.producedTxs[string(filler)] = struct{}{}
	}
}

// TestContinuationAuditCapDisarmIsExplicit covers the other half of the cap
// review: including endorser-block transactions in the producer set makes a
// busy Leios window reach continuationAuditMaxProducedTxs, so the disarm must
// be visible — a Warn line and a counted outcome — instead of the audit going
// quiet with no way to tell it apart from a clean node.
//
// The window is seeded one short of the cap so the spending body's own
// transaction fits and its input probe reaches the endorser-block drain; the
// endorser transaction is then the id that runs the set into the cap, which is
// the path a real Leios window disarms on.
func TestContinuationAuditCapDisarmIsExplicit(t *testing.T) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	continuationAuditFillProducers(
		window,
		continuationAuditMaxProducedTxs-1,
	)

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)
	ls.auditContinuationBlock(f.spenderBlock(t), true)

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

// TestContinuationAuditCapCountsOnlyNewProducers is the regression for the
// review finding on the cap check: it compared the producer set's size plus
// every id the block offered, so an id the set already held was charged against
// the cap a second time and a window whose producer set never grew could
// disarm itself.
//
// That is not hypothetical on the Leios path. The same transaction can appear
// in more than one endorser block — applyEndorserBlock carries
// deduplicateEndorserBlockTransactionIndexes for exactly that — and an endorser
// block is content-addressed, so the same closure can be referenced from more
// than one ranking block inside a window. The cap bounds the size of the set,
// so only ids the set does not already hold may count toward it.
//
// Same seeding as the disarm test above, one id short of the cap, except that
// the endorser block's transaction is already recorded. The drain therefore
// offers a repeat, the set cannot grow, and the window must stay armed.
func TestContinuationAuditCapCountsOnlyNewProducers(t *testing.T) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	// One of the endorser block's two transactions is already a producer.
	// Seeded two short of the cap, the window has room for exactly the two
	// ids it is about to be offered that it does not already hold: the
	// spending body's own transaction and the endorser block's other one.
	window.producedTxs[string(f.ebRepeatTx.Hash().Bytes())] = struct{}{}
	continuationAuditFillProducers(
		window,
		continuationAuditMaxProducedTxs-3,
	)
	require.Len(t, window.producedTxs, continuationAuditMaxProducedTxs-2)

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)
	ls.auditContinuationBlock(f.spenderBlock(t), true)

	assert.Positive(
		t,
		window.remaining,
		"a repeated endorser transaction must not disarm the window",
	)
	assert.Equal(t, 2, window.blocksSeen)
	assert.Len(
		t,
		window.producedTxs,
		continuationAuditMaxProducedTxs,
		"only the two ids the set did not already hold may have been added",
	)
	assert.Contains(t, window.producedTxs, string(f.ebTx.Hash().Bytes()))
	require.Positive(
		t,
		window.endorserResolutions,
		"the endorser block must actually have been drained, or this test proves nothing",
	)
	assert.NotContains(
		t,
		f.logs.String(),
		"disarming cross-fork continuation audit",
	)
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			ls.metrics.continuationAuditOutcomes.WithLabelValues(
				"disarmed_cap",
			),
		),
	)
}

// TestContinuationAuditResolvesEachEndorserBlockOnce is the cost regression for
// the review finding that the audit resolved a block's endorser block for every
// referencing ranking block, inside auditContinuationBlock while
// chainsyncBlockfetchMutex is held.
//
// A certified endorser block is content-addressed and can be certified from
// more than one ranking block in a window; resolving it costs a parent-block
// read plus a hash of every one of its transactions. Three certifying blocks
// over the same closure must cost exactly one resolution, not three.
func TestContinuationAuditResolvesEachEndorserBlockOnce(t *testing.T) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")

	for _, slot := range []uint64{40, 41, 42} {
		certRB := leiosAuditCertifyingBlock(t, slot, f.announceHash)
		ls.auditContinuationBlock(BlockfetchEvent{
			ConnectionId: f.connId,
			Block:        certRB,
			Point: ocommon.NewPoint(
				certRB.SlotNumber(),
				certRB.Hash().Bytes(),
			),
		}, true)
	}
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	require.Len(
		t,
		window.pendingEndorserRefs,
		1,
		"three blocks certifying the same closure must queue one reference",
	)

	ls.auditContinuationBlock(f.spenderBlock(t), true)

	assert.Equal(
		t,
		1,
		window.endorserResolutions,
		"the shared endorser block must be resolved exactly once per window",
	)
	assert.Equal(t, 1, *f.providerCalls)
	assert.Contains(t, window.producedTxs, string(f.ebTx.Hash().Bytes()))
	assert.NotContains(
		t,
		f.logs.String(),
		"no producer on the local applied chain",
	)
}

// TestContinuationAuditSkipsEndorserResolutionWithoutEndorserSpends is the
// other half of that finding: the resolution ran even when nothing in the
// window spent an endorser-resident output, so a node in a dense endorser-block
// backlog — exactly when the audit is armed — paid for it on every body for no
// diagnostic value.
//
// Here the spending body's producer is an ordinary in-window ranking-block
// transaction, so the audit must never touch the queued endorser block.
func TestContinuationAuditSkipsEndorserResolutionWithoutEndorserSpends(
	t *testing.T,
) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)

	producerTxId := testHashBytes("ordinary-in-window-producer")
	producerBlock := &spliceAuditBlock{
		slot: 45,
		hash: lcommon.NewBlake2b256(testHashBytes("ordinary-producer-block")),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				producerTxId,
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, producerTxId, 9),
				},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        producerBlock,
		Point: ocommon.NewPoint(
			producerBlock.slot,
			producerBlock.hash.Bytes(),
		),
	}, true)

	spender := &spliceAuditBlock{
		slot: 50,
		hash: lcommon.NewBlake2b256(testHashBytes("ordinary-spender-block")),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("ordinary-spender-tx"),
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, producerTxId, 0),
				},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        spender,
		Point:        ocommon.NewPoint(spender.slot, spender.hash.Bytes()),
	}, true)

	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	assert.Equal(
		t,
		0,
		window.endorserResolutions,
		"a window that spends nothing endorser-resident must not resolve any endorser block",
	)
	assert.Equal(t, 0, *f.providerCalls)
	assert.Len(
		t,
		window.pendingEndorserRefs,
		1,
		"the reference must stay queued, classified but unresolved",
	)
	assert.NotContains(
		t,
		f.logs.String(),
		"no producer on the local applied chain",
	)
}

// TestContinuationAuditEndorserResolutionIsBudgeted pins the per-block bound on
// endorser-block resolution, the analogue of continuationAuditMaxInputsPerBlock
// for the endorser path: one audited body may not resolve an unbounded number
// of endorser blocks under the blockfetch mutex. The overflow stays queued for
// a later body, the window reports inconclusive rather than missing while it is
// short, and the stop is counted.
func TestContinuationAuditEndorserResolutionIsBudgeted(t *testing.T) {
	f := newLeiosAuditFixture(t)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)

	// Queue one more distinct certified closure than a single body may
	// resolve. Their parents are absent from the chain, so each resolution
	// attempt is spent and accounted without needing a provider entry.
	queued := continuationAuditMaxEndorserBlocksPerBlock + 1
	for i := range queued {
		window.pendingEndorserRefs = append(
			window.pendingEndorserRefs,
			continuationAuditEndorserRef{
				certParentHash: testHashBytes(
					"budget-parent-" + strconv.Itoa(i),
				),
				blockSlot: uint64(60 + i),
			},
		)
		window.pendingEndorserSeen[window.pendingEndorserRefs[i].key()] = struct{}{}
	}

	ls.auditContinuationBlock(f.spenderBlock(t), true)

	assert.Equal(
		t,
		continuationAuditMaxEndorserBlocksPerBlock,
		window.endorserResolutions,
		"one body must not resolve more endorser blocks than its budget",
	)
	assert.Len(
		t,
		window.pendingEndorserRefs,
		queued,
		"nothing may be lost: the overflow stays queued, and the probed "+
			"references are requeued because their parents are not "+
			"resolvable yet",
	)
	assert.True(t, window.endorserProducersIncomplete())
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			ls.metrics.continuationAuditOutcomes.WithLabelValues(
				"ref_unresolvable",
			),
		),
		"a parent that is merely absent is retryable, not unresolvable",
	)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			ls.metrics.continuationAuditOutcomes.WithLabelValues(
				"skipped_budget",
			),
		),
	)
	assert.NotContains(
		t,
		f.logs.String(),
		"no producer on the local applied chain",
	)
}

// TestContinuationAuditAcceptsAnnouncedEndorserBlockProducer covers the
// forward/CIP path, where a ranking block applies the endorser block it
// announces itself rather than one its parent announced. The audit classifies
// the reference the same way the apply path selects it, so both Leios shapes
// are covered by one change; this pins the CIP half so a future divergence in
// that selector cannot silently reintroduce the false positive on the
// conformant path.
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
	providerCalls := 0
	ls.config.EndorserBlockProvider = func(
		hash []byte,
		slot uint64,
	) ([]cbor.RawMessage, bool) {
		providerCalls++
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
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	require.Equal(
		t,
		0,
		providerCalls,
		"an announced reference must be classified without being resolved",
	)

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
	assert.False(t, window.endorserProducersIncomplete())
	assert.Equal(t, 1, window.endorserResolutions)
	assert.Contains(
		t,
		window.producedTxs,
		string(ebTx.Hash().Bytes()),
		"the announced endorser block's transaction must be an in-window producer",
	)
}

// TestContinuationAuditRetriesUnresolvedCertifyingParent is the regression for
// the review finding that a transient parent lookup failure was permanent.
//
// A certifying ranking block names its certified closure only through its
// parent's announcement, so resolving it needs the parent out of the block
// store. When that lookup missed, the reference was dropped for the rest of the
// window: no later body retried it, so the window stayed inconclusive forever
// and could neither recognise the endorser-resident producer nor diagnose a
// genuine missing one. A miss is a transient state, not a verdict — the parent
// arrives — so the reference must be requeued and retried.
func TestContinuationAuditRetriesUnresolvedCertifyingParent(t *testing.T) {
	f := newLeiosAuditFixtureOpts(t, false)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)
	// First attempt: the parent is not in the block store, so the closure
	// cannot be named yet.
	ls.auditContinuationBlock(
		f.spenderBlockAt(t, 50, "retry-spender-before"),
		true,
	)
	require.Equal(t, 0, *f.providerCalls)
	require.True(
		t,
		window.endorserProducersIncomplete(),
		"an unresolvable parent leaves the producer set knowingly short",
	)
	require.Len(
		t,
		window.pendingEndorserRefs,
		1,
		"the reference must stay queued for a later attempt",
	)
	require.NotContains(
		t,
		f.logs.String(),
		"no producer on the local applied chain",
	)

	// A second certifying block over the same parent must not queue the
	// reference a second time: the drain took the dedupe entry when it tried,
	// and requeueing has to put it back.
	dupCertRB := leiosAuditCertifyingBlock(t, 41, f.announceHash)
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        dupCertRB,
		Point: ocommon.NewPoint(
			dupCertRB.SlotNumber(),
			dupCertRB.Hash().Bytes(),
		),
	}, true)
	require.Len(
		t,
		window.pendingEndorserRefs,
		1,
		"a requeued reference must still suppress its duplicates",
	)

	// The parent lands. The next audited body must retry and resolve.
	f.addAnnouncingBlock(t)
	ls.auditContinuationBlock(
		f.spenderBlockAt(t, 51, "retry-spender-after"),
		true,
	)

	assert.Equal(t, 1, *f.providerCalls)
	assert.Contains(
		t,
		window.producedTxs,
		string(f.ebTx.Hash().Bytes()),
		"the retried closure's transaction must become an in-window producer",
	)
	assert.False(
		t,
		window.endorserProducersIncomplete(),
		"with every reference resolved the producer set is complete again",
	)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			ls.metrics.continuationAuditOutcomes.WithLabelValues("clean"),
		),
	)
	assert.NotContains(
		t,
		f.logs.String(),
		"no producer on the local applied chain",
	)
}

// TestContinuationAuditRetriesParentAtMostOncePerBlock bounds the retry: a
// reference that cannot be resolved must not be re-probed for every unresolved
// input of the same body, only once per audited body, so a permanently absent
// parent costs one index miss per block rather than one per input.
func TestContinuationAuditRetriesParentAtMostOncePerBlock(t *testing.T) {
	f := newLeiosAuditFixtureOpts(t, false)
	ls := f.ls
	ls.armContinuationAudit(f.ancestorTip.Point, "test rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        f.certRB,
		Point:        f.certPoint,
	}, true)

	// A body with three unresolvable inputs: three drains, one lookup.
	body := &spliceAuditBlock{
		slot: 50,
		hash: lcommon.NewBlake2b256(testHashBytes("retry-multi-input")),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("retry-multi-input-tx"),
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, f.ebTx.Hash().Bytes(), 0),
					mustSpliceAuditInput(t, f.ebTx.Hash().Bytes(), 1),
					mustSpliceAuditInput(t, f.ebTx.Hash().Bytes(), 2),
				},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: f.connId,
		Block:        body,
		Point:        ocommon.NewPoint(body.slot, body.hash.Bytes()),
	}, true)

	assert.Equal(
		t,
		1,
		window.endorserResolutions,
		"one body must probe an unresolvable parent once, not once per input",
	)
	assert.Len(t, window.pendingEndorserRefs, 1)
}

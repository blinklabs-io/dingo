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
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// TestChainsyncRollbackToAbandonedForkDoesNotSpliceChain is the ledger-level
// regression for issue #3005.
//
// After the node abandons a fork, the rolled-back blocks stay resolvable
// through the chain manager's retained block cache with the block indexes the
// replacement fork now occupies. A peer that later asks the node to roll back
// to one of those abandoned blocks used to be obeyed: the chain truncated to
// the stale index and moved its tip to a block it no longer stored, so the next
// block was appended above a parent that is absent from the chain. That splice
// is what leaves a spender on the primary chain whose producing block was never
// applied, which the ledger reports as an unresolvable producer and can never
// replay past.
//
// The rollback must instead be refused as "point not found" so chainsync
// re-intersects with the peer.
func TestChainsyncRollbackToAbandonedForkDoesNotSpliceChain(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	abandonedPoint := fixture.currentTip.Point

	// Abandon the block at slot 20 and replace its index with a fork block.
	require.NoError(t, ls.chain.Rollback(fixture.ancestorTip.Point))
	forkHash := testHashBytes("splice-fork-block")
	require.NoError(t, ls.chain.AddRawBlocks(
		[]chain.RawBlock{
			{
				Slot:        21,
				Hash:        forkHash,
				BlockNumber: fixture.ancestorTip.BlockNumber + 1,
				Type:        1,
				PrevHash:    fixture.ancestorTip.Point.Hash,
				Cbor:        []byte{0x80},
			},
		},
	))
	forkTip := ls.chain.Tip()

	// The abandoned block is still resolvable by point, and still claims the
	// block index the fork block now holds. That is the precondition for the
	// splice.
	cached, err := ls.chain.BlockByPoint(abandonedPoint, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), cached.ID)

	err = ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        abandonedPoint,
	}, nil)
	require.NoError(t, err)

	assert.Equal(
		t,
		forkTip,
		ls.chain.Tip(),
		"rollback to an abandoned fork block must leave the chain tip alone",
	)
	tipBlock, err := ls.chain.BlockByPoint(ls.chain.Tip().Point, nil)
	require.NoError(t, err)
	assert.True(
		t,
		bytes.Equal(tipBlock.Hash, forkHash),
		"chain tip must still be the block the chain actually stores",
	)
}

// TestValidateRollbackRejectsAbandonedForkPoint covers the loop detector's
// crossability pre-check on the same state: a point resolvable only out of the
// retained cache must not be reported as a rollback the node can cross,
// otherwise the detector keeps re-applying the splice instead of breaking the
// loop.
func TestValidateRollbackRejectsAbandonedForkPoint(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	abandonedPoint := fixture.currentTip.Point

	require.NoError(t, ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, ls.chain.AddRawBlocks(
		[]chain.RawBlock{
			{
				Slot:        21,
				Hash:        testHashBytes("crossable-fork-block"),
				BlockNumber: fixture.ancestorTip.BlockNumber + 1,
				Type:        1,
				PrevHash:    fixture.ancestorTip.Point.Hash,
				Cbor:        []byte{0x80},
			},
		},
	))

	assert.False(
		t,
		ls.rollbackIsAppliable(abandonedPoint),
		"a rollback to an abandoned fork block is not crossable",
	)
	assert.True(
		t,
		ls.rollbackIsAppliable(fixture.ancestorTip.Point),
		"a rollback to a block still on the chain stays crossable",
	)
}

// spliceAuditBlock is a minimal ledger.Block carrying a fixed transaction set.
// Only the fields the continuation audit reads are meaningful; the transactions
// themselves come from the shared ouroboros-mock builders.
type spliceAuditBlock struct {
	txs      []lcommon.Transaction
	hash     lcommon.Blake2b256
	prevHash lcommon.Blake2b256
	slot     uint64
}

func (b *spliceAuditBlock) Hash() lcommon.Blake2b256     { return b.hash }
func (b *spliceAuditBlock) PrevHash() lcommon.Blake2b256 { return b.prevHash }
func (b *spliceAuditBlock) SlotNumber() uint64           { return b.slot }
func (b *spliceAuditBlock) BlockNumber() uint64          { return 1 }
func (b *spliceAuditBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *spliceAuditBlock) BlockBodySize() uint64 { return 0 }
func (b *spliceAuditBlock) Era() lcommon.Era      { return lcommon.Era{} }
func (b *spliceAuditBlock) Cbor() []byte          { return nil }
func (b *spliceAuditBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}
func (b *spliceAuditBlock) Header() lcommon.BlockHeader { return nil }
func (b *spliceAuditBlock) Type() int                   { return 0 }
func (b *spliceAuditBlock) Transactions() []lcommon.Transaction {
	return b.txs
}
func (b *spliceAuditBlock) Utxorpc() (*utxorpc.Block, error) { return nil, nil }

// spliceAuditAddr is an arbitrary well-formed testnet address; the mock
// transaction builder requires at least one output.
const spliceAuditAddr = "addr_test1qpe6s9amgfwtu9u6lqj998vke6uncswr4dg88qqft5d7f67kfjf77qy57hqhnefcqyy7hmhsygj9j38rj984hn9r57fswc4wg0"

func mustSpliceAuditTx(
	t *testing.T,
	txId []byte,
	inputs []lcommon.TransactionInput,
) lcommon.Transaction {
	t.Helper()
	output, err := omockledger.NewTransactionOutputBuilder().
		WithAddress(spliceAuditAddr).
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	tx, err := omockledger.NewTransactionBuilder().
		WithId(txId).
		WithInputs(inputs...).
		WithOutputs(output).
		Build()
	require.NoError(t, err)
	return tx
}

func mustSpliceAuditInput(
	t *testing.T,
	txId []byte,
	index uint32,
) lcommon.TransactionInput {
	t.Helper()
	input, err := omockledger.NewTransactionInputBuilder().
		WithTxId(txId).
		WithIndex(index).
		Build()
	require.NoError(t, err)
	return input
}

// TestContinuationAuditReportsUnresolvableProducer covers the diagnostic the
// issue asked for: once a local rollback arms the audit, a fetched body that
// spends an input with no producer on the local applied chain must be reported
// loudly, naming the peer that delivered it and the fork the node rolled back
// to.
func TestContinuationAuditReportsUnresolvableProducer(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	var logBuf strings.Builder
	ls.config.Logger = slog.New(slog.NewJSONHandler(&logBuf, nil))

	// Not armed: the audit must be a no-op on the steady-state path.
	missing := mustSpliceAuditInput(t, testHashBytes("absent-producer"), 0)
	body := &spliceAuditBlock{
		slot: 30,
		hash: lcommon.NewBlake2b256(testHashBytes("continuation-block")),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes("continuation-parent"),
		),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("spender-tx"),
				[]lcommon.TransactionInput{missing},
			),
		},
	}
	e := BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        body,
		Point:        ocommon.NewPoint(body.slot, body.hash.Bytes()),
	}
	ls.auditContinuationBlock(e, true)
	require.Empty(
		t,
		logBuf.String(),
		"audit must stay silent until a rollback arms it",
	)

	ls.armContinuationAudit(fixture.ancestorTip.Point, "test rollback")
	// Armed, but block validation is off (historical catch-up): the probes
	// are skipped so bulk sync does not pay for them.
	ls.auditContinuationBlock(e, false)
	require.Empty(
		t,
		logBuf.String(),
		"audit must stay silent while block validation is disabled",
	)

	ls.auditContinuationBlock(e, true)

	report := findLogRecord(
		t,
		logBuf.String(),
		"continuation block spends an input with no producer on the local applied chain",
	)
	assert.Equal(t, float64(30), report["block_slot"])
	assert.Equal(t, missing.String(), report["input"])
	assert.Equal(t, fixture.connId.String(), report["peer"])
	assert.Equal(
		t,
		float64(fixture.ancestorTip.Point.Slot),
		report["fork_rollback_slot"],
	)
	assert.Equal(t, "test rollback", report["fork_reason"])
}

// TestRollbackAheadOfLedgerDoesNotArmContinuationAudit covers genesis and
// snapshot catch-up, where the primary chain may already contain blocks beyond
// the applied ledger tip. A rollback to that primary-chain point does not move
// the ledger, so the continuation audit must remain disarmed rather than
// reporting unapplied history as missing producers.
func TestRollbackAheadOfLedgerDoesNotArmContinuationAudit(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.armContinuationAudit(fixture.ancestorTip.Point, "prior rollback")
	require.NotNil(t, ls.continuationAudit.Load())

	ls.Lock()
	ls.currentTip = fixture.ancestorTip
	ls.currentTipBlockNonce = append([]byte(nil), fixture.ancestorNonce...)
	ls.publishSnapshotsLocked()
	ls.Unlock()
	require.NoError(t, ls.db.SetTip(fixture.ancestorTip, nil))

	require.NoError(t, ls.rollbackChainAndStateDeferred(fixture.currentTip.Point, nil))

	assert.Nil(
		t,
		ls.continuationAudit.Load(),
		"a rollback ahead of the applied ledger must disarm any prior audit",
	)
	assert.Equal(t, fixture.ancestorTip, ls.currentTip)
}

// TestContinuationAuditAcceptsProducerInSameWindow guards the audit against
// false positives: blockfetch runs ahead of ledger application, so a producer
// delivered earlier in the same audit window is on the local chain even though
// no UTxO row exists for it yet.
func TestContinuationAuditAcceptsProducerInSameWindow(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	var logBuf strings.Builder
	ls.config.Logger = slog.New(slog.NewJSONHandler(&logBuf, nil))
	ls.armContinuationAudit(fixture.ancestorTip.Point, "test rollback")

	producerTxId := testHashBytes("in-window-producer")
	producerBlock := &spliceAuditBlock{
		slot: 30,
		hash: lcommon.NewBlake2b256(testHashBytes("producer-block")),
		txs: []lcommon.Transaction{
			// The mock builder requires an input; point it at this same
			// transaction so the producer block reports nothing of its own
			// and the assertion below isolates the spender's input.
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
		ConnectionId: fixture.connId,
		Block:        producerBlock,
		Point: ocommon.NewPoint(
			producerBlock.slot,
			producerBlock.hash.Bytes(),
		),
	}, true)

	spenderBlock := &spliceAuditBlock{
		slot: 40,
		hash: lcommon.NewBlake2b256(testHashBytes("spender-block")),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("in-window-spender"),
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, producerTxId, 0),
				},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        spenderBlock,
		Point: ocommon.NewPoint(
			spenderBlock.slot,
			spenderBlock.hash.Bytes(),
		),
	}, true)

	assert.NotContains(
		t,
		logBuf.String(),
		"no producer on the local applied chain",
		"a producer delivered earlier in the window must not be reported",
	)
	assert.Equal(
		t,
		2,
		ls.continuationAudit.Load().blocksSeen,
		"both bodies must have been audited",
	)
}

// armRearmFixture drives one audit window through a producer body and a
// rearm, then audits a body spending that producer's output under the new
// window, and returns what was logged. rearmPoint is the second rollback's
// point, which is what decides whether the producer survives the rearm.
func armRearmFixture(
	t *testing.T,
	rearmPoint ocommon.Point,
) (*LedgerState, string, []byte) {
	t.Helper()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	var logBuf strings.Builder
	ls.config.Logger = slog.New(slog.NewJSONHandler(&logBuf, nil))
	ls.armContinuationAudit(fixture.ancestorTip.Point, "first rollback")

	producerTxId := testHashBytes("rearm-in-flight-producer")
	producerBlock := &spliceAuditBlock{
		slot: 30,
		hash: lcommon.NewBlake2b256(testHashBytes("rearm-producer-block")),
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
		ConnectionId: fixture.connId,
		Block:        producerBlock,
		Point: ocommon.NewPoint(
			producerBlock.slot,
			producerBlock.hash.Bytes(),
		),
	}, true)
	require.Contains(
		t,
		ls.continuationAudit.Load().producedTxs,
		string(producerTxId),
		"producer must be recorded before the rearm",
	)

	ls.armContinuationAudit(rearmPoint, "second rollback")

	spenderBlock := &spliceAuditBlock{
		slot: 40,
		hash: lcommon.NewBlake2b256(testHashBytes("rearm-spender-block")),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("rearm-in-flight-spender"),
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, producerTxId, 0),
				},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        spenderBlock,
		Point: ocommon.NewPoint(
			spenderBlock.slot,
			spenderBlock.hash.Bytes(),
		),
	}, true)

	return ls, logBuf.String(), producerTxId
}

// TestContinuationAuditRearmPreservesInFlightProducer is the regression for
// issue #4102. Fork churn re-arms the audit repeatedly, and every rollback
// target in that issue's run sat ahead of the blocks the previous window had
// already vetted. Such a rollback truncates nothing those blocks occupy, so
// they stay on the primary chain -- unapplied, because ledger apply lags
// blockfetch by design, and never re-fetched, because the chain still has
// them. Forgetting them at the rearm therefore leaves the next audited spend
// of their outputs with no way to resolve, which is the false report.
func TestContinuationAuditRearmPreservesInFlightProducer(t *testing.T) {
	t.Parallel()

	// Slot 35 is above the producer at 30: the rollback this rearm follows
	// left that producer's block on the chain.
	_, logged, _ := armRearmFixture(
		t,
		ocommon.NewPoint(35, testHashBytes("rearm-above-producer")),
	)

	assert.NotContains(
		t,
		logged,
		"no producer on the local applied chain",
		"a producer the rearm's rollback did not truncate must be kept",
	)
}

// TestContinuationAuditRearmDropsTruncatedProducer is the other half of that
// contract, and what keeps the audit worth having. A rearm below an earlier
// window's producer follows a rollback that deleted the producer's block, so
// a body spending its output is spending an output that is no longer on the
// local chain -- the cross-fork splice the audit exists to report. Carrying
// such a producer forward would silence exactly that report.
func TestContinuationAuditRearmDropsTruncatedProducer(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	// The rearm point is the fixture's ancestor at slot 10, below the
	// producer block at slot 30.
	_, logged, producerTxId := armRearmFixture(t, fixture.ancestorTip.Point)

	report := findLogRecord(
		t,
		logged,
		"continuation block spends an input with no producer on the local applied chain",
	)
	assert.Equal(t, float64(40), report["block_slot"])
	assert.Equal(
		t,
		lcommon.NewBlake2b256(producerTxId).String(),
		report["producer_tx_hash"],
	)
	assert.Equal(t, "second rollback", report["fork_reason"])
}

// TestContinuationAuditRearmDoesNotRaceWithBlockfetchAudit pins the memory
// safety the carry-forward needs. Recording producers runs on the blockfetch
// dispatch goroutine under chainsyncBlockfetchMutex; the rearm reads the
// outgoing window's producers on the chainsync dispatch goroutine under
// chainsyncMutex. Neither lock covers both, so a concurrent map iteration and
// write would kill the node.
//
// It is a detector test and nothing more. The lost update the same
// interleaving can produce is a logical fault that no amount of -race proves
// absent, and is pinned deterministically by
// TestContinuationAuditRecordGoesToPublishedWindow.
func TestContinuationAuditRearmDoesNotRaceWithBlockfetchAudit(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	ls.armContinuationAudit(fixture.ancestorTip.Point, "initial rollback")

	const iterations = 300
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range iterations {
			block := &spliceAuditBlock{
				slot: uint64(30 + i),
				hash: lcommon.NewBlake2b256(
					testHashBytes(strconv.Itoa(i) + "-race-block"),
				),
				txs: []lcommon.Transaction{
					mustSpliceAuditTx(
						t,
						testHashBytes(strconv.Itoa(i)+"-race-tx"),
						[]lcommon.TransactionInput{
							mustSpliceAuditInput(
								t,
								testHashBytes("race-input"),
								0,
							),
						},
					),
				},
			}
			ls.chainsyncBlockfetchMutex.Lock()
			ls.auditContinuationBlock(BlockfetchEvent{
				ConnectionId: fixture.connId,
				Block:        block,
				Point:        ocommon.NewPoint(block.slot, block.hash.Bytes()),
			}, true)
			ls.chainsyncBlockfetchMutex.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for range iterations {
			ls.chainsyncMutex.Lock()
			ls.armContinuationAudit(fixture.ancestorTip.Point, "rearm")
			ls.chainsyncMutex.Unlock()
		}
	}()
	wg.Wait()
}

// lateProducerFixture drives the one interleaving a carry-forward rearm cannot
// see: a body that reached the primary chain before the rearm but reaches the
// audit after it. Adding a body to the chain and auditing it are two steps of
// the blockfetch drain under chainsyncBlockfetchMutex, while the rearm runs on
// the chainsync dispatch goroutine under chainsyncMutex, so the rearm can land
// between them and snapshot a producer set the body is not in yet.
//
// The producer block is put on the real primary chain, or abandoned onto a
// fork whose blob the store still holds and whose block index a replacement
// now owns. That second state is the one a hash lookup cannot tell from the
// first, which is why the audit tests primary-chain membership instead.
//
// It returns the audit log produced after the rearm.
func lateProducerFixture(
	t *testing.T,
	producerOnChain bool,
) string {
	t.Helper()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	producerTxId := testHashBytes("late-producer-tx")
	producerHash := testHashBytes("late-producer-block")
	producerBlock := &spliceAuditBlock{
		slot: 30,
		hash: lcommon.NewBlake2b256(producerHash),
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
	require.NoError(t, ls.chain.AddRawBlocks([]chain.RawBlock{{
		Slot:        producerBlock.slot,
		Hash:        producerHash,
		BlockNumber: fixture.currentTip.BlockNumber + 1,
		Type:        1,
		PrevHash:    fixture.currentTip.Point.Hash,
		Cbor:        []byte{0x80},
	}}))
	if !producerOnChain {
		// Abandon it and let a replacement take the block index it held.
		// The blob store is append-only, so the abandoned block stays
		// resolvable by hash.
		require.NoError(t, ls.chain.Rollback(fixture.currentTip.Point))
		require.NoError(t, ls.chain.AddRawBlocks([]chain.RawBlock{{
			Slot:        31,
			Hash:        testHashBytes("late-replacement-block"),
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		}}))
		retained, err := ls.chain.BlockByPoint(
			ocommon.NewPoint(producerBlock.slot, producerHash),
			nil,
		)
		require.NoError(t, err)
		indexed, err := ls.db.BlockPointByIndex(retained.ID, nil)
		require.NoError(t, err)
		require.NotEqual(
			t,
			producerBlock.slot,
			indexed.Slot,
			"the abandoned block must still be resolvable at a block ID "+
				"the replacement now owns, or this fixture is not "+
				"testing what it claims",
		)
	}

	ls.armContinuationAudit(fixture.ancestorTip.Point, "first rollback")
	// The rearm happens while the producer body is between the chain and the
	// audit, so its producers are not in the snapshot. Slot 35 is above the
	// body at 30: this rollback did not truncate it.
	ls.armContinuationAudit(
		ocommon.NewPoint(35, testHashBytes("late-rearm-point")),
		"second rollback",
	)
	require.NotContains(
		t,
		ls.continuationAudit.Load().producedTxs,
		string(producerTxId),
		"the rearm must not have seen the body the drain has not audited yet",
	)

	var logBuf strings.Builder
	ls.config.Logger = slog.New(slog.NewJSONHandler(&logBuf, nil))
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        producerBlock,
		Point: ocommon.NewPoint(
			producerBlock.slot,
			producerBlock.hash.Bytes(),
		),
	}, true)

	spenderBlock := &spliceAuditBlock{
		slot: 40,
		hash: lcommon.NewBlake2b256(testHashBytes("late-spender-block")),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(
				t,
				testHashBytes("late-spender-tx"),
				[]lcommon.TransactionInput{
					mustSpliceAuditInput(t, producerTxId, 0),
				},
			),
		},
	}
	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        spenderBlock,
		Point: ocommon.NewPoint(
			spenderBlock.slot,
			spenderBlock.hash.Bytes(),
		),
	}, true)

	// The body below the fork point must not consume block budget: it cannot
	// extend the chain, so only the spender above the point was audited.
	assert.Equal(t, 1, ls.continuationAudit.Load().blocksSeen)
	return logBuf.String()
}

// TestContinuationAuditRecordsBodyLandedBeforeRearm covers the residual
// false-positive source the carry-forward leaves. The body is on the primary
// chain at its own point and the rearm's rollback did not truncate it, so its
// outputs are on the chain exactly as a carried-forward producer's are, and a
// later spend of them is not a splice.
func TestContinuationAuditRecordsBodyLandedBeforeRearm(t *testing.T) {
	t.Parallel()

	assert.NotContains(
		t,
		lateProducerFixture(t, true),
		"no producer on the local applied chain",
		"a body on the chain below the fork point must still be a producer",
	)
}

// TestContinuationAuditIgnoresAbandonedBodyBelowForkPoint is the other half of
// that contract, and it is why membership is tested against the primary chain
// rather than by hash. The abandoned block is still in the append-only blob
// store and still answers a hash lookup, but the block index its ID holds now
// names the replacement. Accepting it would let a fork the node walked away
// from supply producers and silence a genuine report.
func TestContinuationAuditIgnoresAbandonedBodyBelowForkPoint(t *testing.T) {
	t.Parallel()

	report := findLogRecord(
		t,
		lateProducerFixture(t, false),
		"continuation block spends an input with no producer on the local applied chain",
	)
	assert.Equal(t, float64(40), report["block_slot"])
	assert.Equal(
		t,
		lcommon.NewBlake2b256(testHashBytes("late-producer-tx")).String(),
		report["producer_tx_hash"],
	)
}

// TestContinuationAuditCarryForwardRejectsAbandonedForkPoint pins the same
// membership rule on the rearm's own precondition. A prior window whose fork
// point the node has abandoned describes a chain that no longer exists, and a
// hash lookup still resolves that point out of the retained blob store.
func TestContinuationAuditCarryForwardRejectsAbandonedForkPoint(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	abandonedPoint := fixture.currentTip.Point

	ls.armContinuationAudit(abandonedPoint, "first rollback")
	prior := ls.continuationAudit.Load()
	require.NotNil(t, prior)
	producerTxId := testHashBytes("abandoned-fork-producer")
	_, ok := prior.recordProducers([][]byte{producerTxId}, 20)
	require.True(t, ok)

	// Abandon the fork point and let a replacement take its block index.
	require.NoError(t, ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, ls.chain.AddRawBlocks([]chain.RawBlock{{
		Slot:        21,
		Hash:        testHashBytes("carry-forward-fork-block"),
		BlockNumber: fixture.ancestorTip.BlockNumber + 1,
		Type:        1,
		PrevHash:    fixture.ancestorTip.Point.Hash,
		Cbor:        []byte{0x80},
	}}))
	retained, err := ls.chain.BlockByPoint(abandonedPoint, nil)
	require.NoError(t, err)
	indexed, err := ls.db.BlockPointByIndex(retained.ID, nil)
	require.NoError(t, err)
	require.NotEqual(t, abandonedPoint.Slot, indexed.Slot)

	ls.armContinuationAudit(
		ocommon.NewPoint(25, testHashBytes("carry-forward-rearm")),
		"second rollback",
	)

	assert.NotContains(
		t,
		ls.continuationAudit.Load().producedTxs,
		string(producerTxId),
		"producers of an abandoned fork must not be carried forward",
	)
}

// TestContinuationAuditRecordGoesToPublishedWindow pins the lifecycle contract
// the producer set needs. A body is audited against the window published when
// the blockfetch drain loaded it, and a rearm can publish a replacement before
// the recording happens. Recording into the window the caller loaded would put
// the producer somewhere nothing reads again, and the rearm's snapshot was
// taken before the producer existed, so the producer is lost although its
// block is on the chain.
func TestContinuationAuditRecordGoesToPublishedWindow(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.armContinuationAudit(fixture.ancestorTip.Point, "first rollback")
	stale := ls.continuationAudit.Load()
	require.NotNil(t, stale)

	// The rearm the blockfetch drain does not see. Its point is the primary
	// chain tip, so the audited body at slot 20 survives it.
	ls.armContinuationAudit(fixture.currentTip.Point, "second rollback")
	published := ls.continuationAudit.Load()
	require.NotSame(t, stale, published)

	producerTxId := testHashBytes("published-window-producer")
	require.False(
		t,
		ls.commitContinuationAuditBody(
			stale,
			BlockfetchEvent{
				ConnectionId: fixture.connId,
				Point:        fixture.currentTip.Point,
			},
			[][]byte{producerTxId},
		),
		"a caller whose window was replaced must stop auditing the body",
	)

	assert.Contains(
		t,
		published.producedTxs,
		string(producerTxId),
		"the producer must land in the window that is published now",
	)
	assert.NotContains(t, stale.producedTxs, string(producerTxId))
}

// TestContinuationAuditRecordRejectsOffChainRacedBody is the guard on that
// redirection. The window published by the rearm may have been armed by a
// rollback that deleted the audited block, so a body reaching a window it was
// not audited against has to prove it is on the primary chain first.
func TestContinuationAuditRecordRejectsOffChainRacedBody(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.armContinuationAudit(fixture.ancestorTip.Point, "first rollback")
	stale := ls.continuationAudit.Load()
	ls.armContinuationAudit(fixture.currentTip.Point, "second rollback")
	published := ls.continuationAudit.Load()
	require.NotSame(t, stale, published)

	producerTxId := testHashBytes("raced-off-chain-producer")
	require.False(
		t,
		ls.commitContinuationAuditBody(
			stale,
			BlockfetchEvent{
				ConnectionId: fixture.connId,
				Point: ocommon.NewPoint(
					30,
					testHashBytes("never-on-the-chain"),
				),
			},
			[][]byte{producerTxId},
		),
		"a caller whose window was replaced must stop auditing the body",
	)

	assert.NotContains(
		t,
		published.producedTxs,
		string(producerTxId),
		"a body the chain does not hold must not become a producer",
	)
}

// TestSettleAuditAfterRewind pins what the audit window is once a recovery
// rewind returns. The rewind clears the window and runs without the lifecycle
// lock held, so the decision has to be made from the pointer as it stands
// afterwards rather than from the one that was cleared.
func TestSettleAuditAfterRewind(t *testing.T) {
	t.Parallel()

	target := ocommon.NewPoint(20, testHashBytes("rewind-target"))

	t.Run("restores the cleared window when nothing was truncated", func(t *testing.T) {
		t.Parallel()
		fixture := newChainsyncRollbackFixture(t)
		ls := fixture.ls
		ls.armContinuationAudit(fixture.ancestorTip.Point, "rollback")
		prior, gen := ls.takeContinuationAuditForRewind()

		ls.settleAuditAfterRewind(prior, gen, false, target)

		assert.Same(t, prior, ls.continuationAudit.Load())
	})

	t.Run("keeps a window armed while the rewind ran", func(t *testing.T) {
		t.Parallel()
		fixture := newChainsyncRollbackFixture(t)
		ls := fixture.ls
		ls.armContinuationAudit(fixture.ancestorTip.Point, "rollback")
		prior, gen := ls.takeContinuationAuditForRewind()
		// The arm the rewind could not see, at a point the rewind kept.
		ls.armContinuationAudit(fixture.ancestorTip.Point, "concurrent")
		armed := ls.continuationAudit.Load()
		require.NotSame(t, prior, armed)

		ls.settleAuditAfterRewind(prior, gen, false, target)

		assert.Same(
			t,
			armed,
			ls.continuationAudit.Load(),
			"the newer window describes the chain the rewind left",
		)
	})

	t.Run("drops a window the truncation invalidated", func(t *testing.T) {
		t.Parallel()
		fixture := newChainsyncRollbackFixture(t)
		ls := fixture.ls
		ls.armContinuationAudit(fixture.ancestorTip.Point, "rollback")
		prior, gen := ls.takeContinuationAuditForRewind()
		// A window armed during the rewind, above the rewind target: the
		// truncation deleted the block its fork point names.
		ls.armContinuationAudit(
			ocommon.NewPoint(40, testHashBytes("above-the-target")),
			"concurrent",
		)
		require.NotNil(t, ls.continuationAudit.Load())

		ls.settleAuditAfterRewind(prior, gen, true, target)

		assert.Nil(t, ls.continuationAudit.Load())
	})

	t.Run("does not restore after a committed truncation", func(t *testing.T) {
		t.Parallel()
		fixture := newChainsyncRollbackFixture(t)
		ls := fixture.ls
		ls.armContinuationAudit(fixture.ancestorTip.Point, "rollback")
		prior, gen := ls.takeContinuationAuditForRewind()

		ls.settleAuditAfterRewind(prior, gen, true, target)

		assert.Nil(t, ls.continuationAudit.Load())
	})

	// A nil pointer does not mean "still cleared by this rewind". Both
	// disarm sites follow a committed chain truncation -- a chainsync
	// rollback whose ledger tip did not reach its point, and a truncation
	// whose ledger rollback failed -- and either can land after the
	// post-rewind tip read and before the settle takes the lock. The tip
	// comparison was made before that truncation, so it reports nothing
	// truncated, and restoring puts back a window whose fork point the
	// rollback may have just deleted, over a disarm that was deliberate.
	for _, tc := range []struct {
		name  string
		clear func(ls *LedgerState)
	}{
		{
			name: "a disarm landed in the gap",
			clear: func(ls *LedgerState) {
				ls.disarmContinuationAudit()
			},
		},
		{
			name: "an arm and a disarm landed in the gap",
			clear: func(ls *LedgerState) {
				ls.armContinuationAudit(
					ocommon.NewPoint(40, testHashBytes("gap-arm")),
					"concurrent",
				)
				ls.disarmContinuationAudit()
			},
		},
	} {
		t.Run("does not restore over "+tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			ls.armContinuationAudit(fixture.ancestorTip.Point, "rollback")
			prior, gen := ls.takeContinuationAuditForRewind()
			require.NotNil(t, prior)

			tc.clear(ls)

			ls.settleAuditAfterRewind(prior, gen, false, target)

			assert.Nil(
				t,
				ls.continuationAudit.Load(),
				"a rewind does not get to undo another owner's decision",
			)
		})
	}
}

// TestPrimaryChainTipRegressed pins which tip movements a recovery rewind may
// read as a truncation. Nothing serialises the rewind against blockfetch, so
// the primary chain also grows underneath it, and an append deletes nothing:
// reading one as a truncation discards a window the rewind left entirely valid
// and costs the audit its coverage until the next rollback arms a new one.
func TestPrimaryChainTipRegressed(t *testing.T) {
	t.Parallel()

	before := ocommon.NewPoint(100, testHashBytes("tip-before"))

	for _, tc := range []struct {
		name  string
		after ocommon.Point
		want  bool
	}{
		{
			name:  "an append moved the tip forward",
			after: ocommon.NewPoint(140, testHashBytes("tip-appended")),
			want:  false,
		},
		{
			name:  "the tip did not move",
			after: ocommon.NewPoint(100, testHashBytes("tip-before")),
			want:  false,
		},
		{
			name:  "a truncation moved the tip back",
			after: ocommon.NewPoint(60, testHashBytes("tip-truncated")),
			want:  true,
		},
		{
			name:  "another block took the same slot",
			after: ocommon.NewPoint(100, testHashBytes("tip-forked")),
			want:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				tc.want,
				primaryChainTipRegressed(before, tc.after),
			)
		})
	}
}

// TestContinuationAuditCarriesEndorserRefsItDidNotTruncate applies the
// carry-forward rule to the other thing a window knows about a block. An
// endorser-block reference names the ranking block that carries it, so a
// reference at or below the rearm point belongs to a block the rollback left
// on the chain -- and nothing re-queues it, because that block is not
// re-fetched. Dropping it leaves the new window unable to resolve an
// endorser-resident producer, which is the same false report in the shape
// endorser transactions take.
func TestContinuationAuditCarriesEndorserRefsItDidNotTruncate(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.armContinuationAudit(fixture.ancestorTip.Point, "first rollback")
	prior := ls.continuationAudit.Load()
	require.NotNil(t, prior)
	kept := continuationAuditEndorserRef{
		certParentHash:  testHashBytes("kept-cert-parent"),
		blockSlot:       30,
		lastProbedBlock: 7,
	}
	truncated := continuationAuditEndorserRef{
		certParentHash: testHashBytes("truncated-cert-parent"),
		blockSlot:      40,
	}
	prior.queueEndorserRef(kept)
	prior.queueEndorserRef(truncated)

	ls.armContinuationAudit(
		ocommon.NewPoint(35, testHashBytes("endorser-carry-rearm")),
		"second rollback",
	)

	next := ls.continuationAudit.Load()
	require.Len(t, next.pendingEndorserRefs, 1)
	assert.Equal(t, uint64(30), next.pendingEndorserRefs[0].blockSlot)
	assert.Equal(
		t,
		0,
		next.pendingEndorserRefs[0].lastProbedBlock,
		"probe accounting belongs to the window that did the probing",
	)
	assert.Contains(t, next.pendingEndorserSeen, kept.key())
	assert.NotContains(t, next.pendingEndorserSeen, truncated.key())
	assert.Empty(
		t,
		next.resolvedEndorserBlocks,
		"the merged memo is not carried: re-merging is idempotent, a stale "+
			"memo would suppress a merge the new window needs",
	)
}

// TestContinuationAuditProducerKeepsLowestSlot pins which slot a repeated
// producer is recorded at. The slot answers "which rollback takes this
// producer off the chain", so a transaction delivered by blocks at two slots
// leaves the chain only when the lower one is truncated. Repeats are ordinary
// once endorser-block transactions are producers: the same transaction can
// appear in more than one endorser block, and the same closure can be
// certified from more than one ranking block in a window. Keeping the later
// slot would drop a producer a rearm did not truncate, which is the false
// report this window records slots to prevent.
func TestContinuationAuditProducerKeepsLowestSlot(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.armContinuationAudit(fixture.ancestorTip.Point, "first rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)

	producerTxId := testHashBytes("repeated-producer-tx")
	_, ok := window.recordProducers([][]byte{producerTxId}, 30)
	require.True(t, ok)
	_, ok = window.recordProducers([][]byte{producerTxId}, 50)
	require.True(t, ok)

	assert.Equal(t, uint64(30), window.producedTxs[string(producerTxId)])
	assert.Contains(
		t,
		window.producersAtOrBelow(40),
		string(producerTxId),
		"a rearm above the earliest delivering block must keep the producer",
	)
}

// TestContinuationAuditBudgetIsBounded verifies the audit stops on its own so a
// long-lived node never pays for it outside a fork-churn window.
func TestContinuationAuditBudgetIsBounded(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.config.Logger = slog.New(slog.NewJSONHandler(&strings.Builder{}, nil))
	ls.armContinuationAudit(fixture.ancestorTip.Point, "test rollback")

	empty := &spliceAuditBlock{
		slot: 30,
		hash: lcommon.NewBlake2b256(testHashBytes("empty-block")),
	}
	e := BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        empty,
		Point:        ocommon.NewPoint(empty.slot, empty.hash.Bytes()),
	}
	for range continuationAuditBlockBudget + 5 {
		ls.auditContinuationBlock(e, true)
	}
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	assert.Equal(t, 0, window.remaining)
	assert.Equal(t, continuationAuditBlockBudget, window.blocksSeen)
}

// TestContinuationAuditIgnoresAbandonedFetchedBodies verifies that a body
// delivered after a fork restart is not allowed to seed the producer window.
// The body fails chain insertion, so the audit must not inspect it.
func TestContinuationAuditIgnoresAbandonedFetchedBodies(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	var logBuf strings.Builder
	ls.config.Logger = slog.New(slog.NewJSONHandler(&logBuf, nil))
	ls.armContinuationAudit(fixture.ancestorTip.Point, "test rollback")

	missing := mustSpliceAuditInput(t, testHashBytes("stale-producer"), 0)
	stale := &spliceAuditBlock{
		slot: 30,
		hash: lcommon.NewBlake2b256(testHashBytes("stale-body")),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes("abandoned-parent"),
		),
		txs: []lcommon.Transaction{
			mustSpliceAuditTx(t, testHashBytes("stale-spender"),
				[]lcommon.TransactionInput{missing}),
		},
	}
	ls.pendingBlockfetchEvents = []BlockfetchEvent{{
		ConnectionId: fixture.connId,
		Block:        stale,
		Point:        ocommon.NewPoint(stale.slot, stale.hash.Bytes()),
	}}

	require.NoError(t, ls.flushPendingBlockfetchBlocksDeferred(nil))
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)
	assert.Equal(t, 0, window.blocksSeen)
	assert.NotContains(
		t,
		logBuf.String(),
		"no producer on the local applied chain",
	)
}

// findLogRecord returns the first JSON log record whose message matches msg.
func findLogRecord(
	t *testing.T,
	logs string,
	msg string,
) map[string]any {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if record["msg"] == msg {
			return record
		}
	}
	t.Fatalf("no log record with message %q in:\n%s", msg, logs)
	return nil
}

// rearmMidBodyBlock re-arms the audit the first time the blockfetch handler
// reads its transactions.
//
// auditContinuationBlock reads the published window, then the body's
// transactions, then commits the body against the window published at that
// moment. A rollback handled on the chainsync dispatch goroutine can publish a
// replacement in between, and this hook reproduces that state exactly -- the
// handler is left auditing against a window that is no longer the one the
// body's producers went into -- without depending on an interleaving.
type rearmMidBodyBlock struct {
	*spliceAuditBlock
	rearm func()
	once  sync.Once
}

func (b *rearmMidBodyBlock) Transactions() []lcommon.Transaction {
	b.once.Do(b.rearm)
	return b.spliceAuditBlock.Transactions()
}

// TestContinuationAuditStopsWhenItsWindowIsReplacedMidBody pins what the
// blockfetch handler does when its window stops being the published one
// part-way through a body.
//
// The body's producers are redirected into the published window, which is what
// keeps them from being lost. The rest of the audit is not: it still resolves
// inputs against the window it loaded, which those producers are no longer in.
// A body whose later transaction spends an earlier one's output therefore has
// no producer anywhere the audit looks, and the node reports a cross-fork
// splice against its own block -- the false report from the opposite side of
// the same race the redirection closes.
func TestContinuationAuditStopsWhenItsWindowIsReplacedMidBody(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	var logBuf strings.Builder
	ls.config.Logger = slog.New(slog.NewJSONHandler(&logBuf, nil))
	ls.armContinuationAudit(fixture.ancestorTip.Point, "first rollback")
	stale := ls.continuationAudit.Load()
	require.NotNil(t, stale)

	// The body is on the primary chain at its own point, so the rearm's
	// redirection accepts it as a producer.
	const bodySlot = 30
	bodyHash := testHashBytes("mid-body-rearm-block")
	require.NoError(t, ls.chain.AddRawBlocks([]chain.RawBlock{{
		Slot:        bodySlot,
		Hash:        bodyHash,
		BlockNumber: fixture.currentTip.BlockNumber + 1,
		Type:        1,
		PrevHash:    fixture.currentTip.Point.Hash,
		Cbor:        []byte{0x80},
	}}))

	// Two transactions, the second spending the first's output: the only
	// producer this body needs is itself.
	producerTxId := testHashBytes("mid-body-rearm-producer")
	body := &rearmMidBodyBlock{
		spliceAuditBlock: &spliceAuditBlock{
			slot: bodySlot,
			hash: lcommon.NewBlake2b256(bodyHash),
			txs: []lcommon.Transaction{
				mustSpliceAuditTx(
					t,
					producerTxId,
					[]lcommon.TransactionInput{
						mustSpliceAuditInput(t, producerTxId, 9),
					},
				),
				mustSpliceAuditTx(
					t,
					testHashBytes("mid-body-rearm-spender"),
					[]lcommon.TransactionInput{
						mustSpliceAuditInput(t, producerTxId, 0),
					},
				),
			},
		},
	}
	// Slot 35 is above the body: the rollback this rearm follows left it on
	// the chain, which is why its producers are still legitimate.
	body.rearm = func() {
		ls.armContinuationAudit(
			ocommon.NewPoint(35, testHashBytes("mid-body-rearm-point")),
			"second rollback",
		)
	}

	ls.auditContinuationBlock(BlockfetchEvent{
		ConnectionId: fixture.connId,
		Block:        body,
		Point:        ocommon.NewPoint(bodySlot, bodyHash),
	}, true)

	published := ls.continuationAudit.Load()
	require.NotSame(t, stale, published, "the rearm must have published")
	assert.Contains(
		t,
		published.producedTxs,
		string(producerTxId),
		"the body's producers belong to the window published now",
	)
	assert.Empty(
		t,
		stale.producedTxs,
		"nothing may be recorded into a window nothing reads again",
	)
	assert.NotContains(
		t,
		logBuf.String(),
		"no producer on the local applied chain",
		"a body must not be reported against a window its own producers "+
			"are not in",
	)
}

// certifyingAuditHeader is the smallest header the audit's cert-driven
// endorser-block classification accepts. Only LeiosCertified is reached, so
// the embedded interface is never called.
type certifyingAuditHeader struct {
	lcommon.BlockHeader
}

func (certifyingAuditHeader) LeiosCertified() (bool, bool) { return true, true }

// certifyingAuditBlock is a spliceAuditBlock that certifies an endorser block,
// which is what makes the audit queue an endorser-block reference for it.
type certifyingAuditBlock struct {
	*spliceAuditBlock
}

func (b *certifyingAuditBlock) Header() lcommon.BlockHeader {
	return certifyingAuditHeader{}
}

// TestContinuationAuditRearmDoesNotRaceWithEndorserRefQueue is the memory
// safety the other half of the carry-forward needs.
//
// A rearm carries the outgoing window's pending endorser-block references
// forward as well as its producers, and it reads them on the chainsync
// dispatch goroutine under chainsyncMutex while the blockfetch goroutine is
// still queueing references under chainsyncBlockfetchMutex and taking them off
// the queue to probe them. Neither lock covers both accesses.
//
// It is a detector test: the reference each body queues is deliberately left
// unresolvable, so every audited body also drains, which is when the queue is
// emptied and rebuilt.
func TestContinuationAuditRearmDoesNotRaceWithEndorserRefQueue(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	// A provider that never has the block keeps each reference queued, and
	// it is never reached here anyway: the parent lookup fails first.
	ls.config.EndorserBlockProvider = func(
		[]byte,
		uint64,
	) ([]cbor.RawMessage, bool) {
		return nil, false
	}
	ls.armContinuationAudit(fixture.ancestorTip.Point, "initial rollback")

	// Built up front: the mock builders assert through t, which a
	// non-test goroutine may not do.
	const iterations = 300
	events := make([]BlockfetchEvent, 0, iterations)
	for i := range iterations {
		seed := strconv.Itoa(i)
		body := &certifyingAuditBlock{
			spliceAuditBlock: &spliceAuditBlock{
				slot: uint64(30 + i),
				hash: lcommon.NewBlake2b256(
					testHashBytes(seed + "-eb-race-block"),
				),
				// A parent the block store does not hold, so the
				// reference stays queued for the next body. Distinct
				// per block, so the queue keeps growing rather than
				// deduplicating to one entry.
				prevHash: lcommon.NewBlake2b256(
					testHashBytes(seed + "-eb-race-parent"),
				),
				txs: []lcommon.Transaction{
					mustSpliceAuditTx(
						t,
						testHashBytes(seed+"-eb-race-tx"),
						[]lcommon.TransactionInput{
							mustSpliceAuditInput(
								t,
								testHashBytes("eb-race-input"),
								0,
							),
						},
					),
				},
			},
		}
		events = append(events, BlockfetchEvent{
			ConnectionId: fixture.connId,
			Block:        body,
			Point: ocommon.NewPoint(
				body.slot,
				body.hash.Bytes(),
			),
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, e := range events {
			ls.chainsyncBlockfetchMutex.Lock()
			ls.auditContinuationBlock(e, true)
			ls.chainsyncBlockfetchMutex.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for range iterations {
			ls.chainsyncMutex.Lock()
			ls.armContinuationAudit(fixture.ancestorTip.Point, "rearm")
			ls.chainsyncMutex.Unlock()
		}
	}()
	wg.Wait()
}

// TestContinuationAuditRearmDoesNotRaceWithEndorserRefDrain is the memory
// safety the other half of the carry-forward needs.
//
// A rearm carries the outgoing window's pending endorser-block references
// forward as well as its producers. Queueing a reference happens under
// continuationAuditMutex, with the body's producers, so it is ordered against
// the rearm -- but a drain is not: it empties the queue, probes what it took
// and rebuilds it from the blockfetch goroutine with no lock the chainsync
// dispatch goroutine takes. A concurrent slice write and read is what that
// leaves.
//
// The drain is driven directly. Reaching it through auditContinuationBlock
// needs the audited body to keep ownership of its window for the length of an
// input probe, which is exactly what a rearming goroutine takes away, so the
// entry point cannot be made to exercise this pair at all reliably. Both calls
// below hold what production holds at the same point: the drain runs with no
// lock held, and the rearm holds chainsyncMutex.
func TestContinuationAuditRearmDoesNotRaceWithEndorserRefDrain(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	// Never reached: the parent lookup fails first, which is what keeps
	// every reference queued for the next drain to take again.
	ls.config.EndorserBlockProvider = func(
		[]byte,
		uint64,
	) ([]cbor.RawMessage, bool) {
		return nil, false
	}
	ls.armContinuationAudit(fixture.ancestorTip.Point, "initial rollback")
	window := ls.continuationAudit.Load()
	require.NotNil(t, window)

	const iterations = 200
	for i := range iterations {
		window.queueEndorserRef(continuationAuditEndorserRef{
			certParentHash: testHashBytes(
				strconv.Itoa(i) + "-eb-drain-parent",
			),
			blockSlot: uint64(30 + i),
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range iterations {
			// One drain per audited body, as auditContinuationBlock
			// counts them: the once-per-body probe guard reads this.
			window.blocksSeen = i + 1
			budget := continuationAuditMaxEndorserBlocksPerBlock
			ls.drainContinuationAuditEndorserRefs(
				window,
				&budget,
				uint64(40+i),
			)
		}
	}()
	go func() {
		defer wg.Done()
		for range iterations {
			ls.chainsyncMutex.Lock()
			// The window a drain is working is the published one when a
			// rollback re-arms underneath it. Published through the
			// production writer so the pointer generation stays honest.
			ls.continuationAuditMutex.Lock()
			ls.publishContinuationAudit(window)
			ls.continuationAuditMutex.Unlock()
			ls.armContinuationAudit(fixture.ancestorTip.Point, "rearm")
			ls.chainsyncMutex.Unlock()
		}
	}()
	wg.Wait()

	assert.NotEmpty(
		t,
		window.pendingEndorserRefs,
		"the drains must have kept requeueing, or this exercises nothing",
	)
}

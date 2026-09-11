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
	"github.com/blinklabs-io/dingo/database/models"
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

// TestContinuationAuditRearmDoesNotRaceWithBlockfetchAudit pins the
// synchronization the carry-forward needs. Recording producers runs on the
// blockfetch dispatch goroutine under chainsyncBlockfetchMutex; the rearm
// reads the outgoing window's producers on the chainsync dispatch goroutine
// under chainsyncMutex. Neither lock covers both, so the producer set needs
// its own, or a concurrent map iteration and write kills the node.
func TestContinuationAuditRearmDoesNotRaceWithBlockfetchAudit(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.config.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	// Resolve the rearm's fork-point lookup in memory. Going to the database
	// for it would put a shared lock between the two goroutines on every
	// iteration, and that incidental ordering is enough to hide the
	// unsynchronized map access from the race detector.
	ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		return models.Block{
			Slot: fixture.ancestorTip.Point.Slot,
			Hash: append([]byte(nil), hash...),
		}, nil
	}
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

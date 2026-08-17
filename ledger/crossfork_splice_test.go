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
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
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

// TestContinuationAuditAcceptsProducerInSameWindow guards the audit against
// false positives: blockfetch runs ahead of ledger application, so a producer
// delivered earlier in the same audit window is on the local chain even though
// no UTxO row exists for it yet.
func TestContinuationAuditAcceptsProducerInSameWindow(t *testing.T) {
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

// TestContinuationAuditBudgetIsBounded verifies the audit stops on its own so a
// long-lived node never pays for it outside a fork-churn window.
func TestContinuationAuditBudgetIsBounded(t *testing.T) {
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

	require.NoError(t, ls.flushPendingBlockfetchBlocks())
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

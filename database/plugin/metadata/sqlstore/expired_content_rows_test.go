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

package sqlstore

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

const expiryTestAddress = "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"

// expiringChain is a canonical chain whose blocks the tests apply to a store
// and expire the way history expiry does: the store prepares its content rows
// for the block's expiry, then the body goes.
type expiringChain struct {
	t      *testing.T
	store  *Store
	bodies *bodyStore
	mu     sync.Mutex
	blocks map[uint64][]byte
	gone   map[uint64]bool
}

func newExpiringChain(t *testing.T) *expiringChain {
	return newExpiringChainOver(t, newAPIModeSQLiteStore(t, nil))
}

func newExpiringChainOver(t *testing.T, store *Store) *expiringChain {
	store.storageMode = types.StorageModeAPI
	bodies := &bodyStore{bodies: map[[32]byte][]byte{}}
	store.SetPayloadResolver(bodies)
	return &expiringChain{
		t:      t,
		store:  store,
		bodies: bodies,
		blocks: map[uint64][]byte{},
		gone:   map[uint64]bool{},
	}
}

func (c *expiringChain) hash(slot uint64) []byte {
	return bytes.Repeat([]byte{byte(slot / 100)}, 32)
}

// LocatorAtSlot serves the canonical block at slot, as the block stores do.
func (c *expiringChain) LocatorAtSlot(
	_ context.Context,
	slot uint64,
) (types.PayloadLocator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	body, ok := c.blocks[slot]
	if !ok {
		return nil, fmt.Errorf("slot %d: %w", slot, models.ErrBlockNotFound)
	}
	if c.gone[slot] {
		return nil, &types.HistoryExpiredError{Slot: slot, Hash: c.hash(slot)}
	}
	return types.NewBlockPayloadLocator(slot, c.hash(slot), body)
}

func (c *expiringChain) apply(
	slot uint64,
	tx lcommon.Transaction,
	parts ...[]byte,
) {
	c.t.Helper()
	body := bytes.Join(
		append([][]byte{fmt.Appendf(nil, "block %d", slot)}, parts...),
		[]byte{'|'},
	)
	locator, err := types.NewBlockPayloadLocator(slot, c.hash(slot), body)
	require.NoError(c.t, err)
	c.mu.Lock()
	c.blocks[slot] = body
	c.mu.Unlock()
	c.bodies.put(c.hash(slot), body)
	txn := c.store.Transaction(c.t.Context())
	txn.(*sqlTxn).SetPayloadLocator(locator)
	require.NoError(c.t, c.store.SetTransaction(
		tx, ocommon.Point{Slot: slot, Hash: c.hash(slot)},
		0, nil, true, txn, 0,
	))
	require.NoError(c.t, txn.Commit())
}

func (c *expiringChain) expire(slot uint64) {
	c.t.Helper()
	require.NoError(c.t, c.store.PrepareBlockExpiry(
		c.t.Context(), c, slot, c.hash(slot),
	))
	c.mu.Lock()
	c.gone[slot] = true
	c.mu.Unlock()
	c.bodies.release(c.hash(slot))
}

func expiryTestTx(
	t *testing.T,
	id byte,
	witnesses *mockledger.MockTransactionWitnessSet,
	outputs ...lcommon.TransactionOutput,
) lcommon.Transaction {
	t.Helper()
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{id}, 32))
	tx.WithValid(true)
	if witnesses != nil {
		tx.WithWitnesses(witnesses)
	}
	if len(outputs) > 0 {
		tx.WithOutputs(outputs...)
	}
	return tx
}

func witnessDatum(raw []byte) *mockledger.MockTransactionWitnessSet {
	var datum lcommon.Datum
	datum.SetCbor(raw)
	return mockledger.NewMockTransactionWitnessSet().WithPlutusData(datum)
}

func datumHashOutput(t *testing.T, raw []byte) lcommon.TransactionOutput {
	t.Helper()
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(expiryTestAddress).
		WithLovelace(2_000_000).
		WithDatumHash(lcommon.Blake2b256Hash(raw).Bytes()).
		Build()
	require.NoError(t, err)
	return out
}

// refScriptOutput is an output carrying a reference script inside its own
// CBOR, which is how a reference script's bytes are located.
type refScriptOutput struct {
	lcommon.TransactionOutput
	script lcommon.Script
	cbor   []byte
}

func (o refScriptOutput) ScriptRef() lcommon.Script { return o.script }

func (o refScriptOutput) Cbor() []byte { return o.cbor }

func newRefScriptOutput(
	t *testing.T,
	script lcommon.Script,
	tag string,
) refScriptOutput {
	t.Helper()
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(expiryTestAddress).
		WithLovelace(2_000_000).
		Build()
	require.NoError(t, err)
	return refScriptOutput{
		TransactionOutput: out,
		script:            script,
		cbor: bytes.Join(
			[][]byte{[]byte(tag), script.RawScriptBytes()}, []byte{'#'},
		),
	}
}

// A datum whose first block expired while no live output named it is
// re-pointed by a later block that carries it, so a live output created
// afterwards that names it by hash resolves.
func TestExpiredDatumRowIsRepointedByALaterCarrier(t *testing.T) {
	t.Parallel()
	testExpiredDatumRowIsRepointedByALaterCarrier(t, newExpiringChain(t))
}

func testExpiredDatumRowIsRepointedByALaterCarrier(t *testing.T, c *expiringChain) {
	t.Helper()
	raw := []byte{0x82, 0x01, 0x03}
	c.apply(300, expiryTestTx(t, 0x31, witnessDatum(raw)), raw)
	c.expire(300)

	c.apply(400,
		expiryTestTx(t, 0x41, witnessDatum(raw), datumHashOutput(t, raw)),
		raw,
	)

	got, err := c.store.GetDatum(lcommon.Blake2b256Hash(raw), nil)
	require.NoError(t, err)
	require.Equal(t, raw, got.RawDatum)
	require.EqualValues(t, 400, got.AddedSlot)
	held, err := c.store.BlockHoldsActivePayloads(
		t.Context(), 400, c.hash(400), 0,
	)
	require.NoError(t, err)
	require.True(t, held, "the live output keeps the new carrier")
}

// A script whose first block expired is re-pointed by a later block whose
// output carries it as a reference script.
func TestExpiredScriptRowIsRepointedByALaterReferenceScript(t *testing.T) {
	t.Parallel()
	testExpiredScriptRowIsRepointedByALaterReferenceScript(t, newExpiringChain(t))
}

func testExpiredScriptRowIsRepointedByALaterReferenceScript(t *testing.T, c *expiringChain) {
	t.Helper()
	script := lcommon.PlutusV2Script{0x41, 0x07}
	witnesses := mockledger.NewMockTransactionWitnessSet()
	witnesses.WithPlutusV2Scripts(script)
	c.apply(300, expiryTestTx(t, 0x32, witnesses), script.RawScriptBytes())
	c.expire(300)

	out := newRefScriptOutput(t, script, "output at 400")
	c.apply(400, expiryTestTx(t, 0x42, nil, out), out.cbor)

	got, err := c.store.GetScript(script.Hash(), nil)
	require.NoError(t, err)
	require.Equal(t, script.RawScriptBytes(), got.Content)
	require.EqualValues(t, 400, got.CreatedSlot)
	held, err := c.store.BlockHoldsActivePayloads(
		t.Context(), 400, c.hash(400), 0,
	)
	require.NoError(t, err)
	require.True(t, held, "the live reference script keeps the new carrier")
}

// A datum carried again by a later block before its first block expires
// moves to that block when the first one expires, so an output created after
// the expiry that only names the datum by hash still resolves.
func TestDatumRowMovesToARetainedCarrierWhenItsBlockExpires(t *testing.T) {
	t.Parallel()
	testDatumRowMovesToARetainedCarrierWhenItsBlockExpires(t, newExpiringChain(t))
}

func testDatumRowMovesToARetainedCarrierWhenItsBlockExpires(t *testing.T, c *expiringChain) {
	t.Helper()
	raw := []byte{0x82, 0x01, 0x04}
	c.apply(300, expiryTestTx(t, 0x33, witnessDatum(raw)), raw)
	c.apply(400, expiryTestTx(t, 0x43, witnessDatum(raw)), raw)
	c.expire(300)
	c.apply(500, expiryTestTx(t, 0x53, nil, datumHashOutput(t, raw)))

	got, err := c.store.GetDatum(lcommon.Blake2b256Hash(raw), nil)
	require.NoError(t, err)
	require.Equal(t, raw, got.RawDatum)
	require.EqualValues(t, 400, got.AddedSlot)
}

// The same for a script: its row moves to a retained witness carrier.
func TestScriptRowMovesToARetainedCarrierWhenItsBlockExpires(t *testing.T) {
	t.Parallel()
	testScriptRowMovesToARetainedCarrierWhenItsBlockExpires(t, newExpiringChain(t))
}

func testScriptRowMovesToARetainedCarrierWhenItsBlockExpires(t *testing.T, c *expiringChain) {
	t.Helper()
	script := lcommon.PlutusV2Script{0x41, 0x08}
	witnesses := mockledger.NewMockTransactionWitnessSet()
	witnesses.WithPlutusV2Scripts(script)
	c.apply(300, expiryTestTx(t, 0x34, witnesses), script.RawScriptBytes())
	c.apply(400, expiryTestTx(t, 0x44, witnesses), script.RawScriptBytes())
	c.expire(300)

	got, err := c.store.GetScript(script.Hash(), nil)
	require.NoError(t, err)
	require.Equal(t, script.RawScriptBytes(), got.Content)
	require.EqualValues(t, 400, got.CreatedSlot)
}

// A later carrier leaves a row whose block is still readable where it is.
func TestReadableContentRowKeepsItsFirstCarrier(t *testing.T) {
	t.Parallel()
	testReadableContentRowKeepsItsFirstCarrier(t, newExpiringChain(t))
}

func testReadableContentRowKeepsItsFirstCarrier(t *testing.T, c *expiringChain) {
	t.Helper()
	raw := []byte{0x82, 0x01, 0x05}
	script := lcommon.PlutusV2Script{0x41, 0x09}
	witnesses := witnessDatum(raw)
	witnesses.WithPlutusV2Scripts(script)
	c.apply(300, expiryTestTx(t, 0x35, witnesses), raw, script.RawScriptBytes())
	c.apply(400, expiryTestTx(t, 0x45, witnesses), raw, script.RawScriptBytes())

	got, err := c.store.GetDatum(lcommon.Blake2b256Hash(raw), nil)
	require.NoError(t, err)
	require.EqualValues(t, 300, got.AddedSlot)
	gotScript, err := c.store.GetScript(script.Hash(), nil)
	require.NoError(t, err)
	require.EqualValues(t, 300, gotScript.CreatedSlot)
}

// A rollback past the block a row was re-pointed to keeps the row when a
// surviving transaction carries it, naming that carrier's expired block, and
// does not leave it naming the abandoned block.
func TestRollbackPastARepointedRowKeepsItsSurvivingCarrier(t *testing.T) {
	t.Parallel()
	testRollbackPastARepointedRowKeepsItsSurvivingCarrier(t, newExpiringChain(t))
}

func testRollbackPastARepointedRowKeepsItsSurvivingCarrier(t *testing.T, c *expiringChain) {
	t.Helper()
	raw := []byte{0x82, 0x01, 0x06}
	script := lcommon.PlutusV2Script{0x41, 0x0a}
	witnesses := witnessDatum(raw)
	witnesses.WithPlutusV2Scripts(script)
	c.apply(300, expiryTestTx(t, 0x36, witnesses), raw, script.RawScriptBytes())
	c.expire(300)
	c.apply(400, expiryTestTx(t, 0x46, witnesses), raw, script.RawScriptBytes())

	require.NoError(t, c.store.DeleteTransactionsAfterSlot(350, nil))
	c.bodies.release(c.hash(400))

	_, err := c.store.GetDatum(lcommon.Blake2b256Hash(raw), nil)
	var expired *types.HistoryExpiredError
	require.ErrorAs(t, err, &expired, "the datum is known and expired")
	require.EqualValues(t, 300, expired.Slot)
	require.Equal(t, c.hash(300), expired.Hash)
	_, err = c.store.GetScript(script.Hash(), nil)
	require.ErrorAs(t, err, &expired, "the script is known and expired")
	require.EqualValues(t, 300, expired.Slot)
}

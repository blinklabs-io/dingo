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

package mempool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func indexTestAddress(t *testing.T, seed byte) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkMainnet,
		bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	return addr
}

type indexTxInput struct {
	hash  [32]byte
	index uint32
}

type indexTxSpec struct {
	inputs  []indexTxInput
	outputs []lcommon.Address
	// collateral and collateralReturn are set for a transaction marked as
	// failing phase 2.
	collateral       *indexTxInput
	collateralReturn *lcommon.Address
	invalid          bool
}

func indexTxOutput(addr lcommon.Address, amount uint64) babbage.BabbageTransactionOutput {
	return babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: amount},
	}
}

func shelleyInput(
	t *testing.T,
	in indexTxInput,
) shelley.ShelleyTransactionInput {
	t.Helper()
	input, err := shelley.NewShelleyTransactionInput(
		fmt.Sprintf("%x", in.hash[:]),
		int(in.index),
	)
	require.NoError(t, err)
	return input
}

func buildIndexTx(t *testing.T, spec indexTxSpec) ([]byte, gledger.Transaction) {
	t.Helper()
	var inputs []shelley.ShelleyTransactionInput
	for _, in := range spec.inputs {
		inputs = append(inputs, shelleyInput(t, in))
	}
	var outputs []babbage.BabbageTransactionOutput
	for i, addr := range spec.outputs {
		outputs = append(outputs, indexTxOutput(addr, 2_000_000+uint64(i)))
	}
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs:  conway.NewConwayTransactionInputSet(inputs),
			TxOutputs: outputs,
			TxFee:     1,
		},
		TxIsValid: !spec.invalid,
	}
	if spec.collateral != nil {
		tx.Body.TxCollateral = cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{shelleyInput(t, *spec.collateral)},
			false,
		)
	}
	if spec.collateralReturn != nil {
		ret := indexTxOutput(*spec.collateralReturn, 3_000_000)
		tx.Body.TxCollateralReturn = &ret
	}
	raw, err := tx.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway), raw,
	)
	require.NoError(t, err)
	return raw, decoded
}

// resolvingValidator accepts every transaction and resolves confirmed
// outputs from a table, counting the lookups the pool makes.
type resolvingValidator struct {
	*mockValidator
	mu        sync.Mutex
	confirmed map[utxoref.Key]lcommon.Address
	calls     atomic.Int64
	refs      atomic.Int64
	err       error
	// corrupt makes every resolved output undecodable.
	corrupt bool
	// checksInputs makes validation reject a transaction whose consumed
	// inputs are neither pool outputs nor confirmed, as ledger validation
	// does.
	checksInputs bool
}

func (v *resolvingValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	consumed map[utxoref.Key]struct{},
	created map[utxoref.Key]lcommon.Utxo,
	accounts *utxoref.StateOverlay,
) error {
	return v.validateAgainst(v.snapshot(), tx, consumed, created, accounts)
}

func (v *resolvingValidator) validateAgainst(
	confirmed map[utxoref.Key]lcommon.Address,
	tx gledger.Transaction,
	consumed map[utxoref.Key]struct{},
	created map[utxoref.Key]lcommon.Utxo,
	accounts *utxoref.StateOverlay,
) error {
	if v.checksInputs {
		for _, input := range tx.Consumed() {
			key := utxoref.ForInput(input)
			if _, ok := created[key]; ok {
				continue
			}
			if _, ok := confirmed[key]; !ok {
				return fmt.Errorf("bad input %s", key.String())
			}
		}
	}
	return v.mockValidator.ValidateTxWithOverlay(
		tx, consumed, created, accounts,
	)
}

func newResolvingValidator() *resolvingValidator {
	return &resolvingValidator{
		mockValidator: newMockValidator(),
		confirmed:     make(map[utxoref.Key]lcommon.Address),
	}
}

func (v *resolvingValidator) confirm(in indexTxInput, owner lcommon.Address) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.confirmed[utxoref.Key{
		TxId:  lcommon.NewBlake2b256(in.hash[:]),
		Index: in.index,
	}] = owner
}

func (v *resolvingValidator) unconfirm(in indexTxInput) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.confirmed, utxoref.Key{
		TxId:  lcommon.NewBlake2b256(in.hash[:]),
		Index: in.index,
	})
}

// snapshot copies the confirmed table, as a storage read snapshot sees it.
func (v *resolvingValidator) snapshot() map[utxoref.Key]lcommon.Address {
	v.mu.Lock()
	defer v.mu.Unlock()
	ret := make(map[utxoref.Key]lcommon.Address, len(v.confirmed))
	maps.Copy(ret, v.confirmed)
	return ret
}

func (v *resolvingValidator) UtxosByRefs(
	refs []models.UtxoId,
) ([]models.Utxo, error) {
	return v.resolveFrom(v.snapshot(), refs)
}

func (v *resolvingValidator) resolveFrom(
	confirmed map[utxoref.Key]lcommon.Address,
	refs []models.UtxoId,
) ([]models.Utxo, error) {
	v.calls.Add(1)
	v.refs.Add(int64(len(refs)))
	v.mu.Lock()
	err, corrupt := v.err, v.corrupt
	v.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var ret []models.Utxo
	for _, ref := range refs {
		owner, ok := confirmed[utxoref.Key{
			TxId:  lcommon.NewBlake2b256(ref.Hash),
			Index: ref.Idx,
		}]
		if !ok {
			continue
		}
		raw, err := cbor.Encode(indexTxOutput(owner, 5_000_000))
		if err != nil {
			return nil, err
		}
		if corrupt {
			raw = []byte{0xff}
		}
		ret = append(ret, models.Utxo{
			TxId: ref.Hash, OutputIdx: ref.Idx, Cbor: raw,
		})
	}
	return ret, nil
}

func newIndexTestPool(
	t *testing.T,
	v TxValidator,
	useDAG bool,
	mutate func(*MempoolConfig),
) *Mempool {
	t.Helper()
	cfg := MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       v,
		MempoolCapacity: 1 << 20,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	impl := ImplementationFIFO
	if useDAG {
		impl = ImplementationDAG
	}
	m, err := newMempool(cfg, impl)
	require.NoError(t, err)
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	return m
}

func addIndexTx(t *testing.T, m *Mempool, spec indexTxSpec) string {
	t.Helper()
	raw, decoded := buildIndexTx(t, spec)
	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), raw,
	))
	return decoded.Hash().String()
}

// requireIndexMatchesPool rebuilds the index from the pool's own
// transactions and requires the maintained index to equal it: no stale
// entry, no missing one, no empty bucket left behind.
func requireIndexMatchesPool(t *testing.T, m *Mempool) {
	t.Helper()
	m.RLock()
	defer m.RUnlock()
	want := newAddressIndex()
	want.rebuild(m.transactions)
	require.Len(t, m.addresses.byAddress, len(want.byAddress))
	for address, txs := range want.byAddress {
		got := m.addresses.byAddress[address]
		require.Len(t, got, len(txs), address)
		for hash := range txs {
			require.Contains(t, got, hash, address)
		}
	}
}

func forBothPools(t *testing.T, fn func(t *testing.T, useDAG bool)) {
	t.Helper()
	for _, useDAG := range []bool{false, true} {
		t.Run(fmt.Sprintf("dag=%t", useDAG), func(t *testing.T) {
			t.Parallel()
			fn(t, useDAG)
		})
	}
}

func TestAddressIndexOutputsAndConfirmedInputs(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		v := newResolvingValidator()
		m := newIndexTestPool(t, v, useDAG, nil)
		payee, spender, other := indexTestAddress(t, 1),
			indexTestAddress(t, 2), indexTestAddress(t, 3)
		in := indexTxInput{hash: [32]byte{0xa1}}
		v.confirm(in, spender)

		hash := addIndexTx(t, m, indexTxSpec{
			inputs:  []indexTxInput{in},
			outputs: []lcommon.Address{payee},
		})
		require.Equal(t, int64(1), v.calls.Load())

		require.Equal(t, []string{hash}, m.TransactionHashesByAddress(payee.String()))
		require.Equal(t, []string{hash}, m.TransactionHashesByAddress(spender.String()))
		require.Empty(t, m.TransactionHashesByAddress(other.String()))
		require.Equal(t, int64(1), v.calls.Load(),
			"queries must not resolve inputs again")
		requireIndexMatchesPool(t, m)
	})
}

func TestAddressIndexAnswersWithoutDecodingOrResolving(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		v := newResolvingValidator()
		m := newIndexTestPool(t, v, useDAG, nil)
		payee := indexTestAddress(t, 1)
		var hashes []string
		for i := range 5 {
			hashes = append(hashes, addIndexTx(t, m, indexTxSpec{
				inputs:  []indexTxInput{{hash: [32]byte{0xb0, byte(i)}}},
				outputs: []lcommon.Address{payee},
			}))
		}
		callsBefore := v.calls.Load()
		m.Lock()
		for _, tx := range m.transactions {
			tx.Cbor = []byte{0xff}
		}
		m.Unlock()

		require.Equal(t, hashes, m.TransactionHashesByAddress(payee.String()),
			"a query must not decode pool transactions")
		require.Empty(t, m.TransactionHashesByAddress(
			indexTestAddress(t, 9).String(),
		))
		require.Equal(t, callsBefore, v.calls.Load())
	})
}

func TestAddressIndexChainedChild(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		v := newResolvingValidator()
		m := newIndexTestPool(t, v, useDAG, nil)
		parentPayee, childPayee, funder := indexTestAddress(t, 1),
			indexTestAddress(t, 2), indexTestAddress(t, 3)
		fundIn := indexTxInput{hash: [32]byte{0xc1}}
		v.confirm(fundIn, funder)

		parentRaw, parentTx := buildIndexTx(t, indexTxSpec{
			inputs:  []indexTxInput{fundIn},
			outputs: []lcommon.Address{parentPayee},
		})
		require.NoError(t, m.AddTransaction(
			context.Background(), uint(conway.EraIdConway), parentRaw,
		))
		parent := parentTx.Hash().String()
		require.Equal(t, int64(1), v.calls.Load())

		child := addIndexTx(t, m, indexTxSpec{
			inputs: []indexTxInput{{
				hash: [32]byte(parentTx.Hash().Bytes()),
			}},
			outputs: []lcommon.Address{childPayee},
		})
		require.Equal(t, int64(1), v.calls.Load(),
			"an input naming a pool output needs no storage lookup")

		// The child spends the parent's output, so it spends from that
		// address, and it pays its own.
		require.Equal(t, []string{parent, child},
			m.TransactionHashesByAddress(parentPayee.String()))
		require.Equal(t, []string{child},
			m.TransactionHashesByAddress(childPayee.String()))
		require.Equal(t, []string{parent},
			m.TransactionHashesByAddress(funder.String()))
		requireIndexMatchesPool(t, m)

		// Confirming the parent leaves the child's entries untouched: its
		// input address is the same whether the output is pending or
		// confirmed.
		m.RemoveTxsByHash([]string{parent})
		require.Equal(t, []string{child},
			m.TransactionHashesByAddress(parentPayee.String()))
		require.Empty(t, m.TransactionHashesByAddress(funder.String()))
		requireIndexMatchesPool(t, m)
	})
}

func TestAddressIndexFailingPhaseTwo(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		v := newResolvingValidator()
		m := newIndexTestPool(t, v, useDAG, nil)
		payee, collateralPayee, spender, collateralOwner :=
			indexTestAddress(t, 1), indexTestAddress(t, 2),
			indexTestAddress(t, 3), indexTestAddress(t, 4)
		in := indexTxInput{hash: [32]byte{0xd1}}
		collateral := indexTxInput{hash: [32]byte{0xd2}}
		v.confirm(in, spender)
		v.confirm(collateral, collateralOwner)

		hash := addIndexTx(t, m, indexTxSpec{
			inputs:           []indexTxInput{in},
			outputs:          []lcommon.Address{payee},
			collateral:       &collateral,
			collateralReturn: &collateralPayee,
			invalid:          true,
		})

		require.Equal(t, []string{hash},
			m.TransactionHashesByAddress(collateralPayee.String()))
		require.Equal(t, []string{hash},
			m.TransactionHashesByAddress(collateralOwner.String()))
		require.Empty(t, m.TransactionHashesByAddress(payee.String()),
			"a discarded regular output is not paid")
		require.Empty(t, m.TransactionHashesByAddress(spender.String()),
			"an unspent regular input is not spent")
	})
}

// TestAddressIndexFollowsPoolOrder compares against TransactionHashes, the
// order the listing endpoint reports, across removal of an earlier entry.
func TestAddressIndexFollowsPoolOrder(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		m := newIndexTestPool(t, newResolvingValidator(), useDAG, nil)
		payee := indexTestAddress(t, 1)
		var hashes []string
		for i := range 6 {
			hashes = append(hashes, addIndexTx(t, m, indexTxSpec{
				inputs:  []indexTxInput{{hash: [32]byte{0xe0, byte(i)}}},
				outputs: []lcommon.Address{payee},
			}))
		}
		m.RemoveTransaction(hashes[1])
		addIndexTx(t, m, indexTxSpec{
			inputs:  []indexTxInput{{hash: [32]byte{0xe1}}},
			outputs: []lcommon.Address{payee},
		})
		want, _ := m.TransactionHashes(0)
		require.Len(t, want, 6)
		require.Equal(t, want, m.TransactionHashesByAddress(payee.String()))
	})
}

// TestAddressIndexFollowsDAGOrder reorders the DAG against admission, so only
// a query that reads the DAG's order matches the listing.
func TestAddressIndexFollowsDAGOrder(t *testing.T) {
	t.Parallel()
	m := newIndexTestPool(t, newResolvingValidator(), true, nil)
	payee := indexTestAddress(t, 1)
	var admitted []string
	for i := range 4 {
		admitted = append(admitted, addIndexTx(t, m, indexTxSpec{
			inputs:  []indexTxInput{{hash: [32]byte{0x31, byte(i)}}},
			outputs: []lcommon.Address{payee},
		}))
	}
	m.Lock()
	reversed := make([]appliedTx, 0, len(m.overlay.applied))
	for _, at := range slices.Backward(m.overlay.applied) {
		reversed = append(reversed, at)
	}
	m.dag.rebuild(reversed)
	m.Unlock()

	want, _ := m.TransactionHashes(0)
	require.NotEqual(t, admitted, want, "control: the DAG order differs")
	require.Equal(t, want, m.TransactionHashesByAddress(payee.String()))
}

func TestAddressIndexResolverFailureRejectsAdmission(t *testing.T) {
	t.Parallel()
	v := newResolvingValidator()
	v.err = errors.New("storage unavailable")
	m := newIndexTestPool(t, v, false, nil)
	raw, _ := buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{{hash: [32]byte{0xf2}}},
		outputs: []lcommon.Address{indexTestAddress(t, 1)},
	})
	err := m.AddTransaction(context.Background(), uint(conway.EraIdConway), raw)
	require.ErrorContains(t, err, "storage unavailable")
	require.Empty(t, m.Transactions())
	require.Empty(t, m.TransactionHashesByAddress(indexTestAddress(t, 1).String()))
	requireIndexMatchesPool(t, m)
}

// removalFixture admits a parent paying addrA, a child of the parent paying
// addrB and an unrelated transaction paying addrC.
type removalFixture struct {
	m                    *Mempool
	v                    *resolvingValidator
	parent, child, loner string
	addrA, addrB, addrC  lcommon.Address
	funder               lcommon.Address
	parentRaw, lonerRaw  []byte
}

func newRemovalFixture(
	t *testing.T,
	useDAG bool,
	mutate func(*MempoolConfig),
) *removalFixture {
	t.Helper()
	f := &removalFixture{v: newResolvingValidator()}
	f.m = newIndexTestPool(t, f.v, useDAG, mutate)
	f.addrA, f.addrB, f.addrC, f.funder = indexTestAddress(t, 1),
		indexTestAddress(t, 2), indexTestAddress(t, 3),
		indexTestAddress(t, 4)
	fundIn := indexTxInput{hash: [32]byte{0x11}}
	f.v.confirm(fundIn, f.funder)
	var parentTx gledger.Transaction
	f.parentRaw, parentTx = buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{fundIn},
		outputs: []lcommon.Address{f.addrA},
	})
	require.NoError(t, f.m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), f.parentRaw,
	))
	f.parent = parentTx.Hash().String()
	f.child = addIndexTx(t, f.m, indexTxSpec{
		inputs:  []indexTxInput{{hash: [32]byte(parentTx.Hash().Bytes())}},
		outputs: []lcommon.Address{f.addrB},
	})
	// Ledger validation only passes a transaction whose inputs exist, so
	// the loner's input is a confirmed output too.
	lonerIn := indexTxInput{hash: [32]byte{0x12}}
	f.v.confirm(lonerIn, indexTestAddress(t, 5))
	var lonerTx gledger.Transaction
	f.lonerRaw, lonerTx = buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{lonerIn},
		outputs: []lcommon.Address{f.addrC},
	})
	require.NoError(t, f.m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), f.lonerRaw,
	))
	f.loner = lonerTx.Hash().String()
	require.Equal(t, []string{f.parent, f.child},
		f.m.TransactionHashesByAddress(f.addrA.String()))
	return f
}

func (f *removalFixture) hashesFor(a lcommon.Address) []string {
	return f.m.TransactionHashesByAddress(a.String())
}

// Each removal path below must leave the index equal to one rebuilt from the
// pool, and every address of a removed transaction empty.

func TestAddressIndexRemoveTransactionPrunesDescendants(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newRemovalFixture(t, useDAG, nil)
		f.m.RemoveTransaction(f.parent)
		require.Empty(t, f.hashesFor(f.addrA))
		require.Empty(t, f.hashesFor(f.addrB), "descendant pruned with parent")
		require.Empty(t, f.hashesFor(f.funder))
		require.Equal(t, []string{f.loner}, f.hashesFor(f.addrC))
		requireIndexMatchesPool(t, f.m)
	})
}

func TestAddressIndexRemoveTxsByHashKeepsDescendants(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newRemovalFixture(t, useDAG, nil)
		f.m.RemoveTxsByHash([]string{f.parent, f.loner})
		require.Equal(t, []string{f.child}, f.hashesFor(f.addrA))
		require.Equal(t, []string{f.child}, f.hashesFor(f.addrB))
		require.Empty(t, f.hashesFor(f.addrC))
		require.Empty(t, f.hashesFor(f.funder))
		requireIndexMatchesPool(t, f.m)
	})
}

func TestAddressIndexBlockInclusionAndRollbackReadmission(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newRemovalFixture(t, useDAG, nil)
		ctx := context.Background()
		f.m.confirmedMu.Lock()
		f.m.pendingConfirmed = map[string]struct{}{f.loner: {}}
		f.m.confirmedMu.Unlock()
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.Empty(t, f.hashesFor(f.addrC), "included in a block")
		requireIndexMatchesPool(t, f.m)

		// The block rolls back: the transaction is admitted again and
		// resolves its inputs afresh.
		callsBefore := f.v.calls.Load()
		f.m.confirmedMu.Lock()
		f.m.pendingRolledBack = map[string]MempoolTransaction{
			f.loner: {
				Hash: f.loner, Cbor: f.lonerRaw,
				Type: uint(conway.EraIdConway),
			},
		}
		f.m.pendingRolledBackOrder = []string{f.loner}
		f.m.confirmedMu.Unlock()
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.Equal(t, []string{f.loner}, f.hashesFor(f.addrC))
		require.Equal(t, callsBefore+1, f.v.calls.Load())
		requireIndexMatchesPool(t, f.m)
	})
}

func TestAddressIndexExpiryPrunesDescendants(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newRemovalFixture(t, useDAG, func(c *MempoolConfig) {
			c.TransactionTTL = time.Hour
		})
		f.m.Lock()
		f.m.txByHash[f.parent].LastSeen = time.Now().Add(-2 * time.Hour)
		f.m.Unlock()
		f.m.removeExpiredTransactions()
		require.Empty(t, f.hashesFor(f.addrA))
		require.Empty(t, f.hashesFor(f.addrB))
		require.Empty(t, f.hashesFor(f.funder))
		require.Equal(t, []string{f.loner}, f.hashesFor(f.addrC))
		requireIndexMatchesPool(t, f.m)
	})
}

func TestAddressIndexRevalidationDropsInvalid(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newRemovalFixture(t, useDAG, nil)
		f.v.setFailAll(true)
		require.NoError(t, f.m.rebuildOverlay(context.Background()))
		require.Empty(t, f.m.Transactions())
		for _, a := range []lcommon.Address{
			f.addrA, f.addrB, f.addrC, f.funder,
		} {
			require.Empty(t, f.hashesFor(a))
		}
		requireIndexMatchesPool(t, f.m)
	})
}

func TestAddressIndexRevalidationKeepsSurvivors(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newRemovalFixture(t, useDAG, nil)
		f.v.failHashes[f.loner] = true
		require.NoError(t, f.m.rebuildOverlay(context.Background()))
		require.Empty(t, f.hashesFor(f.addrC))
		require.Equal(t, []string{f.parent, f.child}, f.hashesFor(f.addrA))
		require.Equal(t, []string{f.child}, f.hashesFor(f.addrB))
		requireIndexMatchesPool(t, f.m)
	})
}

func TestAddressIndexStopClears(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newRemovalFixture(t, useDAG, nil)
		require.NoError(t, f.m.Stop(context.Background()))
		require.Empty(t, f.hashesFor(f.addrA))
		require.Empty(t, f.m.addresses.byAddress)
	})
}

// TestAddressIndexCapacityEviction fills a watermarked FIFO pool so the next
// admission evicts its oldest entry together with that entry's descendant.
func TestAddressIndexCapacityEviction(t *testing.T) {
	t.Parallel()
	probe := newRemovalFixture(t, false, nil)
	size := probe.m.SizeBytes() / 3
	f := newRemovalFixture(t, false, func(c *MempoolConfig) {
		c.MempoolCapacity = 5 * size
		c.EvictionWatermark = 0.7
		c.RejectionWatermark = 1
	})
	require.Equal(t, []string{f.parent, f.child}, f.hashesFor(f.addrA))

	newcomer := indexTestAddress(t, 7)
	hash := addIndexTx(t, f.m, indexTxSpec{
		inputs:  []indexTxInput{{hash: [32]byte{0x13}}},
		outputs: []lcommon.Address{newcomer},
	})
	require.Empty(t, f.hashesFor(f.addrA), "oldest entry evicted")
	require.Empty(t, f.hashesFor(f.addrB), "its descendant pruned")
	require.Empty(t, f.hashesFor(f.funder))
	require.Equal(t, []string{hash}, f.hashesFor(newcomer))
	requireIndexMatchesPool(t, f.m)
	_, pooled := f.m.GetTransaction(f.parent)
	require.False(t, pooled)
}

// TestAddressIndexConcurrentMutationAndQueries is for the race detector:
// queries read the index while admissions and removals mutate it.
func TestAddressIndexConcurrentMutationAndQueries(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		m := newIndexTestPool(t, newResolvingValidator(), useDAG, nil)
		payee := indexTestAddress(t, 1)
		stop := make(chan struct{})
		var readers sync.WaitGroup
		for range 4 {
			readers.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						_ = m.TransactionHashesByAddress(payee.String())
					}
				}
			})
		}
		for i := range 20 {
			hash := addIndexTx(t, m, indexTxSpec{
				inputs:  []indexTxInput{{hash: [32]byte{0x21, byte(i)}}},
				outputs: []lcommon.Address{payee},
			})
			if i%2 == 0 {
				m.RemoveTransaction(hash)
			}
		}
		close(stop)
		readers.Wait()
		require.Len(t, m.TransactionHashesByAddress(payee.String()), 10)
		requireIndexMatchesPool(t, m)
	})
}

// sessionConnValidator models LedgerState's validation session: it holds one
// of a fixed number of storage read connections for its whole duration and
// reads one snapshot of the confirmed table, which its validations and its
// resolver share. Its standalone UtxosByRefs needs a connection of its own.
type sessionConnValidator struct {
	*resolvingValidator
	conns chan struct{}
	// beforeSession runs once, before the first session takes its
	// connection.
	beforeSession func()
	once          sync.Once
	// eachSession, when set, runs before every session takes its
	// connection.
	eachSession func()
	// duringSession, when set, runs inside every session after its
	// snapshot is taken.
	duringSession func()
	standalone    atomic.Int64
}

type sessionValidateFunc = func(
	gledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]lcommon.Utxo,
	*utxoref.StateOverlay,
) error

func (v *sessionConnValidator) open() (
	sessionValidateFunc,
	func([]models.UtxoId) ([]models.Utxo, error),
	func(),
) {
	if v.beforeSession != nil {
		v.once.Do(v.beforeSession)
	}
	if v.eachSession != nil {
		v.eachSession()
	}
	v.conns <- struct{}{}
	snap := v.snapshot()
	if v.duringSession != nil {
		v.duringSession()
	}
	validate := func(
		tx gledger.Transaction,
		consumed map[utxoref.Key]struct{},
		created map[utxoref.Key]lcommon.Utxo,
		accounts *utxoref.StateOverlay,
	) error {
		return v.validateAgainst(snap, tx, consumed, created, accounts)
	}
	resolve := func(refs []models.UtxoId) ([]models.Utxo, error) {
		return v.resolveFrom(snap, refs)
	}
	return validate, resolve, func() { <-v.conns }
}

func (v *sessionConnValidator) WithTxValidationSession(
	_ context.Context,
	fn func(
		sessionValidateFunc,
		func() bool,
		func(func() error) (bool, error),
	) error,
) error {
	validate, _, done := v.open()
	defer done()
	return fn(
		validate,
		func() bool { return true },
		func(commit func() error) (bool, error) { return true, commit() },
	)
}

func (v *sessionConnValidator) WithResolvingTxValidationSession(
	_ context.Context,
	fn func(
		sessionValidateFunc,
		func() bool,
		func(func() error) (bool, error),
		func([]models.UtxoId) ([]models.Utxo, error),
	) error,
) error {
	validate, resolve, done := v.open()
	defer done()
	return fn(
		validate,
		func() bool { return true },
		func(commit func() error) (bool, error) { return true, commit() },
		resolve,
	)
}

func (v *sessionConnValidator) UtxosByRefs(
	refs []models.UtxoId,
) ([]models.Utxo, error) {
	v.standalone.Add(1)
	select {
	case v.conns <- struct{}{}:
	case <-time.After(2 * time.Second):
		return nil, errors.New("no read connection available")
	}
	defer func() { <-v.conns }()
	return v.resolvingValidator.UtxosByRefs(refs)
}

func newSessionConnValidator() *sessionConnValidator {
	v := &sessionConnValidator{
		resolvingValidator: newResolvingValidator(),
		conns:              make(chan struct{}, 1),
	}
	v.checksInputs = true
	return v
}

// TestAddressIndexResolvesInputsInsideValidationSession pins that admission
// resolves confirmed inputs through its validation session and never needs a
// second storage connection while that session holds one. Sessions are opened
// before the mutation gate, so as many concurrent admissions as the read pool
// has connections would otherwise leave the gate holder waiting on a
// connection the waiters behind the gate never release.
func TestAddressIndexResolvesInputsInsideValidationSession(t *testing.T) {
	t.Parallel()
	v := newSessionConnValidator()
	m := newIndexTestPool(t, v, false, nil)
	spender := indexTestAddress(t, 2)
	in := indexTxInput{hash: [32]byte{0x51}}
	v.confirm(in, spender)

	hash := addIndexTx(t, m, indexTxSpec{
		inputs:  []indexTxInput{in},
		outputs: []lcommon.Address{indexTestAddress(t, 1)},
	})
	require.Equal(t, []string{hash},
		m.TransactionHashesByAddress(spender.String()))
	require.Zero(t, v.standalone.Load(),
		"no lookup outside the session's own connection")
	requireIndexMatchesPool(t, m)
}

// TestAddressIndexResolvesFromValidationSnapshot covers an input confirmed
// in the snapshot a validation reads and rolled back or confirmed around it:
// the index reads the same snapshot, so the address is found whatever the
// live table holds before or after.
func TestAddressIndexResolvesFromValidationSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		before func(v *sessionConnValidator, in indexTxInput, a lcommon.Address)
		during func(v *sessionConnValidator, in indexTxInput, a lcommon.Address)
	}{
		{
			name: "confirmed as the session opens",
			before: func(v *sessionConnValidator, in indexTxInput, a lcommon.Address) {
				v.confirm(in, a)
			},
		},
		{
			name: "rolled back while the session runs",
			before: func(v *sessionConnValidator, in indexTxInput, a lcommon.Address) {
				v.confirm(in, a)
			},
			during: func(v *sessionConnValidator, in indexTxInput, _ lcommon.Address) {
				v.unconfirm(in)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := newSessionConnValidator()
			m := newIndexTestPool(t, v, false, nil)
			owner := indexTestAddress(t, 0x81)
			in := indexTxInput{hash: [32]byte{0x82}}
			v.eachSession = func() { tc.before(v, in, owner) }
			if tc.during != nil {
				v.duringSession = func() { tc.during(v, in, owner) }
			}
			hash := addIndexTx(t, m, indexTxSpec{
				inputs:  []indexTxInput{in},
				outputs: []lcommon.Address{indexTestAddress(t, 0x83)},
			})
			require.Equal(t, []string{hash},
				m.TransactionHashesByAddress(owner.String()))
			requireIndexMatchesPool(t, m)
		})
	}
}

// parentsLeavingFixture admits three parents and builds a child spending one
// output of each. Once armed, every admission session outside a pool rebuild
// first confirms one remaining parent and removes it from the pool.
type parentsLeavingFixture struct {
	m        *Mempool
	v        *sessionConnValidator
	payees   []lcommon.Address
	parents  []string
	childRaw []byte
	child    string
	armed    atomic.Bool
}

func newParentsLeavingFixture(t *testing.T) *parentsLeavingFixture {
	t.Helper()
	f := &parentsLeavingFixture{v: newSessionConnValidator()}
	f.m = newIndexTestPool(t, f.v, false, nil)
	var childInputs []indexTxInput
	var outs []indexTxInput
	for i := range 3 {
		payee := indexTestAddress(t, byte(0x70+i))
		fund := indexTxInput{hash: [32]byte{0x71, byte(i)}}
		f.v.confirm(fund, indexTestAddress(t, 0x6f))
		raw, tx := buildIndexTx(t, indexTxSpec{
			inputs:  []indexTxInput{fund},
			outputs: []lcommon.Address{payee},
		})
		require.NoError(t, f.m.AddTransaction(
			context.Background(), uint(conway.EraIdConway), raw,
		))
		out := indexTxInput{hash: [32]byte(tx.Hash().Bytes())}
		f.payees = append(f.payees, payee)
		f.parents = append(f.parents, tx.Hash().String())
		outs = append(outs, out)
		childInputs = append(childInputs, out)
	}
	raw, tx := buildIndexTx(t, indexTxSpec{
		inputs:  childInputs,
		outputs: []lcommon.Address{indexTestAddress(t, 0x7f)},
	})
	f.childRaw, f.child = raw, tx.Hash().String()
	next := 0
	f.v.eachSession = func() {
		if !f.armed.Load() || next == len(outs) {
			return
		}
		// A pool rebuild holds rebuildMutex; only admission sessions run
		// without it.
		if !f.m.rebuildMutex.TryLock() {
			return
		}
		f.m.rebuildMutex.Unlock()
		f.v.confirm(outs[next], f.payees[next])
		f.m.RemoveTxsByHash([]string{f.parents[next]})
		next++
	}
	return f
}

// TestAddressIndexParentConfirmedDuringAdmissionIsIndexed covers a pool
// parent confirmed and removed from the pool just before the child's
// validation session: the child spends one confirmed output and two pool
// outputs, and is indexed under every parent's address on the first attempt.
func TestAddressIndexParentConfirmedDuringAdmissionIsIndexed(t *testing.T) {
	t.Parallel()
	f := newParentsLeavingFixture(t)
	f.armed.Store(true)
	require.NoError(t, f.m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), f.childRaw,
	))
	for _, payee := range f.payees {
		require.Contains(t,
			f.m.TransactionHashesByAddress(payee.String()), f.child)
	}
	requireIndexMatchesPool(t, f.m)
}

// TestAddressIndexParentConfirmedDuringReadmissionIsIndexed is the rollback
// readmission side: the restored child is admitted and fully indexed.
func TestAddressIndexParentConfirmedDuringReadmissionIsIndexed(
	t *testing.T,
) {
	t.Parallel()
	f := newParentsLeavingFixture(t)
	block := blockCarryingTx(t, f.childRaw)
	f.m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{block},
	}))
	f.armed.Store(true)
	require.NoError(t, f.m.rebuildAfterChainUpdate(context.Background()))
	_, ok := f.m.GetTransaction(f.child)
	require.True(t, ok)
	for _, payee := range f.payees {
		require.Contains(t,
			f.m.TransactionHashesByAddress(payee.String()), f.child)
	}
	requireIndexMatchesPool(t, f.m)
}

// TestAddressIndexResolverFailureKeepsRolledBackTransaction pins that a
// storage fault while resolving input addresses is not a verdict: a
// transaction restored from a rolled-back block stays pending for the next
// rebuild instead of being dropped as invalid on the selected chain.
func TestAddressIndexResolverFailureKeepsRolledBackTransaction(t *testing.T) {
	t.Parallel()
	v := newResolvingValidator()
	m := newIndexTestPool(t, v, false, nil)
	in := indexTxInput{hash: [32]byte{0x61}}
	spender := indexTestAddress(t, 2)
	v.confirm(in, spender)
	raw, tx := buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{in},
		outputs: []lcommon.Address{indexTestAddress(t, 1)},
	})
	hash := tx.Hash().String()
	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), raw,
	))
	block := blockCarryingTx(t, raw)
	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	_, ok := m.GetTransaction(hash)
	require.False(t, ok, "control: the block confirmed the transaction")

	v.mu.Lock()
	v.err = errors.New("database is locked")
	v.mu.Unlock()
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{block},
	}))
	require.ErrorIs(t, m.rebuildAfterChainUpdate(context.Background()),
		ErrInputResolution)
	require.Equal(t, []string{hash}, m.pendingRolledBackOrder)

	v.mu.Lock()
	v.err = nil
	v.mu.Unlock()
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	_, ok = m.GetTransaction(hash)
	require.True(t, ok)
	require.Equal(t, []string{hash},
		m.TransactionHashesByAddress(spender.String()))
	requireIndexMatchesPool(t, m)
}

// TestAddressIndexUndecodableConfirmedInputIsNotAVerdict: a stored output
// that does not decode is a storage fault, as ledger validation treats it,
// so admission reaches no verdict instead of indexing the transaction
// without that address.
func TestAddressIndexUndecodableConfirmedInputIsNotAVerdict(t *testing.T) {
	t.Parallel()
	v := newResolvingValidator()
	v.corrupt = true
	m := newIndexTestPool(t, v, false, nil)
	in := indexTxInput{hash: [32]byte{0x84}}
	v.confirm(in, indexTestAddress(t, 0x85))
	raw, _ := buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{in},
		outputs: []lcommon.Address{indexTestAddress(t, 0x86)},
	})
	err := m.AddTransaction(context.Background(), uint(conway.EraIdConway), raw)
	require.ErrorIs(t, err, ErrInputResolution)
	require.Empty(t, m.Transactions())
	requireIndexMatchesPool(t, m)
}

// TestAddressIndexInputWithNoLiveOutputIsLeftOut covers a validation that
// does not check inputs, as the Dijkstra prototype bypass and partial rule
// sets do: an input the validated snapshot does not hold has no address to
// index. The transaction is admitted on the first attempt, and restoring it
// from a rolled-back block does not leave it pending for every rebuild.
func TestAddressIndexInputWithNoLiveOutputIsLeftOut(t *testing.T) {
	t.Parallel()
	v := newResolvingValidator()
	m := newIndexTestPool(t, v, false, nil)
	payee := indexTestAddress(t, 0x87)
	raw, tx := buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{{hash: [32]byte{0x88}}},
		outputs: []lcommon.Address{payee},
	})
	hash := tx.Hash().String()
	require.NoError(t, m.AddTransaction(
		context.Background(), uint(conway.EraIdConway), raw,
	))
	require.Equal(t, int64(1), v.calls.Load(), "admitted on the first attempt")
	require.Equal(t, []string{hash},
		m.TransactionHashesByAddress(payee.String()))

	block := blockCarryingTx(t, raw)
	m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{block},
	}))
	require.NoError(t, m.rebuildAfterChainUpdate(context.Background()))
	require.Empty(t, m.pendingRolledBackOrder)
	_, ok := m.GetTransaction(hash)
	require.True(t, ok)
	requireIndexMatchesPool(t, m)
}

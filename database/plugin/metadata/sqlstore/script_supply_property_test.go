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
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// fullScanScriptLockedSupply is the query GetScriptLockedSupply ran before the
// running aggregate: the oracle every step of the property test compares to.
func fullScanScriptLockedSupply(t testing.TB, store *Store) uint64 {
	t.Helper()
	total, err := sumUint64Rows(
		context.Background(),
		store.writeDB,
		`SELECT amount FROM utxo WHERE payment_script = TRUE AND deleted_slot = 0`,
	)
	require.NoError(t, err)
	return total
}

// latestScriptSupplyRow reads the aggregate straight from its table, so a
// regression in GetScriptLockedSupply cannot hide a wrong table.
func latestScriptSupplyRow(
	t testing.TB,
	store *Store,
) (slot, amount int64, ok bool) {
	t.Helper()
	err := store.writeDB.QueryRowContext(
		context.Background(),
		`SELECT slot, amount FROM script_locked_supply ORDER BY slot DESC LIMIT 1`,
	).Scan(&slot, &amount)
	if err == sql.ErrNoRows {
		return 0, 0, false
	}
	require.NoError(t, err)
	return slot, amount, true
}

// supplySim drives random blocks into one store. Inputs are drawn from the
// live rows the store itself holds, so every block is consistent with
// whatever earlier blocks and rollbacks left behind.
type supplySim struct {
	t      *testing.T
	store  *Store
	rng    *rand.Rand
	slot   uint64
	txSeq  uint64
	addrs  []lcommon.Address
	script []bool
}

func newSupplySim(t *testing.T, store *Store, seed int64) *supplySim {
	t.Helper()
	sim := &supplySim{t: t, store: store, rng: rand.New(rand.NewSource(seed))}
	kinds := []struct {
		kind   uint8
		script bool
	}{
		{lcommon.AddressTypeKeyNone, false},
		{lcommon.AddressTypeScriptNone, true},
		{lcommon.AddressTypeKeyKey, false},
		{lcommon.AddressTypeScriptKey, true},
		{lcommon.AddressTypeKeyScript, false},
		{lcommon.AddressTypeScriptScript, true},
	}
	for i, k := range kinds {
		for j := range 3 {
			payment := lcommon.NewBlake2b224([]byte{byte(i), byte(j), 1}).
				Bytes()
			staking := lcommon.NewBlake2b224([]byte{byte(i), byte(j), 2}).
				Bytes()
			var stakePart []byte
			switch k.kind {
			case lcommon.AddressTypeKeyNone, lcommon.AddressTypeScriptNone:
			default:
				stakePart = staking
			}
			addr, err := lcommon.NewAddressFromParts(
				k.kind, lcommon.AddressNetworkTestnet, payment, stakePart,
			)
			require.NoError(t, err)
			sim.addrs = append(sim.addrs, addr)
			sim.script = append(sim.script, k.script)
		}
	}
	return sim
}

func (s *supplySim) output(
	addrIdx int,
	lovelace uint64,
) lcommon.TransactionOutput {
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(s.addrs[addrIdx].String()).
		WithLovelace(lovelace).
		Build()
	require.NoError(s.t, err)
	return out
}

func (s *supplySim) lovelace() uint64 {
	if s.rng.Intn(20) == 0 {
		// Above 2^53 so any float conversion of an amount shows up.
		return 9_010_000_000_000_000 + uint64(s.rng.Intn(1_000_000))
	}
	return 1_000_000 + uint64(s.rng.Intn(500_000_000))
}

type supplyInput struct {
	hash []byte
	idx  uint32
}

func (s *supplySim) liveInputs() []supplyInput {
	rows, err := s.store.writeDB.QueryContext(
		context.Background(),
		`SELECT tx_id, output_idx FROM utxo WHERE deleted_slot = 0 ORDER BY id`,
	)
	require.NoError(s.t, err)
	defer rows.Close()
	var live []supplyInput
	for rows.Next() {
		var in supplyInput
		require.NoError(s.t, rows.Scan(&in.hash, &in.idx))
		live = append(live, in)
	}
	require.NoError(s.t, rows.Err())
	return live
}

func (s *supplySim) txID() []byte {
	s.txSeq++
	id := make([]byte, 32)
	binary.BigEndian.PutUint64(id, s.txSeq)
	id[31] = 0xA5
	return id
}

type simBlock struct {
	point ocommon.Point
	txs   []lcommon.Transaction
	// consumed are the inputs the block spends, preBatch ones first.
	consumed []supplyInput
}

// buildBlock builds one block. produced holds outputs earlier blocks of the
// same batch created and a later block may spend again.
func (s *supplySim) buildBlock(
	produced *[]supplyInput,
	spentInBatch map[string]struct{},
) simBlock {
	s.slot += 1 + uint64(s.rng.Intn(20))
	blockHash := make([]byte, 32)
	binary.BigEndian.PutUint64(blockHash, s.slot)
	block := simBlock{point: ocommon.Point{Slot: s.slot, Hash: blockHash}}
	claimed := map[string]struct{}{}
	key := func(in supplyInput) string {
		return fmt.Sprintf("%x:%d", in.hash, in.idx)
	}
	pick := func(n int) []supplyInput {
		var pool []supplyInput
		for _, in := range s.liveInputs() {
			if _, gone := spentInBatch[key(in)]; gone {
				continue
			}
			pool = append(pool, in)
		}
		for _, in := range *produced {
			if _, gone := spentInBatch[key(in)]; gone {
				continue
			}
			pool = append(pool, in)
		}
		s.rng.Shuffle(
			len(pool),
			func(i, j int) { pool[i], pool[j] = pool[j], pool[i] },
		)
		var chosen []supplyInput
		for _, in := range pool {
			if len(chosen) == n {
				break
			}
			if _, taken := claimed[key(in)]; taken {
				continue
			}
			claimed[key(in)] = struct{}{}
			spentInBatch[key(in)] = struct{}{}
			chosen = append(chosen, in)
		}
		return chosen
	}
	txCount := 1 + s.rng.Intn(3)
	for range txCount {
		id := s.txID()
		tx := mockledger.NewTransactionBuilder().WithCollateral()
		tx.WithId(id)
		invalid := s.rng.Intn(6) == 0
		inputs := pick(s.rng.Intn(3))
		if invalid {
			if len(inputs) == 0 {
				inputs = pick(1)
			}
			var ins []lcommon.TransactionInput
			for _, in := range inputs {
				ins = append(ins, overlayTxInput(s.t, in.hash, in.idx))
			}
			ret := s.output(s.rng.Intn(len(s.addrs)), s.lovelace())
			// Phase-2-invalid: the ledger consumes the collateral inputs
			// and produces only the collateral return.
			tx.WithInputs(ins...)
			tx.WithOutputs(ret)
			tx.WithValid(false)
			tx.WithCollateral(ins...)
			tx.WithCollateralReturn(ret)
			block.consumed = append(block.consumed, inputs...)
			*produced = append(*produced, supplyInput{hash: id, idx: 0})
			block.txs = append(block.txs, tx)
			continue
		}
		var ins []lcommon.TransactionInput
		for _, in := range inputs {
			ins = append(ins, overlayTxInput(s.t, in.hash, in.idx))
		}
		tx.WithInputs(ins...)
		outCount := s.rng.Intn(4)
		var outs []lcommon.TransactionOutput
		for i := range outCount {
			outs = append(
				outs,
				s.output(s.rng.Intn(len(s.addrs)), s.lovelace()),
			)
			*produced = append(*produced, supplyInput{hash: id, idx: uint32(i)})
		}
		tx.WithOutputs(outs...)
		tx.WithValid(true)
		block.consumed = append(block.consumed, inputs...)
		block.txs = append(block.txs, tx)
	}
	return block
}

type applyMode int

const (
	applyPerRow applyMode = iota
	applyRowBatching
	applyOverlay
)

func (m applyMode) String() string {
	return [...]string{"per-row", "row-batching", "overlay"}[m]
}

// applyBatch applies blocks the way each ledger path does: one write
// transaction per transaction (per-row), or one transaction for the whole
// batch through an accumulator, with or without the UTxO overlay.
func (s *supplySim) applyBatch(mode applyMode, blocks []simBlock) {
	s.t.Helper()
	if mode == applyPerRow {
		for _, b := range blocks {
			for i, tx := range b.txs {
				require.NoError(s.t, s.store.SetTransaction(
					tx, b.point, uint32(i), nil, true, nil, 0,
				))
			}
		}
		return
	}
	txn := s.store.Transaction(context.Background())
	acc := s.store.NewBatchAccumulator()
	if mode == applyOverlay {
		produced := map[string]struct{}{}
		var prebatch []models.UtxoId
		for _, b := range blocks {
			for _, tx := range b.txs {
				for _, out := range tx.Produced() {
					produced[fmt.Sprintf("%x:%d", out.Id.Id().Bytes(), out.Id.Index())] = struct{}{}
				}
			}
		}
		for _, b := range blocks {
			for _, in := range b.consumed {
				if _, ok := produced[fmt.Sprintf("%x:%d", in.hash, in.idx)]; !ok {
					prebatch = append(
						prebatch,
						models.UtxoId{Hash: in.hash, Idx: in.idx},
					)
				}
			}
		}
		enabled, err := s.store.EnableBatchUtxoOverlay(acc, txn, prebatch, nil)
		require.NoError(s.t, err)
		require.True(s.t, enabled)
	}
	for _, b := range blocks {
		for i, tx := range b.txs {
			require.NoError(s.t, s.store.SetTransactionBatched(
				tx, b.point, uint32(i), nil, true, acc, txn, 0,
			))
		}
		require.NoError(s.t, s.store.FlushBatchStakeDeltas(acc, txn))
	}
	require.NoError(s.t, s.store.FlushBatch(acc, txn))
	require.NoError(s.t, txn.Commit())
}

// rollback runs the metadata steps of the production rollback sweep in its
// order: delete created outputs, delete transactions, un-spend.
func (s *supplySim) rollback(slot uint64) {
	s.t.Helper()
	require.NoError(s.t, s.store.DeleteUtxosAfterSlot(slot, nil))
	require.NoError(s.t, s.store.DeleteTransactionsAfterSlot(slot, nil))
	require.NoError(s.t, s.store.SetUtxosNotDeletedAfterSlot(slot, nil))
	s.slot = slot
}

func (s *supplySim) assertAggregateMatchesScan(step string) {
	s.t.Helper()
	want := fullScanScriptLockedSupply(s.t, s.store)
	_, got, _ := latestScriptSupplyRow(s.t, s.store)
	require.Equal(s.t, want, uint64(got), "aggregate table after %s", step)
	read, err := s.store.GetScriptLockedSupply(nil)
	require.NoError(s.t, err)
	require.Equal(s.t, want, read, "GetScriptLockedSupply after %s", step)
}

// TestScriptLockedSupplyAggregateMatchesFullScan applies random blocks of key
// and script outputs, spends of both, phase-2-invalid transactions and
// rollbacks of random depth, on every apply path, and requires the running
// aggregate to equal the full-scan SUM it replaced after every step.
func TestScriptLockedSupplyAggregateMatchesFullScan(t *testing.T) {
	t.Parallel()
	for _, mode := range []applyMode{applyPerRow, applyRowBatching, applyOverlay} {
		for seed := int64(1); seed <= 3; seed++ {
			t.Run(fmt.Sprintf("%s/seed=%d", mode, seed), func(t *testing.T) {
				t.Parallel()
				store := newForeignKeySQLiteStore(t)
				sim := newSupplySim(t, store, seed)
				// batchEnds are the slots at which a commit ended; rollbacks
				// land on them (the overlay elides outputs a batch both
				// creates and spends, so it cannot restore a slot inside
				// one) or on any block when the path commits per block.
				var boundaries []uint64
				sawScript := false
				for step := range 40 {
					if step > 4 && len(boundaries) > 0 && sim.rng.Intn(5) == 0 {
						depth := 1 + sim.rng.Intn(min(4, len(boundaries)))
						target := boundaries[len(boundaries)-depth]
						sim.rollback(target)
						boundaries = boundaries[:len(boundaries)-depth+1]
						sim.assertAggregateMatchesScan(
							fmt.Sprintf("rollback to %d", target),
						)
						continue
					}
					n := 1
					if mode != applyPerRow {
						n = 1 + sim.rng.Intn(3)
					}
					produced := []supplyInput{}
					spent := map[string]struct{}{}
					var blocks []simBlock
					for range n {
						blocks = append(
							blocks,
							sim.buildBlock(&produced, spent),
						)
					}
					sim.applyBatch(mode, blocks)
					if mode == applyPerRow {
						for _, b := range blocks {
							boundaries = append(boundaries, b.point.Slot)
						}
					} else {
						boundaries = append(boundaries, blocks[len(blocks)-1].point.Slot)
					}
					sim.assertAggregateMatchesScan(
						fmt.Sprintf("step %d", step),
					)
					if fullScanScriptLockedSupply(t, store) > 0 {
						sawScript = true
					}
				}
				require.True(
					t,
					sawScript,
					"generator never produced a live script output",
				)
			})
		}
	}
}

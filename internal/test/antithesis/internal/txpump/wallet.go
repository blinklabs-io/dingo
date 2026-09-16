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

package txpump

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

// UTxO represents an unspent transaction output.
type UTxO struct {
	TxHash      string
	Index       uint32
	Amount      uint64   // lovelace
	SigningKey  *UTxOKey // optional: Ed25519 key for signing inputs from this UTxO
	availableAt time.Time
	address     []byte
}

// ErrInsufficientFunds is returned by SelectCoins when the wallet does not
// hold enough ADA to cover the requested amount.
var ErrInsufficientFunds = errors.New("wallet: insufficient funds")

type pendingTx struct {
	inputs, outputs []UTxO
	absent          bool
}

// Wallet tracks the set of known UTxOs and provides thread-safe coin
// selection using a largest-first strategy.
type Wallet struct {
	mu        sync.Mutex
	utxos     []UTxO
	now       func() time.Time
	addresses map[string]*UTxOKey
	pending   map[string]pendingTx
}

// NewWallet returns an empty Wallet.
func NewWallet() *Wallet {
	return &Wallet{
		now:       time.Now,
		addresses: make(map[string]*UTxOKey),
		pending:   make(map[string]pendingTx),
	}
}

func utxoKey(
	u UTxO,
) string {
	return u.TxHash + ":" + strconv.FormatUint(uint64(u.Index), 10)
}

func (w *Wallet) currentTime() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

func (w *Wallet) ensureMaps() {
	if w.addresses == nil {
		w.addresses = make(map[string]*UTxOKey)
	}
	if w.pending == nil {
		w.pending = make(map[string]pendingTx)
	}
}

// AddAfter appends UTxOs that become available for coin selection after the
// supplied delay. It is used for outputs of submitted transactions so txpump
// does not immediately build an unconfirmed dependency chain.
func (w *Wallet) AddAfter(delay time.Duration, utxos ...UTxO) {
	if delay <= 0 {
		w.Add(utxos...)
		return
	}
	availableAt := w.currentTime().Add(delay)
	for i := range utxos {
		utxos[i].availableAt = availableAt
	}
	w.Add(utxos...)
}

func (w *Wallet) isAvailable(utxo UTxO, now time.Time) bool {
	return utxo.availableAt.IsZero() || !utxo.availableAt.After(now)
}

func (w *Wallet) SigningAddresses() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensureMaps()
	out := make([][]byte, 0, len(w.addresses))
	for _, k := range w.addresses {
		out = append(out, append([]byte(nil), k.Address...))
	}
	return out
}

func (w *Wallet) Reserve(
	id string,
	inputs, outputs []UTxO,
	delay time.Duration,
) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensureMaps()
	if delay > 0 {
		at := w.currentTime().Add(delay)
		for i := range outputs {
			outputs[i].availableAt = at
		}
	}
	w.pending[id] = pendingTx{
		inputs:  append([]UTxO(nil), inputs...),
		outputs: append([]UTxO(nil), outputs...),
	}
}

// RecordAccepted records an accepted transaction. Unsigned harness wallets
// retain the historical pacing-only output behavior; keyed wallets use
// reservations reconciled against LSQ.
func (w *Wallet) RecordAccepted(
	id string,
	inputs, outputs []UTxO,
	delay time.Duration,
) {
	if len(w.SigningAddresses()) == 0 {
		w.AddAfter(delay, outputs...)
		return
	}
	w.Reserve(id, inputs, outputs, delay)
}

// ReconcileSnapshot replaces spendable state with the acquired LSQ snapshot.
// LocalTxMonitor presence keeps a transaction's input reservation. Once it is
// absent, only source inputs present in the LSQ snapshot can be restored.
func (w *Wallet) ReconcileSnapshot(snapshot []UTxO, presence map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensureMaps()
	chain := make(map[string]UTxO, len(snapshot))
	prior := make(map[string]UTxO, len(w.utxos))
	for _, u := range w.utxos {
		prior[utxoKey(u)] = u
	}
	pendingOutputs := make(map[string]UTxO)
	for _, tx := range w.pending {
		for _, out := range tx.outputs {
			pendingOutputs[utxoKey(out)] = out
		}
	}
	for _, u := range snapshot {
		if old, ok := prior[utxoKey(u)]; ok {
			u.availableAt = old.availableAt
			u.SigningKey = old.SigningKey
		}
		if pending, ok := pendingOutputs[utxoKey(u)]; ok {
			u.availableAt = pending.availableAt
			if pending.SigningKey != nil {
				u.SigningKey = pending.SigningKey
			}
		}
		if k := w.addresses[string(u.address)]; k != nil {
			u.SigningKey = k
		}
		chain[utxoKey(u)] = u
	}
	reserved := make(map[string]struct{})
	for id, tx := range w.pending {
		if presence[id] {
			for _, in := range tx.inputs {
				reserved[utxoKey(in)] = struct{}{}
			}
			for _, out := range tx.outputs {
				reserved[utxoKey(out)] = struct{}{}
			}
			continue
		}
		for _, in := range tx.inputs {
			if _, ok := chain[utxoKey(in)]; ok {
				// Keep the authoritative snapshot's value and restore only
				// signing metadata held by the reservation.
				current := chain[utxoKey(in)]
				current.SigningKey = in.SigningKey
				chain[utxoKey(in)] = current
			}
		}
		if !tx.absent {
			// A node can remove a forged transaction from the mempool before
			// its committed outputs and spent inputs are visible to LSQ. Keep
			// the record through one absent observation so the next reconcile
			// can distinguish that race from a real rollback.
			tx.absent = true
			w.pending[id] = tx
			continue
		}
		delete(w.pending, id)
		// Terminal records are retired immediately; correctness lives in the
		// current snapshot and pending reservations, not an archive.
	}
	for key := range reserved {
		delete(chain, key)
	}
	w.utxos = w.utxos[:0]
	for _, u := range chain {
		w.utxos = append(w.utxos, u)
	}
}

func (w *Wallet) PendingIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.pending))
	for id := range w.pending {
		out = append(out, id)
	}
	return out
}

// Add appends one or more UTxOs to the wallet.
func (w *Wallet) Add(utxos ...UTxO) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensureMaps()
	for _, u := range utxos {
		if u.SigningKey != nil {
			w.addresses[string(u.SigningKey.Address)] = u.SigningKey
		}
	}
	w.utxos = append(w.utxos, utxos...)
}

// Balance returns the total lovelace held by the wallet.
// If the sum would overflow uint64, math.MaxUint64 is returned.
func (w *Wallet) Balance() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var total uint64
	now := w.currentTime()
	for _, u := range w.utxos {
		if !w.isAvailable(u, now) {
			continue
		}
		if u.Amount > math.MaxUint64-total {
			return math.MaxUint64
		}
		total += u.Amount
	}
	return total
}

// Len returns the number of UTxOs currently in the wallet.
func (w *Wallet) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.currentTime()
	available := 0
	for _, u := range w.utxos {
		if w.isAvailable(u, now) {
			available++
		}
	}
	return available
}

// SelectCoins selects UTxOs using a largest-first strategy to cover at least
// targetAmount lovelace. The selected UTxOs are removed from the wallet so
// they cannot be double-spent within the same session.
//
// Returns the selected UTxOs and the change amount (selected total minus
// targetAmount). Returns ErrInsufficientFunds if the wallet cannot cover the
// target.
func (w *Wallet) SelectCoins(targetAmount uint64) ([]UTxO, uint64, error) {
	if targetAmount == 0 {
		return nil, 0, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Sort descending by amount (largest first).
	now := w.currentTime()
	sorted := make([]UTxO, 0, len(w.utxos))
	for _, utxo := range w.utxos {
		if w.isAvailable(utxo, now) {
			sorted = append(sorted, utxo)
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Amount > sorted[j].Amount
	})

	var selected []UTxO
	var collected uint64
	for _, u := range sorted {
		selected = append(selected, u)
		collected += u.Amount
		if collected >= targetAmount {
			break
		}
	}

	if collected < targetAmount {
		return nil, 0, ErrInsufficientFunds
	}

	// Remove selected UTxOs from the wallet.
	selectedSet := make(map[string]struct{}, len(selected))
	for _, u := range selected {
		key := utxoKey(u)
		selectedSet[key] = struct{}{}
	}
	remaining := w.utxos[:0]
	for _, u := range w.utxos {
		key := utxoKey(u)
		if _, spent := selectedSet[key]; !spent {
			remaining = append(remaining, u)
		}
	}
	w.utxos = remaining

	change := collected - targetAmount
	return selected, change, nil
}

// ReturnUTxOs adds UTxOs back to the wallet (e.g. on submission failure).
func (w *Wallet) ReturnUTxOs(utxos []UTxO) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.utxos = append(w.utxos, utxos...)
}

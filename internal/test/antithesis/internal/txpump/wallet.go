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
)

// UTxO represents an unspent transaction output.
type UTxO struct {
	TxHash string
	Index  uint32
	Amount uint64 // lovelace
}

// ErrInsufficientFunds is returned by SelectCoins when the wallet does not
// hold enough ADA to cover the requested amount.
var ErrInsufficientFunds = errors.New("wallet: insufficient funds")

// Wallet tracks the set of known UTxOs and provides thread-safe coin
// selection using a largest-first strategy.
type Wallet struct {
	mu    sync.Mutex
	utxos []UTxO
}

// NewWallet returns an empty Wallet.
func NewWallet() *Wallet {
	return &Wallet{}
}

// Add appends one or more UTxOs to the wallet.
func (w *Wallet) Add(utxos ...UTxO) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.utxos = append(w.utxos, utxos...)
}

// Balance returns the total lovelace held by the wallet.
// If the sum would overflow uint64, math.MaxUint64 is returned.
func (w *Wallet) Balance() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var total uint64
	for _, u := range w.utxos {
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
	return len(w.utxos)
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
	sorted := make([]UTxO, len(w.utxos))
	copy(sorted, w.utxos)
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
		key := u.TxHash + ":" + strconv.FormatUint(uint64(u.Index), 10)
		selectedSet[key] = struct{}{}
	}
	remaining := w.utxos[:0]
	for _, u := range w.utxos {
		key := u.TxHash + ":" + strconv.FormatUint(uint64(u.Index), 10)
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

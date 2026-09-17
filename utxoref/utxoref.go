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

// Package utxoref provides Key, a comparable, allocation-free identifier
// for a transaction input or output reference (a producing transaction
// hash plus output index).
//
// The ledger, ledger/forging, and mempool packages each track UTxO
// overlays -- inputs consumed and outputs created by in-flight
// transactions, whether pending in the mempool, applied earlier in the
// same block, or selected earlier for the block currently being forged --
// keyed by this reference. Historically each of those call sites built the
// key with fmt.Sprintf("%s:%d", ...)/fmt.Sprintf("%x:%d", ...), allocating
// a formatted, hex-encoded string on every consulted or recorded input and
// output. Key is a plain value type (a fixed-size hash array and a
// uint32), so using it as a map key never allocates.
//
// This package has no dependency on ledger, forging, or mempool, and none
// of those packages depend on each other for it: it exists precisely so
// all three (and any future caller) can share one comparable key type
// without introducing a new inter-package coupling.
package utxoref

import (
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Key identifies a UTxO -- or, equivalently, the input that spends one --
// by its producing transaction hash and output index.
type Key struct {
	TxId  lcommon.Blake2b256
	Index uint32
}

// ForInput returns the Key identifying the UTxO a transaction input spends.
func ForInput(input lcommon.TransactionInput) Key {
	return Key{TxId: input.Id(), Index: input.Index()}
}

// ForUtxo returns the Key identifying a produced UTxO.
func ForUtxo(utxo lcommon.Utxo) Key {
	return Key{TxId: utxo.Id.Id(), Index: utxo.Id.Index()}
}

// String renders the key in the same "<hex-hash>:<index>" shape the
// formatted string keys it replaces used. It allocates and exists for
// logging and error messages only -- never call it on a hot path.
func (k Key) String() string {
	return fmt.Sprintf("%s:%d", k.TxId.String(), k.Index)
}

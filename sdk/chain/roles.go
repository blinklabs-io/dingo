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

package chain

import "context"

// Ledger is the role that owns chain state and validates against it. It
// follows the ledger the chain manager is configured with (its security
// parameter) and the validator the mempool and forger call.
type Ledger interface {
	// SecurityParam returns the number of blocks after which the chain is
	// final. It is positive for a ready ledger.
	SecurityParam() int
	// Tip returns the tip the ledger has applied.
	Tip() Tip
	// ValidateTx checks a transaction against the current ledger state.
	ValidateTx(tx Tx) error
}

// Consensus is the role that decides which chain is preferred and when this
// node may produce a block. It follows chain selection and the forger's
// leader-check and forged-block validation interfaces.
type Consensus interface {
	// Compare reports which of two tips chain selection prefers.
	Compare(a, b Tip) Comparison
	// ShouldProduceBlock reports whether this node leads the slot.
	ShouldProduceBlock(slot uint64) bool
	// NextLeaderSlot returns the first slot at or after fromSlot that this
	// node leads, and false when none is known.
	NextLeaderSlot(fromSlot uint64) (slot uint64, ok bool)
	// ValidateForgedBlock checks a locally forged block before it is adopted.
	ValidateForgedBlock(ctx context.Context, block Block) error
}

// Network is the role that moves blocks between nodes. It follows the forger's
// block broadcaster.
type Network interface {
	// Broadcast adds a block to the local chain and propagates it to peers.
	Broadcast(ctx context.Context, block Block) error
}

// Mempool is the optional role that holds transactions waiting for a block. A
// node without one accepts no transactions and forges no blocks.
type Mempool interface {
	// AddTx admits a transaction.
	AddTx(ctx context.Context, tx Tx) error
	// RemoveTx removes the transaction with the given hash, in the node's
	// hexadecimal form, if present.
	RemoveTx(txHash string)
}

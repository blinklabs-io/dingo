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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/mempool"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// MeshChain is the subset of chain.Chain needed by the Mesh server.
type MeshChain interface {
	Tip() ochainsync.Tip
}

// MeshDatabase is the subset of database.Database needed by the Mesh server.
// All calls implicitly use a nil transaction (auto-transaction per method).
type MeshDatabase interface {
	BlockByHash(hash []byte) (models.Block, error)
	BlockByIndex(idx uint64) (models.Block, error)
	GetTransactionByHash(hash []byte) (*models.Transaction, error)
	GetTransactionsByBlockHash(hash []byte) ([]models.Transaction, error)
}

// MeshLedgerState is the subset of ledger.LedgerState needed by the Mesh server.
type MeshLedgerState interface {
	GetCurrentPParams() lcommon.ProtocolParameters
	SlotToTime(slot uint64) (time.Time, error)
	UtxosByAddress(addr lcommon.Address) ([]models.Utxo, error)
}

// MeshMempool is the subset of mempool.Mempool needed by the Mesh server.
type MeshMempool interface {
	AddTransaction(txType uint, txBytes []byte) error
	GetTransaction(hash string) (mempool.MempoolTransaction, bool)
	Transactions() []mempool.MempoolTransaction
}

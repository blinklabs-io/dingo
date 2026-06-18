// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/mempool"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// UtxorpcLedgerState is the subset of ledger.LedgerState needed by the UTxO
// RPC server. Using this interface keeps the server and service handlers free
// of a direct *ledger.LedgerState dependency.
type UtxorpcLedgerState interface {
	BlockByHash(hash []byte) (models.Block, error)
	CardanoNodeConfig() *cardano.CardanoNodeConfig
	Datum(hash []byte) (*models.Datum, error)
	EvaluateTx(tx lcommon.Transaction) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error)
	GetBlock(point ocommon.Point) (models.Block, error)
	GetChainFromPointContext(ctx context.Context, point ocommon.Point, inclusive bool) (*chain.ChainIterator, error)
	GetCurrentPParams() lcommon.ProtocolParameters
	GetEpochs() ([]models.Epoch, error)
	GetIntersectPoint(points []ocommon.Point) (*ocommon.Point, error)
	GetPParamsForEpoch(epoch uint64, era eras.EraDesc) (lcommon.ProtocolParameters, error)
	SlotToTime(slot uint64) (time.Time, error)
	SystemStart() (time.Time, error)
	Tip() ochainsync.Tip
	TransactionByHash(hash []byte) (*models.Transaction, error)
	UtxoByRef(txId []byte, outputIdx uint32) (*models.Utxo, error)
	UtxosByAddressWithOrdering(q *models.UtxoWithOrderingQuery) ([]models.UtxoWithOrdering, error)
}

// UtxorpcMempool is the subset of mempool.Mempool needed by the UTxO RPC
// server.
type UtxorpcMempool interface {
	AddTransaction(txType uint, txBytes []byte) error
	Transactions() []mempool.MempoolTransaction
}

// UtxorpcEventBus is the subset of event.EventBus needed by the UTxO RPC
// server.
type UtxorpcEventBus interface {
	SubscribeFunc(eventType event.EventType, handlerFunc event.EventHandlerFunc) event.EventSubscriberId
	Unsubscribe(eventType event.EventType, subId event.EventSubscriberId)
}

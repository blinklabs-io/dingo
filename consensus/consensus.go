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

package consensus

import (
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
)

type Consensus struct {
	chainManager                     *chain.ChainManager
	eventBus                         *event.EventBus
	ledger                           *ledger.LedgerState
	logger                           *slog.Logger
	chainsyncMutex                   sync.Mutex
	chainsyncBlockEvents             []BlockfetchEvent
	chainsyncBlockfetchBusyTime      time.Time
	chainsyncBlockfetchBatchDoneChan chan struct{}
	chainsyncBlockfetchReadyChan     chan struct{}
	chainsyncBlockfetchMutex         sync.Mutex
	chainsyncBlockfetchWaiting       bool
}

func New(
	logger *slog.Logger,
	eventBus *event.EventBus,
	chainManager *chain.ChainManager,
	ledger *ledger.LedgerState,
) (*Consensus, error) {
	if logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	c := &Consensus{
		logger:       logger,
		eventBus:     eventBus,
		chainManager: chainManager,
		ledger:       ledger,
	}
	// Setup event handlers
	c.eventBus.SubscribeFunc(
		ChainsyncEventType,
		c.handleEventChainsync,
	)
	c.eventBus.SubscribeFunc(
		BlockfetchEventType,
		c.handleEventBlockfetch,
	)
	return c, nil
}

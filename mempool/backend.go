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
	"context"
	"fmt"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// Implementation identifies a mempool storage and ordering backend.
type Implementation string

const (
	// ImplementationFIFO preserves successful-admission order.
	ImplementationFIFO Implementation = "fifo"
)

// Valid reports whether the implementation name is part of the stable config
// surface.
func (i Implementation) Valid() bool {
	switch i {
	case ImplementationFIFO:
		return true
	default:
		return false
	}
}

// RelayConsumer is the backend-neutral cursor and advertised-transaction cache
// used by node-to-node TxSubmission.
type RelayConsumer interface {
	NextTx(blocking bool) *MempoolTransaction
	GetTxFromCache(hash string) *MempoolTransaction
	ClearCache()
	RemoveTxFromCache(hash string)
}

// Pool is the backend-neutral mempool contract used at the node composition
// boundary. Confirmed removals use RemoveTxsByHash; RemoveTransaction is for
// manual removal and may also remove invalid descendants.
type Pool interface {
	Implementation() Implementation
	Stop(ctx context.Context) error
	AddTransaction(txType uint, txBytes []byte) error
	GetTransaction(txHash string) (MempoolTransaction, bool)
	Transactions() []MempoolTransaction
	CapacityBytes() int64
	RemoveTransaction(txHash string)
	RemoveTxsByHash(hashes []string)
	AddConsumer(connId ouroboros.ConnectionId) RelayConsumer
	RemoveConsumer(connId ouroboros.ConnectionId)
	Consumer(connId ouroboros.ConnectionId) RelayConsumer
}

// AdmissionHeadroom is the shared capability owned by #2764. Keeping it
// separate allows that work to land without changing the Pool contract.
type AdmissionHeadroom interface {
	AdmissionHeadroomBytes() int64
	MaxAdmissionHeadroomBytes() int64
	WaitForAdmissionHeadroom(minBytes int64, done <-chan error) bool
}

// FIFO exposes the current ordered mempool explicitly as the FIFO backend. The
// embedded Mempool preserves source compatibility while production composition
// depends on Pool.
type FIFO struct {
	*Mempool
}

// NewFIFO constructs the FIFO backend.
func NewFIFO(config MempoolConfig) (*FIFO, error) {
	pool, err := NewMempool(config)
	if err != nil {
		return nil, err
	}
	return &FIFO{Mempool: pool}, nil
}

func (f *FIFO) Implementation() Implementation {
	return ImplementationFIFO
}

func (f *FIFO) AddConsumer(connId ouroboros.ConnectionId) RelayConsumer {
	consumer := f.Mempool.AddConsumer(connId)
	if consumer == nil {
		return nil
	}
	return consumer
}

func (f *FIFO) Consumer(connId ouroboros.ConnectionId) RelayConsumer {
	consumer := f.Mempool.Consumer(connId)
	if consumer == nil {
		return nil
	}
	return consumer
}

// New constructs the selected mempool implementation. An empty value selects
// FIFO for compatibility with callers that predate configurable backends.
func New(implementation Implementation, config MempoolConfig) (Pool, error) {
	if implementation == "" {
		implementation = ImplementationFIFO
	}
	switch implementation {
	case ImplementationFIFO:
		return NewFIFO(config)
	default:
		return nil, fmt.Errorf("unknown mempool implementation %q", implementation)
	}
}

var (
	_ Pool          = (*FIFO)(nil)
	_ RelayConsumer = (*MempoolConsumer)(nil)
)

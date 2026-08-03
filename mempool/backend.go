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
	// ImplementationDAG tracks transaction dependencies explicitly and exposes
	// a deterministic topological order.
	ImplementationDAG Implementation = "dag"
)

// Valid reports whether the implementation name is part of the stable config
// surface.
func (i Implementation) Valid() bool {
	switch i {
	case ImplementationFIFO, ImplementationDAG:
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

// AdmissionHeadroom is an optional capability used by non-evicting backends to
// pause network intake before requesting transaction bodies that cannot fit.
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
	pool, err := newMempool(config, ImplementationFIFO)
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

// DAG exposes the dependency-indexed mempool backend.
type DAG struct {
	*Mempool
}

// NewDAG constructs the DAG backend.
func NewDAG(config MempoolConfig) (*DAG, error) {
	pool, err := newMempool(config, ImplementationDAG)
	if err != nil {
		return nil, err
	}
	return &DAG{Mempool: pool}, nil
}

func (d *DAG) Implementation() Implementation {
	return ImplementationDAG
}

func (d *DAG) AddConsumer(connId ouroboros.ConnectionId) RelayConsumer {
	consumer := d.Mempool.AddConsumer(connId)
	if consumer == nil {
		return nil
	}
	return consumer
}

func (d *DAG) Consumer(connId ouroboros.ConnectionId) RelayConsumer {
	consumer := d.Mempool.Consumer(connId)
	if consumer == nil {
		return nil
	}
	return consumer
}

// AdmissionHeadroomBytes returns the bytes currently available before DAG
// admission reaches its rejection watermark.
func (d *DAG) AdmissionHeadroomBytes() int64 {
	d.RLock()
	defer d.RUnlock()
	return d.admissionHeadroomBytesLocked()
}

// MaxAdmissionHeadroomBytes returns the maximum DAG admission budget.
func (d *DAG) MaxAdmissionHeadroomBytes() int64 {
	return d.admissionLimitBytes()
}

// WaitForAdmissionHeadroom blocks network intake until the requested admission
// budget is available or either the connection or mempool stops.
func (d *DAG) WaitForAdmissionHeadroom(
	minBytes int64,
	done <-chan error,
) bool {
	if minBytes < 0 || minBytes > d.MaxAdmissionHeadroomBytes() {
		return false
	}
	for {
		d.RLock()
		if d.stopped {
			d.RUnlock()
			return false
		}
		if d.admissionHeadroomBytesLocked() >= minBytes {
			d.RUnlock()
			return true
		}
		changed := d.headroomChanged
		d.RUnlock()

		select {
		case <-changed:
		case <-done:
			return false
		case <-d.done:
			return false
		}
	}
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
	case ImplementationDAG:
		return NewDAG(config)
	default:
		return nil, fmt.Errorf(
			"unknown mempool implementation %q",
			implementation,
		)
	}
}

var (
	_ Pool              = (*FIFO)(nil)
	_ Pool              = (*DAG)(nil)
	_ Service           = (*FIFO)(nil)
	_ Service           = (*DAG)(nil)
	_ AdmissionHeadroom = (*DAG)(nil)
	_ RelayConsumer     = (*MempoolConsumer)(nil)
)

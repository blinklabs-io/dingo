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

package dingo

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// rebuildableRegisterer wraps a prometheus.Registerer and records every
// collector registered through it, so a live database restore/truncate
// (node_lifecycle.go) can unregister them all before the components it
// rebuilds (database, chainManager, ledgerState, mempool, chainsyncState,
// connManager, peerGov, snapshotMgr, the block producer, ...) re-register
// fresh collectors under the same metric names. Without this, the second
// database.New/chain.NewManager/... call against a node's configured
// registry panics with "duplicate metrics collector registration attempted"
// whenever a real (non-nil) registry is configured — every affected
// package's Register method is a no-op on a nil registry, so this only
// surfaces with metrics actually enabled.
//
// Metrics that are registered once and never rebuilt (dingo_build_info,
// the RTS gauges, the EventBus's) are registered directly against the
// pre-wrap registerer in New(), before this wrapper is installed, so
// unregisterAll never touches them.
type rebuildableRegisterer struct {
	inner prometheus.Registerer

	mu         sync.Mutex
	collectors []prometheus.Collector
}

func newRebuildableRegisterer(inner prometheus.Registerer) *rebuildableRegisterer {
	return &rebuildableRegisterer{inner: inner}
}

func (r *rebuildableRegisterer) Register(c prometheus.Collector) error {
	if err := r.inner.Register(c); err != nil {
		return err
	}
	r.mu.Lock()
	r.collectors = append(r.collectors, c)
	r.mu.Unlock()
	return nil
}

func (r *rebuildableRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := r.Register(c); err != nil {
			panic(err)
		}
	}
}

func (r *rebuildableRegisterer) Unregister(c prometheus.Collector) bool {
	return r.inner.Unregister(c)
}

// unregisterAll removes every collector registered through this wrapper
// from the underlying registry, so the next live rebuild can register
// fresh collectors under the same names without a duplicate-registration
// panic. Safe to call on a nil receiver, matching the nil-guard pattern
// node_lifecycle.go already uses for other optional lifecycle fields.
func (r *rebuildableRegisterer) unregisterAll() {
	if r == nil {
		return
	}
	r.mu.Lock()
	collectors := r.collectors
	r.collectors = nil
	r.mu.Unlock()
	for _, c := range collectors {
		r.inner.Unregister(c)
	}
}

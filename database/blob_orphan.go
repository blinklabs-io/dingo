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

package database

import (
	"errors"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// ErrBlobDeleteIncomplete reports that some blob objects could not be deleted.
//
// Blob deletion is supplementary -- metadata is the source of truth -- so the
// callers deliberately continue and remove the metadata anyway: a rolled-back
// UTxO must not stay in the live set just because its blob is stuck. What that
// leaves behind is an object nothing can name again, since the row that
// pointed at it is gone. Callers therefore log and count the condition rather
// than aborting on it, which is what separates a documented, observable
// outcome from a silent one.
var ErrBlobDeleteIncomplete = errors.New(
	"blob delete incomplete: unreachable objects retained",
)

// blobOrphans counts blob objects whose delete failed before their
// authoritative metadata was removed. It is process-wide so every registry
// observes the same total, matching the block-hash index counters.
var blobOrphans atomic.Uint64

// BlobOrphanCount returns the cumulative number of blob objects left
// unreachable by a failed delete.
func BlobOrphanCount() uint64 {
	return blobOrphans.Load()
}

// recordBlobOrphans adds n to the unreachable-object counter.
func recordBlobOrphans(n int) {
	if n <= 0 {
		return
	}
	blobOrphans.Add(uint64(n)) //nolint:gosec // n is guarded positive above
}

// recordBlobOrphansOnCommit adds n to the unreachable-object counter once the
// transaction that removes the naming metadata has committed durably.
//
// The blob delete happens before that metadata removal, so counting at delete
// time would count objects a rollback leaves perfectly reachable, and count
// them again on the retry. A nil txn has no commit to wait for -- there is no
// pending metadata removal to gate on -- so it counts immediately.
func recordBlobOrphansOnCommit(txn *Txn, n int) {
	if n <= 0 {
		return
	}
	if txn == nil {
		recordBlobOrphans(n)
		return
	}
	txn.AfterCommit(func() { recordBlobOrphans(n) })
}

// RegisterBlobOrphanMetrics exposes the unreachable-object counter on the
// given Prometheus registry.
//
// There is no sweep that reclaims these objects, so this counter is the only
// signal that a blob store is accumulating dead data. reg.Register is used
// instead of promauto so a registration conflict never panics during
// Database.New; an AlreadyRegisteredError is ignored and any other error is
// returned.
func RegisterBlobOrphanMetrics(reg prometheus.Registerer) error {
	if reg == nil {
		return nil
	}
	collector := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "dingo_database_blob_orphans_total",
		Help: "Blob objects whose delete failed before their authoritative " +
			"metadata was removed, leaving them unreachable",
	}, func() float64 {
		return float64(blobOrphans.Load())
	})
	err := reg.Register(collector)
	if err == nil {
		return nil
	}
	// A reused registry already exposes this counter, and both collectors
	// read the same process-wide atomic, so the duplicate is safe to ignore.
	if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
		return err
	}
	return nil
}

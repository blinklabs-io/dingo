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

package sqlite

import (
	"errors"
	"os"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/prometheus/client_golang/prometheus"
)

const sqliteFileMetricNamePrefix = "dingo_database_sql_"

// registerSQLiteFileMetrics exposes two gauges a Prometheus scrape can pull
// on its own schedule, complementing dingo_database_sql_operations_total
// (database/plugin/metadata/sqlstore/metrics.go, which counts statements as
// they happen): how large the WAL file on disk currently is, and the
// store's total on-disk footprint. Both are sampled live at scrape time --
// a plain os.Stat for the WAL file, and store.DiskSize()'s existing
// PRAGMA page_count/page_size read for the total -- rather than tracked by
// a background ticker, the same pull-based pattern
// database/plugin/blob/badger/metrics.go already uses for its block/index
// cache gauges (reading db.BlockCacheMetrics() live in a GaugeFunc
// callback). registerSQLiteFileMetrics is called only for an on-disk store
// (dataDir != "") with a non-nil PromRegistry; see openSQLStore.
//
// The WAL gauge reports the -wal file's current size, not whether
// checkpointing is keeping up: SQLite's PASSIVE, FULL, and RESTART
// checkpoint modes (including the automatic checkpoint
// wal_autocheckpoint(10000) triggers -- see sqliteCommonPragmas in
// shared_sqlstore.go) all backfill WAL frames into metadata.sqlite but
// never ftruncate the -wal file itself, so this gauge only ever grows and
// is reset by a TRUNCATE-mode checkpoint, which dingo does not run
// periodically. Measured live against a sustained write workload, it held
// steady at 42007552 bytes across PASSIVE/FULL/RESTART checkpoints alike,
// with busy=0 and checkpointed==log (fully checkpointed) each time, and
// only returned to 0 after an explicit TRUNCATE checkpoint. A healthy store
// and one whose checkpoints have stalled read identically here once the WAL
// has grown once; use it to see the on-disk WAL footprint, not checkpoint
// health. Raising wal_autocheckpoint to 10000 also raises this gauge's
// permanent steady-state floor from SQLite's old ~4MB (1000-page default)
// to ~40MB: that WAL size is now a normal, permanent part of the on-disk
// footprint, not a transient backlog.
func registerSQLiteFileMetrics(
	reg prometheus.Registerer,
	databasePath string,
	store *sqlstore.Store,
) {
	walPath := databasePath + "-wal"
	safeRegisterGaugeFunc(
		reg,
		sqliteFileMetricNamePrefix+"wal_bytes",
		"Current size in bytes of the SQLite WAL file "+
			"(metadata.sqlite-wal). Sampled live from the filesystem on "+
			"each scrape. This is a monotonic high-water mark, not a "+
			"checkpoint-health signal: PASSIVE/FULL/RESTART checkpoints "+
			"never shrink the file, only a TRUNCATE checkpoint does, so a "+
			"steady non-zero value (permanently ~40MB at the configured "+
			"wal_autocheckpoint threshold) is expected, not a backlog.",
		func() float64 {
			info, err := os.Stat(walPath)
			if err != nil {
				return 0
			}
			return float64(info.Size())
		},
	)
	safeRegisterGaugeFunc(
		reg,
		sqliteFileMetricNamePrefix+"disk_bytes",
		"Total on-disk size of the SQLite metadata store: "+
			"max(page_count*page_size, main file size) plus the WAL and "+
			"shared-memory files, per Store.DiskSize.",
		func() float64 {
			size, err := store.DiskSize()
			if err != nil {
				return 0
			}
			return float64(size)
		},
	)
}

// safeRegisterGaugeFunc registers a GaugeFunc under name, replacing whatever
// collector a prior registration installed there instead of silently keeping
// it. Unlike newSQLOperationsCounter's counter (a stateless label schema that
// is genuinely safe to share across two Store instances on the same
// registry, as some test harnesses do) or a plain gauge with no captured
// state, this GaugeFunc's fn closes over one specific store (walPath, or the
// *sqlstore.Store passed to registerSQLiteFileMetrics): keeping the first
// registration on a duplicate-registration error would leave every later
// scrape reading the first, possibly since-closed, store forever, silently
// going stale (or reporting 0) once that store closes while a replacement
// store on the same registry keeps running. Unregistering the stale
// collector and installing the new one keeps the gauge reading whichever
// store most recently called this.
func safeRegisterGaugeFunc(
	reg prometheus.Registerer,
	name, help string,
	fn func() float64,
) {
	gauge := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{Name: name, Help: help},
		fn,
	)
	if err := reg.Register(gauge); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			panic(err)
		}
		if !reg.Unregister(already.ExistingCollector) {
			panic(
				"safeRegisterGaugeFunc: failed to unregister existing collector for " + name,
			)
		}
		if err := reg.Register(gauge); err != nil {
			panic(err)
		}
	}
}

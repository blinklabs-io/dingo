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
// The WAL gauge is the direct, on-disk complement to
// sqliteCommonPragmas' wal_autocheckpoint fix: a WAL that keeps growing well
// past 10000 pages (~40MB) between samples means checkpoints are falling
// behind the write rate, not just running less often than before.
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
			"each scrape.",
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

// safeRegisterGaugeFunc registers a GaugeFunc and silently keeps whatever is
// already registered under name on a duplicate registration, the same
// accommodation sqlstore's newSQLOperationsCounter and
// database/plugin/blob/badger's registerBlobMetrics make for a registry
// shared by more than one store (as some test harnesses use). A gauge, not a
// counter, has no cumulative state to lose by falling back to the existing
// collector instead of panicking.
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
	}
}

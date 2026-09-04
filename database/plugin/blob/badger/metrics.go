// Copyright 2024 Blink Labs Software
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

package badger

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type badgerGCMetrics struct {
	attempts       prometheus.Counter
	successes      prometheus.Counter
	noRewrite      prometheus.Counter
	errors         prometheus.Counter
	duration       prometheus.Observer
	lsmBytes       prometheus.Gauge
	vlogBytes      prometheus.Gauge
	reclaimedBytes prometheus.Gauge
	consecutive    prometheus.Gauge
	lastSuccess    prometheus.Gauge
}

type badgerGCMetricCollectors struct {
	attempts       *prometheus.CounterVec
	successes      *prometheus.CounterVec
	noRewrite      *prometheus.CounterVec
	errors         *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	lsmBytes       *prometheus.GaugeVec
	vlogBytes      *prometheus.GaugeVec
	reclaimedBytes *prometheus.GaugeVec
	consecutive    *prometheus.GaugeVec
	lastSuccess    *prometheus.GaugeVec
}

var (
	badgerGCCollectors sync.Map
	nextBadgerStoreID  atomic.Uint64
)

const (
	badgerMetricNamePrefix = "database_blob_"
)

func safeRegister(reg prometheus.Registerer, c prometheus.Collector) {
	if err := reg.Register(c); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			panic(err)
		}
	}
}

func (d *BlobStoreBadger) registerBlobMetrics() {
	storeID := fmt.Sprintf("store-%d", nextBadgerStoreID.Add(1))
	labels := []string{"store"}
	gcCollectors := &badgerGCMetricCollectors{}
	if existing, ok := badgerGCCollectors.Load(d.promRegistry); ok {
		gcCollectors = existing.(*badgerGCMetricCollectors)
	} else {
		gcCollectors.attempts = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: badgerMetricNamePrefix + "gc_attempts_total", Help: "Total Badger value-log GC attempts.",
		}, labels)
		gcCollectors.successes = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: badgerMetricNamePrefix + "gc_successes_total", Help: "Total successful Badger value-log GC rewrites.",
		}, labels)
		gcCollectors.noRewrite = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: badgerMetricNamePrefix + "gc_no_rewrite_total", Help: "Total Badger value-log GC attempts with no rewrite.",
		}, labels)
		gcCollectors.errors = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: badgerMetricNamePrefix + "gc_errors_total", Help: "Total Badger value-log GC errors.",
		}, labels)
		gcCollectors.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: badgerMetricNamePrefix + "gc_duration_seconds", Help: "Duration of Badger value-log GC attempts in seconds.",
		}, labels)
		gcCollectors.lsmBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "gc_lsm_bytes", Help: "Badger LSM size after the last successful value-log GC rewrite.",
		}, labels)
		gcCollectors.vlogBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "gc_vlog_bytes", Help: "Badger value-log size after the last successful GC rewrite.",
		}, labels)
		gcCollectors.reclaimedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "gc_reclaimed_bytes", Help: "Bytes reclaimed by the last successful Badger GC rewrite.",
		}, labels)
		gcCollectors.consecutive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "gc_consecutive_successes", Help: "Successful value-log GC rewrites in the current GC cycle.",
		}, labels)
		gcCollectors.lastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "gc_last_success_timestamp_seconds", Help: "Unix timestamp of the last successful value-log GC rewrite.",
		}, labels)
		actual, loaded := badgerGCCollectors.LoadOrStore(d.promRegistry, gcCollectors)
		if loaded {
			gcCollectors = actual.(*badgerGCMetricCollectors)
		} else {
			for _, c := range []prometheus.Collector{
				gcCollectors.attempts, gcCollectors.successes, gcCollectors.noRewrite,
				gcCollectors.errors, gcCollectors.duration, gcCollectors.lsmBytes,
				gcCollectors.vlogBytes, gcCollectors.reclaimedBytes, gcCollectors.consecutive,
				gcCollectors.lastSuccess,
			} {
				safeRegister(d.promRegistry, c)
			}
		}
	}
	d.gcMetrics = &badgerGCMetrics{
		attempts:       gcCollectors.attempts.WithLabelValues(storeID),
		successes:      gcCollectors.successes.WithLabelValues(storeID),
		noRewrite:      gcCollectors.noRewrite.WithLabelValues(storeID),
		errors:         gcCollectors.errors.WithLabelValues(storeID),
		duration:       gcCollectors.duration.WithLabelValues(storeID),
		lsmBytes:       gcCollectors.lsmBytes.WithLabelValues(storeID),
		vlogBytes:      gcCollectors.vlogBytes.WithLabelValues(storeID),
		reclaimedBytes: gcCollectors.reclaimedBytes.WithLabelValues(storeID),
		consecutive:    gcCollectors.consecutive.WithLabelValues(storeID),
		lastSuccess:    gcCollectors.lastSuccess.WithLabelValues(storeID),
	}

	// Badger exposes metrics via expvar, so we need to set up some translation
	collector := collectors.NewExpvarCollector(
		map[string]*prometheus.Desc{
			// This list of metrics is derived from the metrics defined here:
			// https://github.com/dgraph-io/badger/blob/v4.2.0/y/metrics.go#L78-L107
			"badger_read_num_vlog": prometheus.NewDesc(
				badgerMetricNamePrefix+"read_num_vlog", "", nil, nil,
			),
			"badger_read_bytes_vlog": prometheus.NewDesc(
				badgerMetricNamePrefix+"read_bytes_vlog", "", nil, nil,
			),
			"badger_write_num_vlog": prometheus.NewDesc(
				badgerMetricNamePrefix+"write_num_vlog", "", nil, nil,
			),
			"badger_write_bytes_vlog": prometheus.NewDesc(
				badgerMetricNamePrefix+"write_bytes_vlog", "", nil, nil,
			),
			"badger_read_bytes_lsm": prometheus.NewDesc(
				badgerMetricNamePrefix+"read_bytes_lsm", "", nil, nil,
			),
			"badger_write_bytes_l0": prometheus.NewDesc(
				badgerMetricNamePrefix+"write_bytes_l0", "", nil, nil,
			),
			"badger_write_bytes_compaction": prometheus.NewDesc(
				badgerMetricNamePrefix+"write_bytes_compaction", "", nil, nil,
			),
			"badger_get_num_lsm": prometheus.NewDesc(
				badgerMetricNamePrefix+"get_num_lsm", "", nil, nil,
			),
			"badger_hit_num_lsm_bloom_filter": prometheus.NewDesc(
				badgerMetricNamePrefix+"hit_num_lsm_bloom_filter", "", nil, nil,
			),
			"badger_get_num_memtable": prometheus.NewDesc(
				badgerMetricNamePrefix+"get_num_memtable", "", nil, nil,
			),
			"badger_get_num_user": prometheus.NewDesc(
				badgerMetricNamePrefix+"get_num_user", "", nil, nil,
			),
			"badger_put_num_user": prometheus.NewDesc(
				badgerMetricNamePrefix+"put_num_user", "", nil, nil,
			),
			"badger_write_bytes_user": prometheus.NewDesc(
				badgerMetricNamePrefix+"write_bytes_user", "", nil, nil,
			),
			"badger_get_with_result_num_user": prometheus.NewDesc(
				badgerMetricNamePrefix+"get_with_result_num_user", "", nil, nil,
			),
			"badger_iterator_num_user": prometheus.NewDesc(
				badgerMetricNamePrefix+"iterator_num_user", "", nil, nil,
			),
			"badger_size_bytes_lsm": prometheus.NewDesc(
				badgerMetricNamePrefix+"size_bytes_lsm", "", nil, nil,
			),
			"badger_size_bytes_vlog": prometheus.NewDesc(
				badgerMetricNamePrefix+"size_bytes_vlog", "", nil, nil,
			),
			"badger_write_pending_num_memtable": prometheus.NewDesc(
				badgerMetricNamePrefix+"write_pending_num_memtable",
				"",
				nil,
				nil,
			),
			"badger_compaction_current_num_lsm": prometheus.NewDesc(
				badgerMetricNamePrefix+"compaction_current_num_lsm",
				"",
				nil,
				nil,
			),
		},
	)
	safeRegister(d.promRegistry, collector)

	// Ristretto block/index cache metrics from Badger's DB handle
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "block_cache_hits_total",
			Help: "Total block cache hits",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.BlockCacheMetrics(); m != nil {
				return float64(m.Hits())
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "block_cache_misses_total",
			Help: "Total block cache misses",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.BlockCacheMetrics(); m != nil {
				return float64(m.Misses())
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "block_cache_hit_ratio",
			Help: "Block cache hit ratio (0.0-1.0)",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.BlockCacheMetrics(); m != nil {
				return m.Ratio()
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "block_cache_cost_bytes",
			Help: "Current block cache cost in bytes (added - evicted)",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.BlockCacheMetrics(); m != nil {
				added := m.CostAdded()
				evicted := m.CostEvicted()
				if added >= evicted {
					return float64(added - evicted)
				}
				return 0
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "block_cache_keys_added_total",
			Help: "Total keys added to block cache",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.BlockCacheMetrics(); m != nil {
				return float64(m.KeysAdded())
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "block_cache_keys_evicted_total",
			Help: "Total keys evicted from block cache",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.BlockCacheMetrics(); m != nil {
				return float64(m.KeysEvicted())
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "index_cache_hits_total",
			Help: "Total index cache hits",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.IndexCacheMetrics(); m != nil {
				return float64(m.Hits())
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "index_cache_misses_total",
			Help: "Total index cache misses",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.IndexCacheMetrics(); m != nil {
				return float64(m.Misses())
				}
			}
			return 0
		},
	))
	safeRegister(d.promRegistry, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: badgerMetricNamePrefix + "index_cache_hit_ratio",
			Help: "Index cache hit ratio (0.0-1.0)",
		},
		func() float64 {
			if db := d.DB(); db != nil {
				if m := db.IndexCacheMetrics(); m != nil {
				return m.Ratio()
				}
			}
			return 0
		},
	))
}

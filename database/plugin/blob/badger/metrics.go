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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	badgerMetricNamePrefix = "database_blob_"
)

// safeRegister registers a collector, silently ignoring duplicates.
func safeRegister(reg prometheus.Registerer, c prometheus.Collector) {
	if err := reg.Register(c); err != nil {
		// Ignore AlreadyRegisteredError — a second blob store instance
		// (e.g. chain-manager store) shares the same default registry.
		var are prometheus.AlreadyRegisteredError
		if !errors.As(err, &are) {
			panic(err)
		}
	}
}

func (d *BlobStoreBadger) registerBlobMetrics() {
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
			if m := d.DB().BlockCacheMetrics(); m != nil {
				return float64(m.Hits())
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
			if m := d.DB().BlockCacheMetrics(); m != nil {
				return float64(m.Misses())
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
			if m := d.DB().BlockCacheMetrics(); m != nil {
				return m.Ratio()
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
			if m := d.DB().BlockCacheMetrics(); m != nil {
				added := m.CostAdded()
				evicted := m.CostEvicted()
				if added >= evicted {
					return float64(added - evicted)
				}
				return 0
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
			if m := d.DB().BlockCacheMetrics(); m != nil {
				return float64(m.KeysAdded())
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
			if m := d.DB().BlockCacheMetrics(); m != nil {
				return float64(m.KeysEvicted())
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
			if m := d.DB().IndexCacheMetrics(); m != nil {
				return float64(m.Hits())
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
			if m := d.DB().IndexCacheMetrics(); m != nil {
				return float64(m.Misses())
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
			if m := d.DB().IndexCacheMetrics(); m != nil {
				return m.Ratio()
			}
			return 0
		},
	))
}

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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	badgerMetricNamePrefix = "database_blob_"
)

func (d *Database) registerBlobMetrics() {
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
	prometheus.MustRegister(collector)
}

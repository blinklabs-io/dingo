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

	"github.com/prometheus/client_golang/prometheus"
)

type extraDiskSize struct {
	gauge  prometheus.Gauge
	source func() (int64, error)
}

// RegisterDiskSizeSource reports source's figure as
// dingo_database_size_bytes{store="<store>"}, sampled with the blob and
// metadata stores' sizes. It does nothing without a Prometheus registry, and
// registering a store again replaces its source.
func (d *Database) RegisterDiskSizeSource(
	store string,
	source func() (int64, error),
) {
	reg := d.config.PromRegistry
	if reg == nil || source == nil {
		return
	}
	d.extraDiskMu.Lock()
	defer d.extraDiskMu.Unlock()
	if cur, ok := d.extraDiskSizes[store]; ok {
		cur.source = source
		return
	}
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "dingo_database_size_bytes",
		Help:        "on-disk size of the database in bytes",
		ConstLabels: prometheus.Labels{"store": store},
	})
	if err := reg.Register(gauge); err != nil {
		are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err)
		if !ok {
			return
		}
		gauge, ok = are.ExistingCollector.(prometheus.Gauge)
		if !ok {
			return
		}
	}
	if d.extraDiskSizes == nil {
		d.extraDiskSizes = map[string]*extraDiskSize{}
	}
	d.extraDiskSizes[store] = &extraDiskSize{gauge: gauge, source: source}
}

// sampleExtraDiskSizes refreshes the gauges RegisterDiskSizeSource added.
func (d *Database) sampleExtraDiskSizes() {
	type sample struct {
		gauge  prometheus.Gauge
		source func() (int64, error)
	}
	d.extraDiskMu.Lock()
	samples := make([]sample, 0, len(d.extraDiskSizes))
	for _, e := range d.extraDiskSizes {
		samples = append(samples, sample{e.gauge, e.source})
	}
	d.extraDiskMu.Unlock()
	for _, e := range samples {
		if size, err := e.source(); err == nil {
			e.gauge.Set(float64(size))
		}
	}
}

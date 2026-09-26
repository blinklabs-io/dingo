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

package chain

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	blockCacheMetricName              = "dingo_chain_manager_cached_blocks"
	rollbackPointNotOnChainMetricName = "dingo_chain_rollback_point_not_on_chain_total"
)

func registerPrometheusMetric[T interface {
	prometheus.Collector
	prometheus.Metric
}](registry prometheus.Registerer, metric T) (T, error) {
	if err := registry.Register(metric); err == nil {
		return metric, nil
	} else {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			var zero T
			return zero, fmt.Errorf("%s: %w", metric.Desc(), err)
		}

		existing, ok := alreadyRegistered.ExistingCollector.(T)
		if !ok {
			var zero T
			return zero, fmt.Errorf(
				"registered metric %s has type %T, want %T",
				metric.Desc(),
				alreadyRegistered.ExistingCollector,
				metric,
			)
		}
		existingKind, err := metricKind(existing)
		if err != nil {
			var zero T
			return zero, fmt.Errorf("inspect registered metric %s: %w", metric.Desc(), err)
		}
		newKind, err := metricKind(metric)
		if err != nil {
			var zero T
			return zero, fmt.Errorf("inspect metric %s: %w", metric.Desc(), err)
		}
		if existingKind != newKind {
			var zero T
			return zero, fmt.Errorf(
				"registered metric %s has kind %s, want %s",
				metric.Desc(),
				existingKind,
				newKind,
			)
		}
		if existing.Desc().String() != metric.Desc().String() {
			var zero T
			return zero, fmt.Errorf(
				"registered metric descriptor mismatch: got %s, want %s",
				existing.Desc(),
				metric.Desc(),
			)
		}
		return existing, nil
	}
}

func metricKind(metric prometheus.Metric) (string, error) {
	value := &dto.Metric{}
	if err := metric.Write(value); err != nil {
		return "", err
	}
	switch {
	case value.GetCounter() != nil:
		return "counter", nil
	case value.GetGauge() != nil:
		return "gauge", nil
	case value.GetHistogram() != nil:
		return "histogram", nil
	case value.GetSummary() != nil:
		return "summary", nil
	case value.GetUntyped() != nil:
		return "untyped", nil
	default:
		return "unset", nil
	}
}

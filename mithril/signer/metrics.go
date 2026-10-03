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

package signer

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	rounds     prometheus.Counter
	signatures prometheus.Counter
	errors     prometheus.Counter
}

// newMetrics registers the signer's counters with reg, which may be nil.
func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		rounds: registerCounter(reg, prometheus.CounterOpts{
			Name: "dingo_mithril_signer_rounds_total",
			Help: "Signer rounds attempted.",
		}),
		signatures: registerCounter(reg, prometheus.CounterOpts{
			Name: "dingo_mithril_signer_signatures_submitted_total",
			Help: "Individual signatures accepted by the aggregator.",
		}),
		errors: registerCounter(reg, prometheus.CounterOpts{
			Name: "dingo_mithril_signer_errors_total",
			Help: "Signer rounds that failed.",
		}),
	}
}

func registerCounter(
	reg prometheus.Registerer,
	opts prometheus.CounterOpts,
) prometheus.Counter {
	counter := prometheus.NewCounter(opts)
	if reg == nil {
		return counter
	}
	if err := reg.Register(counter); err != nil {
		if alreadyRegistered, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			existing, ok := alreadyRegistered.ExistingCollector.(prometheus.Counter)
			if ok {
				return existing
			}
		}
		panic(err)
	}
	return counter
}

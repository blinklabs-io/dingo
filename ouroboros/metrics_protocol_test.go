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

package ouroboros

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecordProtocolMessage_LabelsByOutcome(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := NewOuroboros(OuroborosConfig{PromRegistry: reg})

	o.recordProtocolMessage("chainsync", nil, time.Millisecond)
	o.recordProtocolMessage("chainsync", nil, time.Millisecond)
	o.recordProtocolMessage("chainsync", errors.New("boom"), time.Millisecond)
	o.recordProtocolMessage("blockfetch", nil, time.Millisecond)

	assert.Equal(
		t,
		float64(2),
		testutil.ToFloat64(
			o.protocolMetrics.messagesReceived.WithLabelValues(
				"chainsync", "success",
			),
		),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			o.protocolMetrics.messagesReceived.WithLabelValues(
				"chainsync", "error",
			),
		),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			o.protocolMetrics.messagesReceived.WithLabelValues(
				"blockfetch", "success",
			),
		),
	)
	// Histogram should have observed three series: chainsync/success,
	// chainsync/error, blockfetch/success.
	assert.Equal(
		t,
		3,
		testutil.CollectAndCount(o.protocolMetrics.messageDuration),
	)
}

func TestRecordProtocolMessage_NoopWhenMetricsDisabled(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{}) // no PromRegistry → no metrics
	// Must not panic when metrics are uninitialized.
	o.recordProtocolMessage("chainsync", nil, time.Millisecond)
	o.recordProtocolMessage("chainsync", errors.New("x"), time.Millisecond)
}

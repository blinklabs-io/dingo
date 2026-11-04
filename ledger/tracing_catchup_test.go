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

package ledger

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestCatchupSessionSpanPerEpoch(t *testing.T) {
	spans := testutil.RecordSpans(t)
	ls := &LedgerState{}

	ls.beginCatchupTrace(context.Background())
	for _, epoch := range []uint64{4, 5} {
		_, span := tracing.Start(
			ls.catchupContext(context.Background()),
			"ledger.epoch_transition",
			tracing.Uint64("epoch", epoch),
		)
		span.End()
	}
	ls.endCatchupTrace()
	ls.endCatchupTrace()
	_, after := tracing.Start(
		ls.catchupContext(context.Background()),
		"ledger.epoch_transition",
	)
	after.End()

	var session string
	for _, span := range spans.Ended() {
		if span.Name() == "ledger.catchup" {
			require.Empty(t, session, "one catch-up session span")
			session = span.SpanContext().SpanID().String()
		}
	}
	require.NotEmpty(t, session)
	attached := map[int64]bool{}
	for _, span := range spans.Ended() {
		if span.Name() != "ledger.epoch_transition" {
			continue
		}
		isChild := span.Parent().SpanID().String() == session
		epoch := int64(-1)
		for _, kv := range span.Attributes() {
			if kv.Key == "epoch" {
				epoch = kv.Value.AsInt64()
			}
		}
		if epoch < 0 {
			require.False(t, isChild, "span after the session ended")
			continue
		}
		attached[epoch] = isChild
	}
	require.Equal(t, map[int64]bool{4: true, 5: true}, attached)
}

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

package forging

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestForgedBlockSpanInBlockTrace(t *testing.T) {
	raw := testutil.BuildDecodableConwayBlockBytes(t, 42, 7)
	block, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, raw)
	require.NoError(t, err)
	spans := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{
		SampleRatio:       0,
		AlwaysSamplePools: [][28]byte{[28]byte(tracing.IssuerKeyHash(block))},
	})
	start := time.Unix(2_000, 0)

	traceForgedBlock(context.Background(), block, 42, start)

	ended := spans.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, "block.forge", ended[0].Name())
	require.Equal(t, start, ended[0].StartTime())
	require.Equal(
		t,
		tracing.BlockTraceID(block.Hash().Bytes()),
		ended[0].SpanContext().TraceID(),
	)
}

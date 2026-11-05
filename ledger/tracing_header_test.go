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
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestHeaderValidatedSpanSharesTrace(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis"))
	ls, _ := newByronGenesisAnchorTestLedger(t, genesisHash.String())
	header := &byron.ByronMainBlockHeader{PrevBlock: genesisHash}

	_ = ls.ValidateBlockHeaderCrypto(header)

	var found bool
	for _, span := range recorder.Ended() {
		if span.Name() != "block.header_validated" {
			continue
		}
		found = true
		require.Equal(
			t,
			tracing.BlockTraceID(header.Hash().Bytes()),
			span.SpanContext().TraceID(),
		)
	}
	require.True(t, found)
}

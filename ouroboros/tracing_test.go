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
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// onlySpan returns the attributes of the single ended span, after checking
// its name and that it did not record a failure.
func onlySpan(
	t *testing.T,
	recorder *tracetest.SpanRecorder,
	name string,
) map[attribute.Key]attribute.Value {
	t.Helper()
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, name, spans[0].Name())
	require.Equal(t, codes.Unset, spans[0].Status().Code)
	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range spans[0].Attributes() {
		attrs[kv.Key] = kv.Value
	}
	return attrs
}

func newTracingTestOuroboros(t *testing.T) *Ouroboros {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, _ = bus.Subscribe(ledger.ChainsyncEventType)
	return newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestChainsyncClientRollForwardRecordsSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	o := newTracingTestOuroboros(t)
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID},
		0,
		header,
		tip,
	))

	attrs := onlySpan(t, recorder, "chainsync.roll_forward")
	require.Equal(t, connID.String(), attrs["connection.id"].AsString())
	require.Equal(t, int64(100), attrs["block.slot"].AsInt64())
	require.Equal(
		t,
		hex.EncodeToString(header.Hash().Bytes()),
		attrs["block.hash"].AsString(),
	)
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestChainsyncClientRollBackwardRecordsSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	o := newTracingTestOuroboros(t)
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	point := ocommon.NewPoint(90, []byte{0xbb})

	require.NoError(t, o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: connID},
		point,
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte{0xaa})},
	))

	attrs := onlySpan(t, recorder, "chainsync.roll_backward")
	require.Equal(t, connID.String(), attrs["connection.id"].AsString())
	require.Equal(t, int64(90), attrs["block.slot"].AsInt64())
	require.Equal(t, "bb", attrs["block.hash"].AsString())
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestBlockfetchClientBlockRawRecordsSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	o := newTracingTestOuroboros(t)
	connID := testConnId()
	raw := testutil.BuildDecodableConwayBlockBytes(t, 42, 7)
	block, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, raw)
	require.NoError(t, err)

	require.NoError(t, o.blockfetchClientBlockRaw(
		blockfetch.CallbackContext{ConnectionId: connID},
		gledger.BlockTypeConway,
		raw,
	))

	attrs := onlySpan(t, recorder, "blockfetch.block")
	require.Equal(t, connID.String(), attrs["connection.id"].AsString())
	require.Equal(t, int64(42), attrs["block.slot"].AsInt64())
	require.Equal(
		t,
		block.Hash().String(),
		attrs["block.hash"].AsString(),
	)
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestBlockfetchClientBlockRawTracesDecodeFailure(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	o := newTracingTestOuroboros(t)
	connID := testConnId()

	require.Error(t, o.blockfetchClientBlockRaw(
		blockfetch.CallbackContext{ConnectionId: connID},
		gledger.BlockTypeConway,
		[]byte{0xff},
	))

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, "blockfetch.block", ended[0].Name())
	require.Equal(t, codes.Error, ended[0].Status().Code)
	require.Contains(
		t,
		ended[0].Attributes(),
		attribute.String("connection.id", connID.String()),
	)
}

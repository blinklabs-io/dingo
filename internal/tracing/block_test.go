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

package tracing

import (
	"context"
	"crypto/rand"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Not t.Parallel: every test here installs the process-global tracer
// provider and the package's block tracing configuration.

func resetForTest() {
	state.Store(nil)
	nearTip.Store(false)
	forced.mu.Lock()
	clear(forced.set)
	clear(forced.ring)
	forced.next = 0
	forced.mu.Unlock()
}

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(
		sdktrace.NewTracerProvider(
			sdktrace.WithSampler(
				sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1)),
			),
			sdktrace.WithSpanProcessor(recorder),
		),
	)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return recorder
}

func configure(t *testing.T, cfg Config) {
	t.Helper()
	Configure(cfg)
	SetNearTip(true)
	t.Cleanup(resetForTest)
}

func randomHash(t *testing.T) []byte {
	t.Helper()
	h := make([]byte, 32)
	_, err := rand.Read(h)
	require.NoError(t, err)
	return h
}

func TestBlockTraceIDIsHashPrefix(t *testing.T) {
	h := randomHash(t)
	id := BlockTraceID(h)
	require.Equal(t, h[:16], id[:])
}

func TestTwoNodesAgree(t *testing.T) {
	h := randomHash(t)
	configure(t, Config{SampleRatio: 0.5})

	var ids []trace.TraceID
	var sampled []bool
	for range 2 {
		recorder := recordSpans(t)
		_, span := StartBlock(
			context.Background(), StageApply, "block.apply", h, nil,
		)
		span.End()
		sc := span.SpanContext()
		ids = append(ids, sc.TraceID())
		sampled = append(sampled, len(recorder.Ended()) == 1)
	}
	require.Equal(t, sampled[0], sampled[1])
	require.Equal(t, BlockSampled(h, nil), sampled[0])
	if sampled[0] {
		require.Equal(t, ids[0], ids[1])
		require.Equal(t, BlockTraceID(h), ids[0])
	}
}

func TestRatioMatchesSDK(t *testing.T) {
	for _, ratio := range []float64{0.01, 0.5} {
		configure(t, Config{SampleRatio: ratio})
		sdk := sdktrace.TraceIDRatioBased(ratio)
		for range 1000 {
			h := randomHash(t)
			want := sdk.ShouldSample(sdktrace.SamplingParameters{
				TraceID: BlockTraceID(h),
			}).Decision == sdktrace.RecordAndSample
			require.Equal(t, want, BlockSampled(h, nil), "ratio %v", ratio)
		}
		resetForTest()
	}
}

func TestPoolListForcesSampling(t *testing.T) {
	pool := [28]byte{1, 2, 3}
	other := [28]byte{9}
	configure(t, Config{SampleRatio: 0, AlwaysSamplePools: [][28]byte{pool}})
	require.True(t, BlockSampled(randomHash(t), pool[:]))
	require.False(t, BlockSampled(randomHash(t), other[:]))
}

func TestRollbackOfPoolSampledBlockIsSampled(t *testing.T) {
	pool := [28]byte{7}
	configure(t, Config{SampleRatio: 0, AlwaysSamplePools: [][28]byte{pool}})
	recordSpans(t)
	h := randomHash(t)

	_, applied := StartBlock(
		context.Background(), StageApply, "block.apply", h, pool[:],
	)
	applied.End()
	require.True(t, applied.IsRecording() || applied.SpanContext().IsSampled())

	_, rolledBack := StartBlock(
		context.Background(), StageRollback, "block.rolled_back", h, nil,
	)
	defer rolledBack.End()
	require.True(t, rolledBack.IsRecording())
	require.Equal(t, BlockTraceID(h), rolledBack.SpanContext().TraceID())
}

func TestStartBlockByronIssuerFallsBackToRatio(t *testing.T) {
	recordSpans(t)
	for _, tc := range []struct {
		ratio float64
		want  bool
	}{{1, true}, {0, false}} {
		configure(t, Config{SampleRatio: tc.ratio})
		_, span := StartBlock(
			context.Background(), StageApply, "block.apply",
			randomHash(t), nil,
		)
		require.Equal(t, tc.want, span.IsRecording(), "ratio %v", tc.ratio)
		span.End()
		resetForTest()
	}
}

func TestIssuerKeyHashNilForByron(t *testing.T) {
	require.Nil(t, IssuerKeyHash(fakeHeader{era: lcommon.Era{Id: 0}}))
	vkey := lcommon.IssuerVkey{1}
	got := IssuerKeyHash(fakeHeader{era: lcommon.Era{Id: 1}, vkey: vkey})
	require.Equal(t, vkey.Hash().Bytes(), got)
}

func TestBlockIDsFromShortOrZeroHash(t *testing.T) {
	configure(t, Config{SampleRatio: 1})
	recordSpans(t)
	short := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	zeroSpan := randomHash(t)
	clear(zeroSpan[16:24])
	for _, h := range [][]byte{short, zeroSpan} {
		_, span := StartBlock(
			context.Background(), StageApply, "block.apply", h, nil,
		)
		require.True(t, span.SpanContext().IsValid())
		span.End()
	}
}

func TestCatchUpReducedSet(t *testing.T) {
	configure(t, Config{SampleRatio: 1})
	SetNearTip(false)
	recordSpans(t)
	h := randomHash(t)

	_, header := StartBlock(
		context.Background(), StageHeaderReceived, "block.header_received",
		h, nil,
	)
	require.False(t, header.IsRecording())
	header.End()

	_, apply := StartBlock(
		context.Background(), StageApply, "block.apply", h, nil,
	)
	require.True(t, apply.IsRecording())
	apply.End()
}

func TestDisabledAllocatesNothing(t *testing.T) {
	resetForTest()
	h := make([]byte, 32)
	ctx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		_, span := StartBlock(ctx, StageApply, "block.apply", h, nil)
		span.End()
	})
	require.Zero(t, allocs)
}

func TestStartTxUsesTxHash(t *testing.T) {
	configure(t, Config{SampleRatio: 1})
	recordSpans(t)
	h := randomHash(t)
	_, span := StartTx(context.Background(), "tx.mempool_add", h)
	defer span.End()
	require.Equal(t, BlockTraceID(h), span.SpanContext().TraceID())
	require.True(t, span.IsRecording())
}

type fakeHeader struct {
	era  lcommon.Era
	vkey lcommon.IssuerVkey
}

func (fakeHeader) Hash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

func (fakeHeader) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (fakeHeader) BlockNumber() uint64              { return 0 }
func (fakeHeader) SlotNumber() uint64               { return 0 }
func (f fakeHeader) IssuerVkey() lcommon.IssuerVkey { return f.vkey }
func (fakeHeader) BlockBodySize() uint64            { return 0 }
func (f fakeHeader) Era() lcommon.Era               { return f.era }
func (fakeHeader) Cbor() []byte                     { return nil }

func (fakeHeader) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func TestDisableStopsBlockTracing(t *testing.T) {
	configure(t, Config{SampleRatio: 1})
	recordSpans(t)
	require.True(t, Enabled())
	Disable()
	require.False(t, Enabled())
	_, span := StartBlock(
		context.Background(), StageApply, "block.apply", randomHash(t), nil,
	)
	defer span.End()
	require.False(t, span.IsRecording())
}

type countingHeader struct {
	fakeHeader
	hash        lcommon.Blake2b256
	hashCalls   *int
	issuerCalls *int
}

func (c countingHeader) Hash() lcommon.Blake2b256 {
	*c.hashCalls++
	return c.hash
}

func (c countingHeader) IssuerVkey() lcommon.IssuerVkey {
	*c.issuerCalls++
	return c.vkey
}

func TestStartHeaderComputesNothingWhenDisabled(t *testing.T) {
	resetForTest()
	var hashCalls, issuerCalls int
	h := countingHeader{
		fakeHeader:  fakeHeader{era: lcommon.Era{Id: 6}},
		hashCalls:   &hashCalls,
		issuerCalls: &issuerCalls,
	}
	_, span := StartHeader(context.Background(), StageApply, "block.apply", h)
	span.End()
	require.Zero(t, hashCalls)
	require.Zero(t, issuerCalls)
}

func TestStartHeaderSkipsIssuerWhenRatioDecides(t *testing.T) {
	recordSpans(t)
	var hashCalls, issuerCalls int
	h := countingHeader{
		fakeHeader:  fakeHeader{era: lcommon.Era{Id: 6}},
		hash:        lcommon.Blake2b256(randomHash(t)),
		hashCalls:   &hashCalls,
		issuerCalls: &issuerCalls,
	}

	configure(t, Config{SampleRatio: 1, AlwaysSamplePools: [][28]byte{{1}}})
	_, span := StartHeader(context.Background(), StageApply, "block.apply", h)
	require.True(t, span.IsRecording())
	span.End()
	require.Zero(t, issuerCalls)

	configure(t, Config{SampleRatio: 0})
	_, span = StartHeader(context.Background(), StageApply, "block.apply", h)
	require.False(t, span.IsRecording())
	span.End()
	require.Zero(t, issuerCalls)

	pool := [28]byte(h.vkey.Hash().Bytes())
	configure(t, Config{SampleRatio: 0, AlwaysSamplePools: [][28]byte{pool}})
	_, span = StartHeader(context.Background(), StageApply, "block.apply", h)
	require.True(t, span.IsRecording())
	span.End()
	require.Equal(t, 1, issuerCalls)
}

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

package ledger

import (
	"context"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin the properties of
// github.com/blinklabs-io/gouroboros/pipeline that ARCHITECTURE.md's "Why
// dingo's ledger apply is not wired into pipeline.ApplyFunc" decision rests
// on. They deliberately assert upstream behavior rather than dingo behavior:
// the decision is only sound while the pipeline keeps this contract, so a
// gouroboros bump that changes it must fail here rather than silently
// invalidate a recorded architecture decision. If one of these fails, revisit
// the decision, not the test.

func noApply(*pipeline.BlockItem) error { return nil }

// A pipeline that applies blocks needs the full authoritative configuration,
// and the modes dingo uses to decode persisted blocks cannot carry an
// ApplyFunc, so using the pipeline for decode means leaving apply out.
func TestPipelineModesRejectAnApplyFunc(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		opts []pipeline.PipelineOption
		want error
	}{
		{
			name: "trusted decode-only",
			opts: []pipeline.PipelineOption{
				pipeline.WithTrustedDecodeOnly(),
				pipeline.WithApplyFunc(noApply),
			},
			want: pipeline.ErrTrustedDecodeOnlyApply,
		},
		{
			name: "header-only",
			opts: []pipeline.PipelineOption{
				pipeline.WithHeaderOnly(),
				pipeline.WithApplyFunc(noApply),
			},
			want: pipeline.ErrInvalidPipelineMode,
		},
		{
			name: "normal mode without block-local validation",
			opts: []pipeline.PipelineOption{
				pipeline.WithApplyFunc(noApply),
			},
			want: pipeline.ErrBlockValidationRequired,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := pipeline.NewBlockPipeline(tt.opts...)
			require.ErrorIs(t, p.Start(t.Context()), tt.want)
		})
	}
}

// In trusted decode-only mode, the one dingo uses, an undecodable block comes
// back as a result carrying its decode error and the pipeline carries on with
// the next block. decodeReadChainBatch relies on this to see a decode failure
// per item instead of losing the pipeline shared by every attempt.
func TestPipelineRejectedDecodeIsNotFatalInTrustedMode(t *testing.T) {
	t.Parallel()

	p := pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithTrustedDecodeOnly(),
	)
	require.NoError(t, p.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, p.Stop()) })
	go func() {
		//nolint:revive // draining is the point; the values are unused
		for range p.Errors() {
		}
	}()

	good, _ := buildDecodableTestBlock(t, 20, 2)
	submit := func(raw []byte, slot, number uint64) {
		t.Helper()
		require.NoError(t, p.Submit(
			t.Context(),
			gledger.BlockTypeConway,
			raw,
			ocommon.Tip{
				Point:       ocommon.NewPoint(slot, []byte{byte(slot)}),
				BlockNumber: number,
			},
		))
	}
	submit([]byte{0xff}, 10, 1)
	submit(good.Cbor, good.Slot, good.Number)

	first := testutil.RequireReceive(
		t, p.Results(), testutil.AsyncWait, "first result",
	)
	require.Error(t, first.DecodeError())
	second := testutil.RequireReceive(
		t, p.Results(), testutil.AsyncWait, "second result",
	)
	require.NoError(t, second.DecodeError())
	require.Equal(t, good.Slot, second.Block().SlotNumber())
}

// Outside trusted decode-only mode a rejected item ends the pipeline's
// ordered stage: the rejection is reported by a fence and every later Submit
// fails, so one bad block is fatal to a pipeline shared by every attempt.
func TestPipelineRejectedItemStopsOrderedProcessing(t *testing.T) {
	t.Parallel()

	p := pipeline.NewBlockPipeline(
		pipeline.WithHeaderOnly(),
		pipeline.WithValidateWorkers(1),
		pipeline.WithBlockTypeResolver(
			func(context.Context, []byte) (uint, error) {
				return gledger.BlockTypeConway, nil
			},
		),
		pipeline.WithEta0(strings.Repeat("00", 32)),
		pipeline.WithSlotsPerKesPeriod(129600),
	)
	require.NoError(t, p.Start(t.Context()))
	t.Cleanup(func() { _ = p.Stop() })
	go func() {
		//nolint:revive // draining is the point; the values are unused
		for range p.Errors() {
		}
	}()

	tip := func(slot uint64) ocommon.Tip {
		return ocommon.Tip{
			Point:       ocommon.NewPoint(slot, []byte{byte(slot)}),
			BlockNumber: slot,
		}
	}
	require.NoError(t, p.Submit(
		t.Context(), gledger.BlockTypeConway, []byte{0xff}, tip(1),
	))
	require.ErrorIs(
		t,
		p.Fence(t.Context()),
		pipeline.ErrPipelineItemRejected,
	)
	good, _ := buildDecodableTestBlock(t, 20, 2)
	require.Error(t, p.Submit(
		t.Context(), gledger.BlockTypeConway, good.Cbor, tip(2),
	))
}

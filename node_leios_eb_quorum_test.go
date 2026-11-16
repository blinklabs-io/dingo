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

package dingo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/leios"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// The slot's own epoch and parameters decide, not the tip's: the first
// endorser block of an epoch is forged under that epoch's committee.
func TestLeiosEndorserBlockQuorumRefusalUsesTheSlotEpoch(t *testing.T) {
	t.Parallel()
	source := &fakeQuorumSource{possible: true}
	var paramsSlot uint64
	refusal := leiosEndorserBlockQuorumRefusal(
		source,
		func(slot uint64) (uint64, error) { return slot / 100, nil },
		func(slot uint64) lcommon.ProtocolParameters {
			paramsSlot = slot
			return dijkstraQuorumPParams(big.NewRat(3, 4))
		},
		1000,
	)
	require.Empty(t, refusal)
	require.Equal(t, 1, source.calls)
	require.Equal(t, uint64(10), source.epoch)
	require.Equal(t, uint64(1000), paramsSlot)
	require.Zero(t, big.NewRat(3, 4).Cmp(source.tau))
}

func TestLeiosEndorserBlockQuorumRefusal(t *testing.T) {
	t.Parallel()
	epochOf := func(slot uint64) (uint64, error) { return slot / 100, nil }
	dijkstra := func(uint64) lcommon.ProtocolParameters {
		return dijkstraQuorumPParams(big.NewRat(3, 4))
	}
	cases := []struct {
		name    string
		source  leiosQuorumSource
		epochOf func(uint64) (uint64, error)
		pparams func(uint64) lcommon.ProtocolParameters
		want    string
	}{
		{
			name:    "committee reaches quorum",
			source:  &fakeQuorumSource{possible: true},
			epochOf: epochOf,
			pparams: dijkstra,
		},
		{
			name:    "committee short of quorum",
			source:  &fakeQuorumSource{},
			epochOf: epochOf,
			pparams: dijkstra,
			want:    leiosEBSkipQuorumNotPossible,
		},
		{
			name:    "committee cannot be resolved",
			source:  &fakeQuorumSource{possible: true, err: errors.New("no snapshot")},
			epochOf: epochOf,
			pparams: dijkstra,
			want:    leiosEBSkipQuorumUnresolved,
		},
		{
			name:    "no vote manager",
			epochOf: epochOf,
			pparams: dijkstra,
			want:    leiosEBSkipQuorumUnresolved,
		},
		{
			name:   "slot epoch unknown",
			source: &fakeQuorumSource{possible: true},
			epochOf: func(uint64) (uint64, error) {
				return 0, errors.New("beyond the horizon")
			},
			pparams: dijkstra,
			want:    leiosEBSkipQuorumUnresolved,
		},
		{
			name:    "no quorum threshold",
			source:  &fakeQuorumSource{possible: true},
			epochOf: epochOf,
			pparams: func(uint64) lcommon.ProtocolParameters {
				return dijkstraQuorumPParams(nil)
			},
			want: leiosEBSkipQuorumUnresolved,
		},
		{
			// The Conway-header prototype has no committee to consult; the
			// reference sizes no endorser block there either way.
			name:    "parameters before Dijkstra",
			source:  &fakeQuorumSource{},
			epochOf: epochOf,
			pparams: func(uint64) lcommon.ProtocolParameters {
				return &conway.ConwayProtocolParameters{}
			},
		},
		{
			name:    "parameters unresolved",
			source:  &fakeQuorumSource{},
			epochOf: epochOf,
			pparams: func(uint64) lcommon.ProtocolParameters { return nil },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t,
				tc.want,
				leiosEndorserBlockQuorumRefusal(tc.source, tc.epochOf, tc.pparams, 500),
			)
		})
	}
}

// A committee that cannot reach its quorum leaves no endorser block to
// forge, whatever the pipeline allows; the refusal carries its own reason.
func TestLeiosPipelineAdapterRefusesEndorserBlockWithoutQuorum(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()
	mgr, err := leios.NewPipelineManager(leios.PipelineManagerConfig{
		EventBus:      bus,
		SlotProvider:  occurrencePipelineClock{},
		EpochProvider: occurrencePipelineClock{},
		Timing:        leios.DefaultPipelineTiming(),
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Start(t.Context()))
	defer func() { require.NoError(t, mgr.Stop()) }()

	refusal := leiosEBSkipQuorumNotPossible
	var askedSlot uint64
	adapter := &leiosPipelineAdapter{
		mgr: mgr,
		endorserBlockQuorumRefusal: func(slot uint64) string {
			askedSlot = slot
			return refusal
		},
	}
	allowed, reason, err := adapter.MayProduceEndorserBlock(101)
	require.NoError(t, err)
	require.False(t, allowed)
	require.Equal(t, leiosEBSkipQuorumNotPossible, reason)
	require.Equal(t, uint64(101), askedSlot)

	refusal = ""
	allowed, reason, err = adapter.MayProduceEndorserBlock(101)
	require.NoError(t, err)
	require.True(t, allowed)
	require.Empty(t, reason)
}

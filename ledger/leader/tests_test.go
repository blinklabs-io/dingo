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

package leader_test

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/consensus/leaderthreshold"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/stretchr/testify/require"
)

func TestThresholdPreservesProtocolBoundary(t *testing.T) {
	tests := []struct {
		name  string
		mode  consensus.ConsensusMode
		f     *big.Rat
		pool  uint64
		total uint64
		want  func(*big.Int) *big.Int
	}{
		{
			name:  "cpraos fractional cutoff admits its floor",
			mode:  consensus.ConsensusModeCPraos,
			f:     big.NewRat(1, 3),
			pool:  1,
			total: 1,
			want: func(upper *big.Int) *big.Int {
				return new(big.Int).Add(
					new(big.Int).Quo(upper, big.NewInt(3)),
					big.NewInt(1),
				)
			},
		},
		{
			name:  "tpraos fractional cutoff admits its floor",
			mode:  consensus.ConsensusModeTPraos,
			f:     big.NewRat(1, 3),
			pool:  1,
			total: 1,
			want: func(upper *big.Int) *big.Int {
				return new(big.Int).Add(
					new(big.Int).Quo(upper, big.NewInt(3)),
					big.NewInt(1),
				)
			},
		},
		{
			name:  "cpraos exact cutoff rejects its boundary",
			mode:  consensus.ConsensusModeCPraos,
			f:     big.NewRat(1, 2),
			pool:  1,
			total: 1,
			want: func(upper *big.Int) *big.Int {
				return new(big.Int).Quo(upper, big.NewInt(2))
			},
		},
		{
			name:  "tpraos exact partial stake cutoff rejects its boundary",
			mode:  consensus.ConsensusModeTPraos,
			f:     big.NewRat(3, 4),
			pool:  1,
			total: 2,
			want: func(upper *big.Int) *big.Int {
				return new(big.Int).Quo(upper, big.NewInt(2))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			threshold, err := leaderthreshold.Threshold(
				tt.pool, tt.total, tt.f, tt.mode,
			)
			require.NoError(t, err)

			bits := uint(256)
			if tt.mode == consensus.ConsensusModeTPraos {
				bits = 512
			}
			upper := new(big.Int).Lsh(big.NewInt(1), bits)
			want := tt.want(upper)
			require.Equal(t, 0, threshold.Cmp(want))
		})
	}
}

func TestThresholdRejectsInvalidExactBoundaryForPartialStake(t *testing.T) {
	threshold, err := leaderthreshold.Threshold(
		1,
		2,
		big.NewRat(3, 4),
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)

	upper := new(big.Int).Lsh(big.NewInt(1), 256)
	want := new(big.Int).Quo(upper, big.NewInt(2))
	require.Equal(t, 0, threshold.Cmp(want))
}

func TestThresholdExactBoundaryRemainsRejected(t *testing.T) {
	threshold, err := leaderthreshold.Threshold(
		1,
		1,
		big.NewRat(1, 2),
		consensus.ConsensusModeTPraos,
	)
	require.NoError(t, err)

	atOutput := make([]byte, 64)
	copy(atOutput[len(atOutput)-len(threshold.Bytes()):], threshold.Bytes())
	eligible, err := consensus.IsVRFOutputBelowThresholdWithMode(
		atOutput,
		threshold,
		consensus.ConsensusModeTPraos,
	)
	require.NoError(t, err)
	require.False(t, eligible,
		"an integer real cutoff must reject its exact boundary")

	below := new(big.Int).Sub(threshold, big.NewInt(1))
	belowOutput := make([]byte, 64)
	copy(belowOutput[len(belowOutput)-len(below.Bytes()):], below.Bytes())
	eligible, err = consensus.IsVRFOutputBelowThresholdWithMode(
		belowOutput,
		threshold,
		consensus.ConsensusModeTPraos,
	)
	require.NoError(t, err)
	require.True(t, eligible,
		"the value below an integer cutoff must remain eligible")
}

func TestThresholdBoundaryFeedsTheVRFComparator(t *testing.T) {
	threshold, err := leaderthreshold.Threshold(
		1,
		1,
		big.NewRat(1, 3),
		consensus.ConsensusModeTPraos,
	)
	require.NoError(t, err)

	below := new(big.Int).Sub(threshold, big.NewInt(1))
	belowOutput := make([]byte, 64)
	copy(belowOutput[len(belowOutput)-len(below.Bytes()):], below.Bytes())
	eligible, err := consensus.IsVRFOutputBelowThresholdWithMode(
		belowOutput,
		threshold,
		consensus.ConsensusModeTPraos,
	)
	require.NoError(t, err)
	require.True(t, eligible,
		"the valid floor of a fractional real cutoff must be eligible")

	atOutput := make([]byte, 64)
	copy(atOutput[len(atOutput)-len(threshold.Bytes()):], threshold.Bytes())
	eligible, err = consensus.IsVRFOutputBelowThresholdWithMode(
		atOutput,
		threshold,
		consensus.ConsensusModeTPraos,
	)
	require.NoError(t, err)
	require.False(t, eligible,
		"the integer comparison boundary must remain strictly rejected")
}

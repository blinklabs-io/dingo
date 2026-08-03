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
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLeiosCommitteeParamsFromPParamsDefaultsWhenBothUnset covers the issue
// #2836 root cause: musashi ships a refScript-only dijkstra genesis, so
// neither committee stake coverage (sigma_c) nor quorum stake threshold
// (tau) is configured. The genesis is immutable, so the adapter must fall
// back to the CIP-0164 defaults (0.99 / 0.75) rather than erroring or
// returning nil, which is what lets committee formation and certification
// proceed.
func TestLeiosCommitteeParamsFromPParamsDefaultsWhenBothUnset(t *testing.T) {
	pp := &gdijkstra.DijkstraProtocolParameters{}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(99, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(3, 4)))
}

// TestLeiosCommitteeParamsFromPParamsDefaultsMissingCoverage confirms a
// configured tau is preserved while an unset sigma_c falls back to its
// default (tau=1/2 < default sigma_c=0.99 holds).
func TestLeiosCommitteeParamsFromPParamsDefaultsMissingCoverage(t *testing.T) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		QuorumStakeThreshold: &cbor.Rat{Rat: big.NewRat(1, 2)},
	}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(99, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(1, 2)))
}

// TestLeiosCommitteeParamsFromPParamsDefaultsMissingQuorum confirms a
// configured sigma_c is preserved while an unset tau falls back to its
// default (default tau=0.75 < sigma_c=0.99 holds).
func TestLeiosCommitteeParamsFromPParamsDefaultsMissingQuorum(t *testing.T) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(99, 100)},
	}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(99, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(3, 4)))
}

// TestLeiosCommitteeParamsFromPParamsReturnsBothWhenConfigured confirms a
// fully configured genesis flows both values through unchanged (no default
// applied).
func TestLeiosCommitteeParamsFromPParamsReturnsBothWhenConfigured(
	t *testing.T,
) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(95, 100)},
		QuorumStakeThreshold:   &cbor.Rat{Rat: big.NewRat(3, 5)},
	}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(95, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(3, 5)))
}

// TestLeiosCommitteeParamsFromPParamsRejectsInvariantViolation confirms the
// tau < sigma_c invariant is enforced when both are configured.
func TestLeiosCommitteeParamsFromPParamsRejectsInvariantViolation(
	t *testing.T,
) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(3, 4)},
		QuorumStakeThreshold:   &cbor.Rat{Rat: big.NewRat(3, 4)},
	}
	_, _, err := leiosCommitteeParamsFromPParams(pp)
	require.Error(t, err)
}

// TestLeiosCommitteeParamsFromPParamsRejectsDefaultInvariantViolation
// confirms the post-default re-check: a configured sigma_c below the default
// tau (0.75) with tau unset would otherwise yield tau >= sigma_c.
func TestLeiosCommitteeParamsFromPParamsRejectsDefaultInvariantViolation(
	t *testing.T,
) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(1, 2)},
	}
	_, _, err := leiosCommitteeParamsFromPParams(pp)
	require.Error(t, err)
}

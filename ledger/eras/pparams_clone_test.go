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

package eras

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func TestCloneGovernanceProtocolParametersOwnsDijkstraPointers(t *testing.T) {
	t.Parallel()

	rat := func() *cbor.Rat {
		return &cbor.Rat{Rat: big.NewRat(1, 2)}
	}
	bootstrapRound := uint32(7)
	source := &gdijkstra.DijkstraProtocolParameters{
		RefScriptCostMultiplier:          rat(),
		MaxPledgeLeverage:                rat(),
		MinPoolMargin:                    rat(),
		LeiosQuorumStakeThreshold:        rat(),
		PerasHealingFactor:               rat(),
		PerasBootstrapRound:              &bootstrapRound,
		PerasQuorumThresholdSafetyMargin: rat(),
		CommitteeStakeCoverage:           rat(),
		QuorumStakeThreshold:             rat(),
	}

	clonedParams, err := CloneGovernanceProtocolParameters(source)
	require.NoError(t, err)
	cloned := clonedParams.(*gdijkstra.DijkstraProtocolParameters)

	ratFields := []struct {
		name   string
		source *cbor.Rat
		cloned *cbor.Rat
	}{
		{
			"reference script cost multiplier",
			source.RefScriptCostMultiplier,
			cloned.RefScriptCostMultiplier,
		},
		{
			"maximum pledge leverage",
			source.MaxPledgeLeverage,
			cloned.MaxPledgeLeverage,
		},
		{"minimum pool margin", source.MinPoolMargin, cloned.MinPoolMargin},
		{
			"Leios quorum stake threshold",
			source.LeiosQuorumStakeThreshold,
			cloned.LeiosQuorumStakeThreshold,
		},
		{
			"Peras healing factor",
			source.PerasHealingFactor,
			cloned.PerasHealingFactor,
		},
		{
			"Peras quorum safety margin",
			source.PerasQuorumThresholdSafetyMargin,
			cloned.PerasQuorumThresholdSafetyMargin,
		},
		{
			"committee stake coverage",
			source.CommitteeStakeCoverage,
			cloned.CommitteeStakeCoverage,
		},
		{
			"quorum stake threshold",
			source.QuorumStakeThreshold,
			cloned.QuorumStakeThreshold,
		},
	}
	for _, field := range ratFields {
		t.Run(field.name, func(t *testing.T) {
			require.NotSame(t, field.source, field.cloned)
			require.NotSame(t, field.source.Rat, field.cloned.Rat)
			field.cloned.Rat.SetInt64(3)
			require.Zero(t, field.source.Cmp(big.NewRat(1, 2)))
		})
	}

	require.NotSame(t, source.PerasBootstrapRound, cloned.PerasBootstrapRound)
	*cloned.PerasBootstrapRound = 9
	require.Equal(t, uint32(7), *source.PerasBootstrapRound)
}

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

package plugins

import (
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestGenesisStakingEmpty(t *testing.T) {
	t.Parallel()
	require.False(
		t,
		GenesisStakingEmpty(nil),
		"unknown genesis must not certify empty staking",
	)
	for _, tc := range []struct {
		name      string
		configure func(*shelley.ShelleyGenesis)
		want      bool
	}{
		{name: "empty genesis", want: true},
		{name: "empty extra staking", want: true, configure: func(g *shelley.ShelleyGenesis) {
			g.ExtraConfig = &shelley.ShelleyGenesisExtraConfig{}
		}},
		{name: "native pool", configure: func(g *shelley.ShelleyGenesis) {
			g.Staking.Pools = map[string]common.PoolRegistrationCertificate{"pool": {}}
		}},
		{name: "native delegation", configure: func(g *shelley.ShelleyGenesis) {
			g.Staking.Stake = map[string]string{"credential": "pool"}
		}},
		{name: "extra credential", configure: func(g *shelley.ShelleyGenesis) {
			g.ExtraConfig = &shelley.ShelleyGenesisExtraConfig{}
			g.ExtraConfig.StakeCredentials.Data = map[string]string{"credential": "pool"}
		}},
		{name: "extra pool", configure: func(g *shelley.ShelleyGenesis) {
			g.ExtraConfig = &shelley.ShelleyGenesisExtraConfig{}
			g.ExtraConfig.StakePools.Data = map[string]shelley.ShelleyGenesisExtraPool{"pool": {}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			genesis := &shelley.ShelleyGenesis{}
			if tc.configure != nil {
				tc.configure(genesis)
			}
			require.Equal(t, tc.want, GenesisStakingEmpty(genesis))
		})
	}
}

func TestResolveGenesisStakingEmptyRequiresKnownGenesis(t *testing.T) {
	t.Parallel()
	empty, err := ResolveGenesisStakingEmpty(nil, "", "")
	require.NoError(t, err)
	require.False(
		t,
		empty,
		"an unspecified network must not certify a different network's genesis",
	)
	empty, err = ResolveGenesisStakingEmpty(nil, "", "preview")
	require.NoError(t, err)
	require.True(t, empty)
	config, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath(
			"preview",
		),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	empty, err = ResolveGenesisStakingEmpty(
		config,
		"invalid config path",
		"unknown network",
	)
	require.NoError(t, err)
	require.True(t, empty, "a supplied parsed config is authoritative")
	empty, err = ResolveGenesisStakingEmpty(
		nil,
		"missing config path",
		"unknown network",
	)
	require.Error(t, err)
	require.False(
		t,
		empty,
		"a failed explicit config load cannot certify genesis",
	)
}

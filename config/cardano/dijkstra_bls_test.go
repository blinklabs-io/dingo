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

package cardano

import (
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestDijkstraPoolBlsProofDomainCanonicalIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, network string
		magic         uint32
		mutate        func(*CardanoNodeConfig)
		legacy        bool
	}{
		{name: "canonical", network: "dijkstra", magic: 6, legacy: true},
		{name: "named", network: "dijkstra", legacy: true},
		{name: "magic_only", magic: 6, legacy: true},
		{name: "missing_identity"},
		{name: "conflicting_name", network: "preview", magic: 6},
		{name: "private_name", network: "private", magic: 6},
		{name: "conflicting_magic", network: "dijkstra", magic: 164},
		{name: "private_magic", network: "dijkstra", magic: 42},
		{name: "private_byron", network: "dijkstra", magic: 6, mutate: func(c *CardanoNodeConfig) { c.ByronGenesisHash = "private" }},
		{name: "private_shelley", network: "dijkstra", magic: 6, mutate: func(c *CardanoNodeConfig) { c.ShelleyGenesisHash = "private" }},
		{name: "private_alonzo", network: "dijkstra", magic: 6, mutate: func(c *CardanoNodeConfig) { c.AlonzoGenesisHash = "private" }},
		{name: "private_conway", network: "dijkstra", magic: 6, mutate: func(c *CardanoNodeConfig) { c.ConwayGenesisHash = "private" }},
		{name: "private_dijkstra", network: "dijkstra", magic: 6, mutate: func(c *CardanoNodeConfig) { c.DijkstraGenesisHash = "private" }},
		{name: "missing_loaded_genesis", network: "dijkstra", magic: 6, mutate: func(c *CardanoNodeConfig) { *c = CardanoNodeConfig{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewCardanoNodeConfigFromEmbedFS(
				EmbeddedConfigFS,
				EmbeddedConfigPath("dijkstra"),
			)
			require.NoError(t, err)
			if tc.mutate != nil {
				tc.mutate(c)
			}
			want := lcommon.DijkstraBlsProofDomainPop
			if tc.legacy {
				want = lcommon.DijkstraBlsProofDomainLegacySig
			}
			require.Equal(
				t,
				want,
				DijkstraPoolBlsProofDomain(c, tc.network, tc.magic),
			)
		})
	}
	require.Equal(
		t,
		lcommon.DijkstraBlsProofDomainPop,
		DijkstraPoolBlsProofDomain(nil, "dijkstra", 6),
	)
	musashi, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		EmbeddedConfigPath("musashi"),
	)
	require.NoError(t, err)
	require.Equal(
		t,
		lcommon.DijkstraBlsProofDomainPop,
		DijkstraPoolBlsProofDomain(musashi, "musashi", 164),
	)
	require.Equal(
		t,
		lcommon.DijkstraBlsProofDomainPop,
		DijkstraPoolBlsProofDomain(musashi, "dijkstra", 6),
	)
}

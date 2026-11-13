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
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLedgerStateConfigSelectsDijkstraBlsProofDomain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		network string
		magic   uint32
		legacy  bool
	}{
		{"dijkstra", 6, true}, {"musashi", 164, false}, {"preview", 2, false}, {"preview", 6, false},
	} {
		t.Run(tc.network, func(t *testing.T) {
			t.Parallel()
			c, err := cardano.NewCardanoNodeConfigFromEmbedFS(
				cardano.EmbeddedConfigFS,
				cardano.EmbeddedConfigPath("dijkstra"),
			)
			require.NoError(t, err)
			n := &Node{
				config: Config{
					cfg: &internalconfig.Config{
						Network:      tc.network,
						NetworkMagic: tc.magic,
					},
					network:           tc.network,
					networkMagic:      tc.magic,
					cardanoNodeConfig: c,
				},
			}
			want := lcommon.DijkstraBlsProofDomainPop
			if tc.legacy {
				want = lcommon.DijkstraBlsProofDomainLegacySig
			}
			require.Equal(t, want, n.ledgerStateConfig().DijkstraBlsProofDomain)
		})
	}
}

func TestLeiosVoteManagerUsesSelectedKeyProofDomain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		network string
		magic   uint32
		want    lcommon.DijkstraBlsProofDomain
	}{
		{"dijkstra", 6, lcommon.DijkstraBlsProofDomainLegacySig},
		{"musashi", 164, lcommon.DijkstraBlsProofDomainPop},
	} {
		t.Run(tc.network, func(t *testing.T) {
			t.Parallel()
			c, err := cardano.NewCardanoNodeConfigFromEmbedFS(
				cardano.EmbeddedConfigFS,
				cardano.EmbeddedConfigPath("dijkstra"),
			)
			require.NoError(t, err)
			n, _ := newLiveLifecycleTestNode(t, 1)
			n.config.network = tc.network
			n.config.networkMagic = tc.magic
			n.config.cardanoNodeConfig = c
			require.NoError(t, n.initLeiosVoteManager(t.Context()))
			t.Cleanup(func() { _ = n.leiosVoteManager.Stop() })
			require.Equal(t, tc.want, n.leiosVoteManager.KeyProofDomain())
		})
	}
}

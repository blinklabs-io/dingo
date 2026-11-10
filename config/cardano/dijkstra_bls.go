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
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// DijkstraPoolBlsProofDomain selects the older Haskell pool-proof domain only
// for the canonical DijkstraNet chain. Reusing its name or magic on a private
// chain must not change that chain's validation rules.
func DijkstraPoolBlsProofDomain(
	c *CardanoNodeConfig,
	network string,
	magic uint32,
) lcommon.DijkstraBlsProofDomain {
	net := ouroboros.NetworkCardanoDijkstra
	if c == nil || (network != net.Name && magic != net.NetworkMagic) ||
		(network != "" && network != net.Name) ||
		(magic != 0 && magic != net.NetworkMagic) {
		return lcommon.DijkstraBlsProofDomainPop
	}
	genesis := c.ShelleyGenesis()
	if genesis == nil || genesis.NetworkMagic != net.NetworkMagic {
		return lcommon.DijkstraBlsProofDomainPop
	}
	// These hashes identify the chain whose POOL rule uses BLS_SIG, rather
	// than the IETF BLS_POP domain used by the Leios release candidate.
	if c.ByronGenesisHash != "245e5b28a31c6bc3e8b78f1223294ae09e57b6d2543b51aba7cf9dadff161238" ||
		c.ShelleyGenesisHash != "b47eb627b52bf9cfe5ffd865658b47b5747a088d150a9c27216de374e836c385" ||
		c.AlonzoGenesisHash != "387a7c4880477ce7b128566fa7f9f9ed99ee04476084e9f6332b6d42d907faab" ||
		c.ConwayGenesisHash != "e18ab8efc363ad170142706723568f1520a57f0dc05354e1d458041da18a3704" ||
		c.DijkstraGenesisHash != "ad58e282eff1f48c241faf6319f88ed54843821627a6be69470f05d7904e4601" {
		return lcommon.DijkstraBlsProofDomainPop
	}
	return lcommon.DijkstraBlsProofDomainLegacySig
}

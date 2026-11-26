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
	"bytes"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// A new node whose genesis has initial staking and governance writes the
// genesis pool, accounts and DRep into the live UTxO set on its first start,
// and a restart still at origin writes the genesis again without failing.
func TestNodeWritesStakingGenesisIntoTheSetOnItsFirstStart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for boot := range 2 {
		n, stop := bootSetNode(t, dir, "devnet", withGenesisGovernance)
		require.Eventually(t, func() bool {
			var pools int
			if err := n.utxoSet.PoolStates(func(*ledger.PoolState) error {
				pools++
				return nil
			}); err != nil {
				return false
			}
			refs, err := n.utxoSet.ActiveAccountRefs()
			if err != nil || pools != 1 || len(refs) != 2 {
				return false
			}
			drep, err := n.utxoSet.Drep(
				uint8(lcommon.CredentialTypeAddrKeyHash), genesisDRep[:],
			)
			return err == nil && drep != nil
		}, 5*time.Second, 10*time.Millisecond,
			"boot %d: the genesis pool, accounts and DRep are in the set", boot)
		stop()
	}
}

var genesisDRep = lcommon.CredentialHash(bytes.Repeat([]byte{0xd1}, 28))

// withGenesisGovernance gives the Conway genesis one initial DRep and one
// account delegating its vote to it.
func withGenesisGovernance(cfg *cardano.CardanoNodeConfig) {
	g := cfg.ConwayGenesis()
	cred := &lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash, Credential: genesisDRep,
	}
	g.InitialDReps = conway.ConwayGenesisInitialDReps{
		cred: {Expiry: 100, Deposit: 500_000_000},
	}
	g.Delegs = conway.ConwayGenesisDelegs{
		{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(bytes.Repeat([]byte{0xc1}, 28)),
		}: {
			Type: conway.ConwayGenesisDelegateeTypeVote,
			DRep: lcommon.Drep{
				Type:       lcommon.DrepTypeAddrKeyHash,
				Credential: genesisDRep[:],
			},
		},
	}
}

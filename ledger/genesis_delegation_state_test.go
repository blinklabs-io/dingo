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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestGenesisDelegationStateTracksAppliedDelegations(t *testing.T) {
	ls, _, raw := newMIRTestLedger(t)
	nodeConfig, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	ls.config.CardanoNodeConfig = nodeConfig
	genesis := nodeConfig.ShelleyGenesis()
	initial, err := parseShelleyGenesisDelegations(genesis)
	require.NoError(t, err)
	require.NotEmpty(t, initial)
	ls.SetTipForTesting(ochainsync.Tip{Point: ocommon.Point{Slot: 100}})

	delegates, err := ls.GenesisDelegateKeyHashes()
	require.NoError(t, err)
	require.Len(t, delegates, len(initial))
	require.Equal(t, common.NewBlake2b224(initial[0].delegateHash), delegates[0])
	require.Equal(t, uint(genesis.UpdateQuorum), mustGenesisQuorum(t, ls))

	newDelegate := bytes.Repeat([]byte{0xD1}, common.Blake2b224Size)
	_, err = raw.Exec(`
INSERT INTO genesis_delegation (
    genesis_hash, genesis_delegate_hash, vrf_key_hash, added_slot,
    block_index, cert_index, certificate_id
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		initial[0].genesisHash, newDelegate,
		bytes.Repeat([]byte{0xE2}, common.Blake2b256Size),
		100, 0, 0, 1,
	)
	require.NoError(t, err)

	delegates, err = ls.GenesisDelegateKeyHashes()
	require.NoError(t, err)
	require.Equal(t, common.NewBlake2b224(newDelegate), delegates[0])

	ls.SetTipForTesting(ochainsync.Tip{Point: ocommon.Point{Slot: 99}})
	delegates, err = ls.GenesisDelegateKeyHashes()
	require.NoError(t, err)
	require.Equal(t, common.NewBlake2b224(initial[0].delegateHash), delegates[0])

}

func mustGenesisQuorum(t *testing.T, ls *LedgerState) uint {
	t.Helper()
	quorum, err := ls.GenesisUpdateQuorum()
	require.NoError(t, err)
	return quorum
}

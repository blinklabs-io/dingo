//go:build linux && devnet

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

package devnet

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/leios"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	ledgerbyron "github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLeiosFetchBitmapsSelectAllReferences(t *testing.T) {
	for _, tc := range []struct {
		count int
		want  map[uint16]uint64
	}{
		{count: 1, want: map[uint16]uint64{0: 1 << 63}},
		{count: 64, want: map[uint16]uint64{0: ^uint64(0)}},
		{count: 65, want: map[uint16]uint64{0: ^uint64(0), 1: 1 << 63}},
	} {
		got, err := leiosFetchBitmaps(tc.count)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestLeiosFetchBitmapsRejectsInvalidCounts(t *testing.T) {
	for _, count := range []int{0, -1, (int(^uint16(0))+1)*64 + 1} {
		_, err := leiosFetchBitmaps(count)
		require.Error(t, err, "count %d", count)
	}
}

func TestLeiosDevnetCredentialsMatchGenesisRegistration(t *testing.T) {
	var registered lcommon.LeiosKey
	keyData, err := os.ReadFile("testdata/leios-key.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(keyData, &registered))
	require.NoError(t, leios.VerifyLeiosKeyProofOfPossession(&registered))

	secretData, err := os.ReadFile("testdata/leios-vote.skey")
	require.NoError(t, err)
	voteKey, err := leios.ParseVoteSigningKey(strings.TrimSpace(string(secretData)))
	require.NoError(t, err)
	require.Equal(t, registered.PublicKey, voteKey.PublicKeyBytes())

	proof, err := leios.SignLeiosKeyProofOfPossession(voteKey)
	require.NoError(t, err)
	require.Equal(t, registered.PossessionProof, proof)
}

func TestDevnetByronGenesisTrustRootIsValid(t *testing.T) {
	data, err := os.ReadFile("testdata/byron-heavy-delegation.json")
	require.NoError(t, err)
	var fixture struct {
		BootStakeholders map[string]int                                     `json:"bootStakeholders"`
		HeavyDelegation  map[string]ledgerbyron.ByronGenesisHeavyDelegation `json:"heavyDelegation"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Len(t, fixture.BootStakeholders, 1)
	require.Len(t, fixture.HeavyDelegation, 1)

	_, err = byronconsensus.NewByronConfigFromGenesis(&ledgerbyron.ByronGenesis{
		ProtocolConsts: ledgerbyron.ByronGenesisProtocolConsts{
			K:             10,
			ProtocolMagic: 42,
		},
		BlockVersionData: ledgerbyron.ByronGenesisBlockVersionData{
			SlotDuration: 20_000,
		},
		BootStakeholders: fixture.BootStakeholders,
		HeavyDelegation:  fixture.HeavyDelegation,
	})
	require.NoError(t, err)
}

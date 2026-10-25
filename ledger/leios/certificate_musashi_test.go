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

package leios

import (
	"context"
	"encoding/hex"
	"maps"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The Musashi epoch 7 stake snapshot, Dijkstra committee parameters, and
// registered Leios keys, as the node's ledger adapters return them while
// syncing Musashi. They seat the committee for epoch 8.
var (
	musashiEpoch7PoolStakes = map[string]uint64{
		"5f0b42b4fbdc7e442d5da37744cafdca86e0e7f3bf94223e75e9312b": 10034050691231,
		"8a04e2c7db3dff509c9f5f2b8e27d715eb8f3d0665deddc2e436f414": 11040096330872,
		"8a89ad6a871bdc17039285768e4ac8e24df94c7ddcc342752852976b": 10038142240282,
	}
	musashiEpoch7TotalStake uint64 = 31112289262385
	// Public key, then proof of possession.
	musashiEpoch7LeiosKeys = map[string][2]string{
		"5f0b42b4fbdc7e442d5da37744cafdca86e0e7f3bf94223e75e9312b": {
			"b4696f340bf20cda70e2558fcae97da3d3dfca58fdfc8388505f824d7a3a67f4" +
				"3d93b6d30ade366aa5571a64defb17ca10310605bb7dfb200704f25ae9c51fe8" +
				"5b3bd4f707b72679a1cd7c1e434b48c34a169c29eef3440a8245a4611fd60c1d",
			"93f0913cb68490566dab9ea5f7ca2963b795ba662a5599a41c65017803e4c98c" +
				"f6497ca5d083896ec100af28aacc6a3b",
		},
		"8a04e2c7db3dff509c9f5f2b8e27d715eb8f3d0665deddc2e436f414": {
			"8d5e3e9901dbfa64a9c5d9f9949ecb7491b4e35656963bbf6e478c4e53d329f3" +
				"d1209ed5924c228305d4fcff8a4935d40d508fe3ec6cb8e892d8a00b22c78966" +
				"9336e852d0a10eb2340ea1becf7c87e76eaeacfa5a847104354c5b095b84e2b4",
			"805438cd07ebc6fd19f424401fd0395dc801a8561302bd1ae7bcc48cbdbe664d" +
				"3a4dfb8fa5dc3a0bc69045b0cd9f9a4c",
		},
		"8a89ad6a871bdc17039285768e4ac8e24df94c7ddcc342752852976b": {
			"954ef2360afbbe4afe1318aa78dd13a218534e70cb746c94794a75d0454e3baa" +
				"9e68b3195dbdc77e59305007fd7ca4c10cd7f25102b3f8ab23f6a00ad06ecf25" +
				"e4e7bdd140de37217eece53fea007b836b17a376ffc4fc9fc03e89c3efbabec7",
			"881bf5706270b5de986da87b5621088e5cbf6fa441daaa5a5b2d8cdaeb15fd4c" +
				"8b49cea0df2d211d764d138d015ded34",
		},
	}
)

// musashiEpoch8Certificate is the leios_certificate of a Musashi epoch 8
// certifying ranking block. The certificate signs the hash of the block that
// announced the certified endorser block, which is the certifying block's
// parent.
type musashiEpoch8Certificate struct {
	announcingRbHash    string
	signers             string
	aggregatedSignature string
}

var musashiEpoch8Certificates = []musashiEpoch8Certificate{
	{
		announcingRbHash: "65e760407055964f0087b66e55eb7e25" +
			"a22910eb6af06d054e89f342b2d3f8ac",
		signers: "e0",
		aggregatedSignature: "afbfc37dd61e2f5fa6a3554ab172a273f2fd22cd553ac278" +
			"fe7e6a63f5032d6b61432217bf110a860f16458a8ccf4a95",
	},
	{
		announcingRbHash: "9b9efef02c5c44a4433b91321927c6fa" +
			"4e7ae0c6bdcec86e994ad36f9ac56fab",
		signers: "e0",
		aggregatedSignature: "a1e3fcc6a55c9ef54e304f9bb910904866fc237217d14391" +
			"0b3001d0444af6c54fa2ffda6792439385b580d57eef0aff",
	},
}

func musashiEpoch7KeyMap(t *testing.T) map[string]*lcommon.LeiosKey {
	t.Helper()
	keys := make(map[string]*lcommon.LeiosKey, len(musashiEpoch7LeiosKeys))
	for pool, encoded := range musashiEpoch7LeiosKeys {
		publicKey, err := hex.DecodeString(encoded[0])
		require.NoError(t, err)
		proof, err := hex.DecodeString(encoded[1])
		require.NoError(t, err)
		keys[pool] = &lcommon.LeiosKey{
			PublicKey:       publicKey,
			PossessionProof: proof,
		}
	}
	return keys
}

func newMusashiEpoch8VoteManager(
	t *testing.T,
	keys map[string]*lcommon.LeiosKey,
) *VoteManager {
	t.Helper()
	mgr, err := NewVoteManager(VoteManagerConfig{
		EventBus: event.NewEventBus(nil, nil),
		StakeProvider: &fakeStakeProvider{
			pools: maps.Clone(musashiEpoch7PoolStakes),
			total: musashiEpoch7TotalStake,
		},
		EpochProvider: &fakeEpochProvider{currentEpoch: 8},
		ParamsProvider: &fakeParamsProvider{
			committeeSize: 900,
			tau:           big.NewRat(3, 4),
		},
		KeyProvider: &fakeLeiosKeyProvider{keys: keys},
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Start(context.Background()))
	t.Cleanup(func() {
		_ = mgr.Stop()
	})
	return mgr
}

func (c musashiEpoch8Certificate) decode(
	t *testing.T,
) (lcommon.Blake2b256, []byte, []byte) {
	t.Helper()
	hashBytes, err := hex.DecodeString(c.announcingRbHash)
	require.NoError(t, err)
	signers, err := hex.DecodeString(c.signers)
	require.NoError(t, err)
	signature, err := hex.DecodeString(c.aggregatedSignature)
	require.NoError(t, err)
	return lcommon.NewBlake2b256(hashBytes), signers, signature
}

// TestValidateDijkstraCertificateMusashiVector verifies real Musashi
// certificates through committee selection from the stake snapshot, voter-ID
// mapping, on-chain key resolution with proof of possession, the stake
// quorum, and the aggregate BLS signature. A mismatch in any of them rejects
// every certifying block on Musashi. Both certificates set all three seats,
// so they do not distinguish the order of voter IDs among those seats.
func TestValidateDijkstraCertificateMusashiVector(t *testing.T) {
	t.Parallel()
	mgr := newMusashiEpoch8VoteManager(t, musashiEpoch7KeyMap(t))
	for _, cert := range musashiEpoch8Certificates {
		hash, signers, signature := cert.decode(t)
		require.NoError(t, mgr.ValidateDijkstraCertificate(
			8,
			signers,
			signature,
			PrototypeVoteMessageBytes(hash),
		), "certificate for announcing block %s", cert.announcingRbHash)
	}

	// Each certificate binds its own announcing block.
	firstHash, _, _ := musashiEpoch8Certificates[0].decode(t)
	_, signers, signature := musashiEpoch8Certificates[1].decode(t)
	require.ErrorIs(t, mgr.ValidateDijkstraCertificate(
		8,
		signers,
		signature,
		PrototypeVoteMessageBytes(firstHash),
	), ErrInvalidCertificate)
}

func TestValidateDijkstraCertificateMusashiVectorRejectsMissingSignerKey(
	t *testing.T,
) {
	t.Parallel()
	keys := musashiEpoch7KeyMap(t)
	delete(keys, "8a04e2c7db3dff509c9f5f2b8e27d715eb8f3d0665deddc2e436f414")
	mgr := newMusashiEpoch8VoteManager(t, keys)
	hash, signers, signature := musashiEpoch8Certificates[0].decode(t)
	require.ErrorIs(t, mgr.ValidateDijkstraCertificate(
		8,
		signers,
		signature,
		PrototypeVoteMessageBytes(hash),
	), ErrInvalidCertificate)
}

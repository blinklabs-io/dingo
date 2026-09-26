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

package byronupdate

import (
	"crypto/ed25519"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

const testMagic = 764824073

// testConfig is a small chain: k = 10, so 100-slot epochs, and seven
// genesis keys, so the mainnet softfork minimum of 0.6 needs four.
var testConfig = Config{ProtocolMagic: testMagic, K: 10, NumGenesisKeys: 7}

// delegate is one genesis key's active block-signing delegate.
type delegate struct {
	private ed25519.PrivateKey
	xpub    []byte
	keyHash KeyHash
	genesis KeyHash
}

func newDelegate(t *testing.T, seed byte) delegate {
	t.Helper()
	raw := make([]byte, ed25519.SeedSize)
	for i := range raw {
		raw[i] = seed
	}
	private := ed25519.NewKeyFromSeed(raw)
	public, ok := private.Public().(ed25519.PublicKey)
	require.True(t, ok)
	xpub := append(append([]byte{}, public...), make([]byte, 32)...)
	keyHash, err := byronconsensus.PBFTVerificationKeyHash(xpub)
	require.NoError(t, err)
	var genesis KeyHash
	genesis[0] = seed
	genesis[1] = 0x47
	return delegate{
		private: private,
		xpub:    xpub,
		keyHash: keyHash,
		genesis: genesis,
	}
}

func newDelegates(t *testing.T, n int) []delegate {
	t.Helper()
	ret := make([]delegate, n)
	for i := range ret {
		ret[i] = newDelegate(t, byte(0x10+i))
	}
	return ret
}

func testEnv(delegates []delegate) Environment {
	env := Environment{
		Config:            testConfig,
		DelegateToGenesis: make(map[KeyHash]KeyHash),
	}
	for _, d := range delegates {
		env.DelegateToGenesis[d.keyHash] = d.genesis
	}
	return env
}

func testGenesisParams() *eras.ByronProtocolParameters {
	return &eras.ByronProtocolParameters{
		SlotDuration:      big.NewInt(20_000),
		MaxBlockSize:      big.NewInt(2_000_000),
		MaxHeaderSize:     big.NewInt(2_000_000),
		MaxTxSize:         big.NewInt(4_096),
		MaxProposalSize:   big.NewInt(700),
		MpcThd:            20_000_000_000_000,
		HeavyDelThd:       300_000_000_000,
		UpdateVoteThd:     1_000_000_000_000,
		UpdateProposalThd: 100_000_000_000_000,
		UpdateProposalTTL: 50,
		SoftforkRule: eras.ByronSoftforkRule{
			InitThd:      900_000_000_000_000,
			MinThd:       600_000_000_000_000,
			ThdDecrement: 50_000_000_000_000,
		},
		TxFeeSummand:        155_381,
		TxFeeMultiplierNano: big.NewInt(43_946_000_000),
		UnlockStakeEpoch:    18446744073709551615,
	}
}

// proposalSpec describes an update proposal to sign and decode.
type proposalSpec struct {
	version  ProtocolVersion
	maxBlock *big.Int
	maxTx    *big.Int
	script   *uint16
	appName  string
	appVer   uint32
	// padding lengthens the metadata so the proposal can exceed
	// maxProposalSize.
	padding int
}

func optional(value any) []any {
	if value == nil {
		return []any{}
	}
	return []any{value}
}

func (spec proposalSpec) build(
	t *testing.T,
	issuer delegate,
	corrupt bool,
) *byron.ByronUpdateProposal {
	t.Helper()
	var maxBlock, maxTx, script any
	if spec.maxBlock != nil {
		maxBlock = spec.maxBlock
	}
	if spec.maxTx != nil {
		maxTx = spec.maxTx
	}
	if spec.script != nil {
		script = *spec.script
	}
	mod := []any{
		optional(script), []any{}, optional(maxBlock), []any{},
		optional(maxTx), []any{}, []any{}, []any{}, []any{}, []any{},
		[]any{}, []any{}, []any{}, []any{},
	}
	metadata := map[string]any{}
	if spec.padding > 0 {
		metadata["linux"] = []any{
			[]byte{}, make([]byte, 32), []byte{}, make([]byte, spec.padding),
		}
	}
	appName := spec.appName
	if appName == "" {
		appName = "csl-daedalus"
	}
	fields := make([]cbor.RawMessage, 0, 5)
	for _, value := range []any{
		[]any{spec.version.Major, spec.version.Minor, spec.version.Alt},
		mod,
		[]any{appName, spec.appVer},
		metadata,
		map[uint64]any{},
	} {
		encoded, err := cbor.Encode(value)
		require.NoError(t, err)
		fields = append(fields, encoded)
	}
	signedBody := []byte{0x85}
	for _, field := range fields {
		signedBody = append(signedBody, field...)
	}
	signature := ed25519.Sign(
		issuer.private,
		byronSigned(t, byron.SignTagUSProposal, signedBody),
	)
	if corrupt {
		signature[0] ^= 0xff
	}
	raw, err := cbor.Encode([]any{
		cbor.RawMessage(fields[0]), cbor.RawMessage(fields[1]),
		cbor.RawMessage(fields[2]), cbor.RawMessage(fields[3]),
		cbor.RawMessage(fields[4]), issuer.xpub, signature,
	})
	require.NoError(t, err)
	var proposal byron.ByronUpdateProposal
	_, err = cbor.Decode(raw, &proposal)
	require.NoError(t, err)
	return &proposal
}

func byronSigned(t *testing.T, tag byte, body []byte) []byte {
	t.Helper()
	magic, err := cbor.Encode(uint32(testMagic))
	require.NoError(t, err)
	return append(append([]byte{tag}, magic...), body...)
}

func upIdOf(proposal *byron.ByronUpdateProposal) UpId {
	return lcommon.Blake2b256Hash(proposal.Cbor())
}

func newVote(
	t *testing.T,
	voter delegate,
	upId UpId,
	corrupt bool,
) *byron.UpdateVote {
	t.Helper()
	idCbor, err := cbor.Encode(upId.Bytes())
	require.NoError(t, err)
	inner := append(append([]byte{0x82}, idCbor...), 0xf5)
	signature := ed25519.Sign(
		voter.private,
		byronSigned(t, byron.SignTagUSVote, inner),
	)
	if corrupt {
		signature[0] ^= 0xff
	}
	raw, err := cbor.Encode([]any{
		voter.xpub, cbor.RawMessage(idCbor), true, signature,
	})
	require.NoError(t, err)
	vote, err := byron.ParseUpdateVote(raw)
	require.NoError(t, err)
	return vote
}

// apply runs Apply for a block at slot issued by issuer, endorsing version.
func apply(
	t *testing.T,
	state State,
	env Environment,
	slot uint64,
	issuer delegate,
	version ProtocolVersion,
	proposal *byron.ByronUpdateProposal,
	votes ...*byron.UpdateVote,
) (State, error) {
	t.Helper()
	next := state.Tick(env.Config, slot)
	return next.Apply(env, Block{
		Slot:          slot,
		BlockNo:       slot,
		Proposal:      proposal,
		Votes:         votes,
		Version:       version,
		IssuerKeyHash: issuer.keyHash,
	})
}

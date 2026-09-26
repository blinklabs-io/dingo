// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"crypto/ed25519"
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type byronUpdateTestDelegate struct {
	verificationKey []byte
	privateKey      ed25519.PrivateKey
	genesisHash     common.Blake2b224
	delegateHash    common.Blake2b224
}

func newByronUpdateTestDelegate(t *testing.T) byronUpdateTestDelegate {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	verificationKey := append(append([]byte(nil), publicKey...), make([]byte, 32)...)
	delegateHash, err := byronconsensus.PBFTVerificationKeyHash(verificationKey)
	require.NoError(t, err)
	genesisHash := common.Blake2b224Hash(publicKey)
	return byronUpdateTestDelegate{
		verificationKey: verificationKey,
		privateKey:      privateKey,
		genesisHash:     genesisHash,
		delegateHash:    delegateHash,
	}
}

func encodeByronTestArray(fields ...[]byte) []byte {
	ret := []byte{byte(0x80 + len(fields))}
	for _, field := range fields {
		ret = append(ret, field...)
	}
	return ret
}

func makeByronUpdateTestProposal(
	t *testing.T,
	protocolMagic uint32,
	delegate byronUpdateTestDelegate,
) byron.ByronUpdateProposal {
	t.Helper()
	version, err := cbor.Encode(byron.ByronBlockVersion{Major: 0, Minor: 1})
	require.NoError(t, err)
	modFields := make([]any, 14)
	for index := range modFields {
		modFields[index] = []any{}
	}
	modFields[2] = []any{uint64(1_500_000)}
	mod, err := cbor.Encode(modFields)
	require.NoError(t, err)
	software, err := cbor.Encode(byron.ByronSoftwareVersion{
		Name: "cardano-sl", Version: 1,
	})
	require.NoError(t, err)
	metadata := []byte{0xa0}
	attributes := []byte{0xa0}
	signedBody := encodeByronTestArray(version, mod, software, metadata, attributes)
	magic, err := cbor.Encode(protocolMagic)
	require.NoError(t, err)
	signed := append([]byte{byron.SignTagUSProposal}, magic...)
	signed = append(signed, signedBody...)
	signature := ed25519.Sign(delegate.privateKey, signed)
	verificationKey, err := cbor.Encode(delegate.verificationKey)
	require.NoError(t, err)
	signatureCBOR, err := cbor.Encode(signature)
	require.NoError(t, err)
	proposalRaw := encodeByronTestArray(
		version, mod, software, metadata, attributes, verificationKey, signatureCBOR,
	)
	var proposal byron.ByronUpdateProposal
	_, err = cbor.Decode(proposalRaw, &proposal)
	require.NoError(t, err)
	require.NoError(t, proposal.Validate(protocolMagic))
	return proposal
}

func makeByronUpdateTestVote(
	t *testing.T,
	protocolMagic uint32,
	proposalID byronUpdateProposalID,
	delegate byronUpdateTestDelegate,
) cbor.RawMessage {
	t.Helper()
	verificationKey, err := cbor.Encode(delegate.verificationKey)
	require.NoError(t, err)
	proposalIDCBOR, err := cbor.Encode(proposalID[:])
	require.NoError(t, err)
	magic, err := cbor.Encode(protocolMagic)
	require.NoError(t, err)
	signed := append([]byte{byron.SignTagUSVote}, magic...)
	signed = append(signed, 0x82)
	signed = append(signed, proposalIDCBOR...)
	signed = append(signed, 0xf5)
	signature := ed25519.Sign(delegate.privateKey, signed)
	signatureCBOR, err := cbor.Encode(signature)
	require.NoError(t, err)
	return encodeByronTestArray(
		verificationKey, proposalIDCBOR, []byte{0xf5}, signatureCBOR,
	)
}

func makeByronUpdateTestMainBlock(
	t *testing.T,
	protocolMagic uint32,
	proposal byron.ByronUpdateProposal,
	votes ...cbor.RawMessage,
) *byron.ByronMainBlock {
	t.Helper()
	proposalList := encodeByronTestArray(proposal.Cbor())
	voteList := []byte{0x9f}
	for _, vote := range votes {
		voteList = append(voteList, vote...)
	}
	voteList = append(voteList, 0xff)
	updatePayload := encodeByronTestArray(proposalList, voteList)
	sscPayload, err := cbor.Encode([]any{uint64(3), []any{}})
	require.NoError(t, err)
	bodyRaw := encodeByronTestArray(
		[]byte{0x9f, 0xff},
		sscPayload,
		[]byte{0x9f, 0xff},
		updatePayload,
	)
	var body byron.ByronMainBlockBody
	_, err = cbor.Decode(bodyRaw, &body)
	require.NoError(t, err)
	return &byron.ByronMainBlock{
		BlockHeader: &byron.ByronMainBlockHeader{ProtocolMagic: protocolMagic},
		Body:        body,
	}
}

func TestApplyByronBlockVersionMod(t *testing.T) {
	t.Parallel()

	current := byron.ByronGenesisBlockVersionData{
		ScriptVersion:     1,
		SlotDuration:      20,
		MaxBlockSize:      2_000_000,
		MaxHeaderSize:     2_000,
		MaxTxSize:         4_000,
		MaxProposalSize:   1_000,
		MpcThd:            100,
		HeavyDelThd:       200,
		UpdateVoteThd:     300,
		UpdateProposalThd: 400,
		UpdateImplicit:    5,
		SoftforkRule: byron.ByronGenesisBlockVersionDataSoftforkRule{
			InitThd:      10,
			MinThd:       20,
			ThdDecrement: 30,
		},
		TxFeePolicy: byron.ByronGenesisBlockVersionDataTxFeePolicy{
			Summand: 1, Multiplier: 2,
		},
		UnlockStakeEpoch: 7,
	}
	mod := byron.ByronUpdateProposalBlockVersionMod{
		ScriptVersion:     []uint16{2},
		SlotDuration:      []*big.Int{big.NewInt(10)},
		MaxBlockSize:      []*big.Int{big.NewInt(3_000_000)},
		MaxHeaderSize:     []*big.Int{big.NewInt(3_000)},
		MaxTxSize:         []*big.Int{big.NewInt(5_000)},
		MaxProposalSize:   []*big.Int{big.NewInt(2_000)},
		MpcThd:            []byron.ByronLovelacePortion{11},
		HeavyDelThd:       []byron.ByronLovelacePortion{21},
		UpdateVoteThd:     []byron.ByronLovelacePortion{31},
		UpdateProposalThd: []byron.ByronLovelacePortion{41},
		UpdateImplicit:    []uint64{6},
		SoftForkRule: []byron.ByronSoftForkRule{{
			InitThreshold:      12,
			MinThreshold:       22,
			ThresholdDecrement: 32,
		}},
		TxFeePolicy: []byron.ByronTxFeePolicy{{
			SummandNano:    big.NewInt(1_500_000_000),
			MultiplierNano: big.NewInt(2_500_000_000),
		}},
		UnlockStakeEpoch: []uint64{8},
	}

	updated, err := applyByronBlockVersionMod(current, mod)
	require.NoError(t, err)
	require.Equal(t, 2, updated.ScriptVersion)
	require.Equal(t, 10, updated.SlotDuration)
	require.Equal(t, 3_000_000, updated.MaxBlockSize)
	require.Equal(t, 3_000, updated.MaxHeaderSize)
	require.Equal(t, 5_000, updated.MaxTxSize)
	require.Equal(t, 2_000, updated.MaxProposalSize)
	require.Equal(t, int64(11), updated.MpcThd)
	require.Equal(t, int64(21), updated.HeavyDelThd)
	require.Equal(t, int64(31), updated.UpdateVoteThd)
	require.Equal(t, int64(41), updated.UpdateProposalThd)
	require.Equal(t, 6, updated.UpdateImplicit)
	require.Equal(t, byron.ByronGenesisBlockVersionDataSoftforkRule{
		InitThd: 12, MinThd: 22, ThdDecrement: 32,
	}, updated.SoftforkRule)
	require.Equal(t, byron.ByronGenesisBlockVersionDataTxFeePolicy{
		Summand: 2, Multiplier: 2,
	}, updated.TxFeePolicy)
	require.Equal(t, uint64(8), updated.UnlockStakeEpoch)
	require.Equal(t, 2_000_000, current.MaxBlockSize)
}

func TestApplyByronBlockVersionModRejectsUnrepresentableValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mod  byron.ByronUpdateProposalBlockVersionMod
	}{
		{
			name: "nil size parameter",
			mod:  byron.ByronUpdateProposalBlockVersionMod{MaxBlockSize: []*big.Int{nil}},
		},
		{
			name: "size parameter overflows int",
			mod: byron.ByronUpdateProposalBlockVersionMod{
				MaxBlockSize: []*big.Int{new(big.Int).Lsh(big.NewInt(1), 100)},
			},
		},
		{
			name: "negative size parameter",
			mod:  byron.ByronUpdateProposalBlockVersionMod{MaxTxSize: []*big.Int{big.NewInt(-1)}},
		},
		{
			name: "softfork threshold overflows int64",
			mod: byron.ByronUpdateProposalBlockVersionMod{
				SoftForkRule: []byron.ByronSoftForkRule{{InitThreshold: byron.ByronLovelacePortion(^uint64(0))}},
			},
		},
		{
			name: "fee policy missing coefficient",
			mod:  byron.ByronUpdateProposalBlockVersionMod{TxFeePolicy: []byron.ByronTxFeePolicy{{}}},
		},
		{
			name: "negative fee coefficient",
			mod: byron.ByronUpdateProposalBlockVersionMod{
				TxFeePolicy: []byron.ByronTxFeePolicy{{
					SummandNano: big.NewInt(-1), MultiplierNano: big.NewInt(0),
				}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := applyByronBlockVersionMod(
				byron.ByronGenesisBlockVersionData{}, tc.mod,
			)
			require.Error(t, err)
		})
	}
}

func TestRoundByronNanoToInteger(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		value int64
		want  int64
	}{
		{value: 1_499_999_999, want: 1},
		{value: 1_500_000_000, want: 2},
		{value: 2_500_000_000, want: 2},
		{value: 3_500_000_000, want: 4},
	} {
		require.Equal(t, big.NewInt(tc.want), roundByronNanoToInteger(big.NewInt(tc.value)))
	}
}

func TestLedgerViewByronFeePolicyUsesBlockParameters(t *testing.T) {
	t.Parallel()

	params := &byron.ByronGenesisBlockVersionData{
		TxFeePolicy: byron.ByronGenesisBlockVersionDataTxFeePolicy{
			Summand: 17, Multiplier: 23,
		},
	}
	view := &LedgerView{byronParams: params}
	summand, multiplier, err := view.ByronFeePolicy()
	require.NoError(t, err)
	require.Equal(t, int64(17), summand)
	require.Equal(t, int64(23), multiplier)
}

func TestByronUpdateStateRegistersVotesAndAdoptsProtocolUpdate(t *testing.T) {
	t.Parallel()
	const protocolMagic = 42
	delegates := []byronUpdateTestDelegate{
		newByronUpdateTestDelegate(t),
		newByronUpdateTestDelegate(t),
		newByronUpdateTestDelegate(t),
	}
	delegations := make(map[common.Blake2b224]common.Blake2b224, len(delegates))
	for _, delegate := range delegates {
		delegations[delegate.genesisHash] = delegate.delegateHash
	}
	params := byron.ByronGenesisBlockVersionData{
		MaxBlockSize:      1_000_000,
		MaxHeaderSize:     10_000,
		MaxProposalSize:   10_000,
		MaxTxSize:         100_000,
		UpdateVoteThd:     666_666_666_666_667,
		UpdateProposalThd: 666_666_666_666_667,
	}
	state := newByronUpdateState(params, len(delegates))
	proposal := makeByronUpdateTestProposal(t, protocolMagic, delegates[0])
	proposalID := byronUpdateProposalID(common.Blake2b256Hash(proposal.Cbor()))
	block := makeByronUpdateTestMainBlock(
		t,
		protocolMagic,
		proposal,
		makeByronUpdateTestVote(t, protocolMagic, proposalID, delegates[0]),
		makeByronUpdateTestVote(t, protocolMagic, proposalID, delegates[1]),
	)
	state, err := state.applyUpdatePayload(block, delegations, protocolMagic, 12)
	require.NoError(t, err)
	require.Contains(t, state.proposals, proposalID)
	confirmedAt, confirmed := state.confirmed[proposalID]
	require.True(t, confirmed)
	require.Equal(t, uint64(12), confirmedAt)
	require.Equal(t, uint32(1), state.applications["cardano-sl"])
	_, err = state.registerVote(
		mustParseByronUpdateTestVote(t, protocolMagic, proposalID, delegates[0]),
		delegations,
		13,
	)
	require.ErrorContains(t, err, "voted more than once")
	state, err = state.registerEndorsement(
		byron.ByronBlockVersion{Major: 9},
		delegates[0].delegateHash,
		delegations,
		13,
		2,
	)
	require.NoError(t, err)
	require.Empty(t, state.endorsements)

	version := proposal.BlockVersion
	state, err = state.registerEndorsement(
		version,
		delegates[0].delegateHash,
		delegations,
		14,
		2,
	)
	require.NoError(t, err)
	require.Empty(t, state.candidates)
	require.Len(t, state.endorsements[version], 1)
	premature, err := state.registerEndorsement(
		version,
		delegates[1].delegateHash,
		delegations,
		15,
		2,
	)
	require.ErrorContains(t, err, "not confirmed and stable")
	require.Empty(t, premature.candidates)
	state, err = state.registerEndorsement(
		version,
		delegates[1].delegateHash,
		delegations,
		16,
		2,
	)
	require.NoError(t, err)
	require.Len(t, state.candidates, 1)

	state, err = state.advanceEpoch(1, 23, 2)
	require.NoError(t, err)
	require.Equal(t, uint16(0), state.protocolVersion.Major)
	state, err = state.advanceEpoch(2, 24, 2)
	require.NoError(t, err)
	require.Equal(t, version, state.protocolVersion)
	require.Equal(t, 1_500_000, state.params.MaxBlockSize)
	require.Empty(t, state.proposals)
}

func mustParseByronUpdateTestVote(
	t *testing.T,
	protocolMagic uint32,
	proposalID byronUpdateProposalID,
	delegate byronUpdateTestDelegate,
) *byron.UpdateVote {
	t.Helper()
	vote, err := byron.ParseUpdateVote(makeByronUpdateTestVote(
		t, protocolMagic, proposalID, delegate,
	))
	require.NoError(t, err)
	require.NoError(t, vote.Verify(protocolMagic))
	return vote
}

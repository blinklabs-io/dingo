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
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const byronUpdateTestMagic = 42

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
	verificationKey := append(
		append([]byte(nil), publicKey...),
		make([]byte, 32)...,
	)
	delegateHash, err := byronconsensus.PBFTVerificationKeyHash(verificationKey)
	require.NoError(t, err)
	return byronUpdateTestDelegate{
		verificationKey: verificationKey,
		privateKey:      privateKey,
		genesisHash:     common.Blake2b224Hash(publicKey),
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

// byronUpdateTestProposal describes an update proposal; mod holds the 14
// optional ProtocolParametersUpdate fields, each an empty or singleton list.
type byronUpdateTestProposal struct {
	version  byron.ByronBlockVersion
	mod      []any
	software byron.ByronSoftwareVersion
	// signer, when set, signs in place of the proposing delegate.
	signer *byronUpdateTestDelegate
}

func emptyByronUpdateTestMod() []any {
	mod := make([]any, 14)
	for index := range mod {
		mod[index] = []any{}
	}
	return mod
}

func byronUpdateTestFeePolicy(
	t *testing.T,
	summandNano uint64,
	multiplierNano uint64,
) any {
	t.Helper()
	coefficients, err := cbor.Encode([]any{summandNano, multiplierNano})
	require.NoError(t, err)
	return []any{[]any{uint64(0), cbor.WrappedCbor(coefficients)}}
}

func makeByronUpdateTestProposal(
	t *testing.T,
	delegate byronUpdateTestDelegate,
	spec byronUpdateTestProposal,
) byron.ByronUpdateProposal {
	t.Helper()
	if spec.mod == nil {
		spec.mod = emptyByronUpdateTestMod()
	}
	version, err := cbor.Encode(spec.version)
	require.NoError(t, err)
	mod, err := cbor.Encode(spec.mod)
	require.NoError(t, err)
	software, err := cbor.Encode(spec.software)
	require.NoError(t, err)
	metadata := []byte{0xa0}
	attributes := []byte{0xa0}
	signedBody := encodeByronTestArray(version, mod, software, metadata, attributes)
	magic, err := cbor.Encode(uint32(byronUpdateTestMagic))
	require.NoError(t, err)
	signed := append([]byte{byron.SignTagUSProposal}, magic...)
	signed = append(signed, signedBody...)
	signer := delegate
	if spec.signer != nil {
		signer = *spec.signer
	}
	signature := ed25519.Sign(signer.privateKey, signed)
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
	if spec.signer == nil {
		require.NoError(t, proposal.Validate(byronUpdateTestMagic))
	}
	return proposal
}

func byronUpdateTestProposalID(
	proposal byron.ByronUpdateProposal,
) byronUpdateProposalID {
	return byronUpdateProposalID(common.Blake2b256Hash(proposal.Cbor()))
}

func makeByronUpdateTestVote(
	t *testing.T,
	proposalID byronUpdateProposalID,
	delegate byronUpdateTestDelegate,
) *byron.UpdateVote {
	t.Helper()
	vote := makeByronUpdateTestVoteSignedBy(t, proposalID, delegate, delegate)
	require.NoError(t, vote.Verify(byronUpdateTestMagic))
	return vote
}

func makeByronUpdateTestVoteSignedBy(
	t *testing.T,
	proposalID byronUpdateProposalID,
	delegate byronUpdateTestDelegate,
	signer byronUpdateTestDelegate,
) *byron.UpdateVote {
	t.Helper()
	verificationKey, err := cbor.Encode(delegate.verificationKey)
	require.NoError(t, err)
	proposalIDCBOR, err := cbor.Encode(proposalID[:])
	require.NoError(t, err)
	magic, err := cbor.Encode(uint32(byronUpdateTestMagic))
	require.NoError(t, err)
	signed := append([]byte{byron.SignTagUSVote}, magic...)
	signed = append(signed, 0x82)
	signed = append(signed, proposalIDCBOR...)
	signed = append(signed, 0xf5)
	signature := ed25519.Sign(signer.privateKey, signed)
	signatureCBOR, err := cbor.Encode(signature)
	require.NoError(t, err)
	vote, err := byron.ParseUpdateVote(encodeByronTestArray(
		verificationKey, proposalIDCBOR, []byte{0xf5}, signatureCBOR,
	))
	require.NoError(t, err)
	return vote
}

// byronUpdateTestNetwork is a seven-key genesis with mainnet's softfork rule,
// so the adoption threshold is floor(0.6 * 7) = 4, while updateVoteThd and
// updateProposalThd would each give 0.
type byronUpdateTestNetwork struct {
	delegates   []byronUpdateTestDelegate
	delegations map[common.Blake2b224]common.Blake2b224
	state       byronUpdateState
}

const byronUpdateTestK = 2

func mainnetByronBlockVersionData() byron.ByronGenesisBlockVersionData {
	return byron.ByronGenesisBlockVersionData{
		HeavyDelThd:       300_000_000_000,
		MaxBlockSize:      2_000_000,
		MaxHeaderSize:     2_000_000,
		MaxProposalSize:   700,
		MaxTxSize:         4096,
		MpcThd:            20_000_000_000_000,
		SlotDuration:      20_000,
		UnlockStakeEpoch:  18446744073709551615,
		UpdateImplicit:    10_000,
		UpdateProposalThd: 100_000_000_000_000,
		UpdateVoteThd:     1_000_000_000_000,
		SoftforkRule: byron.ByronGenesisBlockVersionDataSoftforkRule{
			InitThd:      900_000_000_000_000,
			MinThd:       600_000_000_000_000,
			ThdDecrement: 50_000_000_000_000,
		},
		TxFeePolicy: byron.ByronGenesisBlockVersionDataTxFeePolicy{
			Summand:    155_381_000_000_000,
			Multiplier: 43_946_000_000,
		},
	}
}

func newByronUpdateTestNetwork(
	t *testing.T,
	genesis byron.ByronGenesisBlockVersionData,
) byronUpdateTestNetwork {
	t.Helper()
	network := byronUpdateTestNetwork{
		delegations: make(map[common.Blake2b224]common.Blake2b224),
	}
	for range 7 {
		delegate := newByronUpdateTestDelegate(t)
		network.delegates = append(network.delegates, delegate)
		network.delegations[delegate.genesisHash] = delegate.delegateHash
	}
	state, err := newByronUpdateState(genesis, len(network.delegates))
	require.NoError(t, err)
	require.Equal(t, 4, state.params.adoptionThreshold(state.numGenesisKeys))
	network.state = state
	return network
}

// apply registers one main block's signal. endorser indexes the delegate
// that issued the block.
func (n *byronUpdateTestNetwork) apply(
	t *testing.T,
	slot uint64,
	endorser int,
	endorsement byron.ByronBlockVersion,
	proposal *byron.ByronUpdateProposal,
	votes ...*byron.UpdateVote,
) error {
	t.Helper()
	state, err := n.state.registerUpdate(
		byronUpdateSignal{
			proposal:     proposal,
			votes:        votes,
			endorsement:  endorsement,
			endorserHash: n.delegates[endorser].delegateHash,
		},
		n.delegations,
		byronUpdateTestMagic,
		slot,
		byronUpdateTestK,
	)
	if err == nil {
		n.state = state
	}
	return err
}

func (n *byronUpdateTestNetwork) votes(
	t *testing.T,
	proposal byron.ByronUpdateProposal,
	delegates ...int,
) []*byron.UpdateVote {
	t.Helper()
	proposalID := byronUpdateTestProposalID(proposal)
	votes := make([]*byron.UpdateVote, 0, len(delegates))
	for _, delegate := range delegates {
		votes = append(votes, makeByronUpdateTestVote(t, proposalID, n.delegates[delegate]))
	}
	return votes
}

var byronUpdateTestV1 = byron.ByronBlockVersion{Major: 1}

func TestByronUpdateConfirmationUsesSoftforkMinThreshold(t *testing.T) {
	t.Parallel()

	network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
	proposal := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
		version:  byronUpdateTestV1,
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 1},
	})
	proposalID := byronUpdateTestProposalID(proposal)
	require.NoError(t, network.apply(
		t, 10, 0, byron.ByronBlockVersion{}, &proposal,
		network.votes(t, proposal, 0, 1, 2)...,
	))
	require.NotContains(t, network.state.confirmed, proposalID)
	require.NotContains(t, network.state.applications, "cardano-sl")

	require.NoError(t, network.apply(
		t, 11, 1, byron.ByronBlockVersion{}, nil,
		network.votes(t, proposal, 3)...,
	))
	require.Equal(t, uint64(11), network.state.confirmed[proposalID])
	require.Equal(t, uint32(1), network.state.applications["cardano-sl"])
	require.Empty(t, network.state.softwareProposals)
	require.Contains(t, network.state.protocolProposals, proposalID)

	err := network.apply(
		t, 12, 1, byron.ByronBlockVersion{}, nil,
		network.votes(t, proposal, 3)...,
	)
	require.ErrorContains(t, err, "voted more than once")
}

func TestByronUpdateEndorsementsCountOnlyOnceStable(t *testing.T) {
	t.Parallel()

	network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
	proposal := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
		version:  byronUpdateTestV1,
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 1},
	})
	require.NoError(t, network.apply(
		t, 10, 0, byron.ByronBlockVersion{}, &proposal,
		network.votes(t, proposal, 0, 1, 2, 3)...,
	))

	// Confirmed at slot 10 and stable from 10+2k = 14. Endorsements before
	// that are dropped, even by enough keys to reach the threshold.
	for index := range 4 {
		require.NoError(t, network.apply(t, 13, index, byronUpdateTestV1, nil))
	}
	require.Empty(t, network.state.endorsements)
	require.Empty(t, network.state.candidates)

	stranger := newByronUpdateTestDelegate(t)
	network.delegates = append(network.delegates, stranger)
	require.NoError(t, network.apply(t, 14, len(network.delegates)-1, byronUpdateTestV1, nil))
	require.Empty(t, network.state.endorsements)

	for index := range 3 {
		require.NoError(t, network.apply(t, 14+uint64(index), index, byronUpdateTestV1, nil))
	}
	require.Len(t, network.state.endorsements, 3)
	require.Empty(t, network.state.candidates)
	require.NoError(t, network.apply(t, 20, 3, byronUpdateTestV1, nil))
	require.Len(t, network.state.candidates, 1)
	require.Equal(t, uint64(20), network.state.candidates[0].slot)

	// A candidate at slot 20 is adoptable at an epoch whose first slot is
	// at least 20+4k.
	next, err := network.state.advanceEpoch(1, 27, byronUpdateTestK)
	require.NoError(t, err)
	require.Equal(t, byron.ByronBlockVersion{}, next.protocolVersion)
	next, err = network.state.advanceEpoch(1, 28, byronUpdateTestK)
	require.NoError(t, err)
	require.Equal(t, byronUpdateTestV1, next.protocolVersion)
	require.Empty(t, next.protocolProposals)
	require.Equal(t, uint32(1), next.applications["cardano-sl"])
}

func TestByronUpdateConfirmedSoftwareProposalReleasesApplication(t *testing.T) {
	t.Parallel()

	network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
	first := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
		software: byron.ByronSoftwareVersion{Name: "csl-daedalus", Version: 1},
	})
	require.NoError(t, network.apply(
		t, 10, 0, byron.ByronBlockVersion{}, &first,
		network.votes(t, first, 0, 1, 2, 3)...,
	))
	require.Equal(t, uint32(1), network.state.applications["csl-daedalus"])
	require.Empty(t, network.state.protocolProposals)

	second := makeByronUpdateTestProposal(t, network.delegates[1], byronUpdateTestProposal{
		software: byron.ByronSoftwareVersion{Name: "csl-daedalus", Version: 2},
	})
	require.NoError(t, network.apply(t, 20, 1, byron.ByronBlockVersion{}, &second))
	require.Contains(t, network.state.softwareProposals, byronUpdateTestProposalID(second))

	duplicate := makeByronUpdateTestProposal(t, network.delegates[2], byronUpdateTestProposal{
		version:  byronUpdateTestV1,
		software: byron.ByronSoftwareVersion{Name: "csl-daedalus", Version: 2},
	})
	require.ErrorContains(
		t,
		network.apply(t, 21, 2, byron.ByronBlockVersion{}, &duplicate),
		"already proposed",
	)
}

func TestByronUpdateProtocolOnlyProposalLeavesApplicationFree(t *testing.T) {
	t.Parallel()

	network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
	release := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 1},
	})
	require.NoError(t, network.apply(
		t, 10, 0, byron.ByronBlockVersion{}, &release,
		network.votes(t, release, 0, 1, 2, 3)...,
	))

	protocolOnly := makeByronUpdateTestProposal(t, network.delegates[1], byronUpdateTestProposal{
		version:  byronUpdateTestV1,
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 1},
	})
	require.NoError(t, network.apply(t, 20, 1, byron.ByronBlockVersion{}, &protocolOnly))
	require.Empty(t, network.state.softwareProposals)

	software := makeByronUpdateTestProposal(t, network.delegates[2], byronUpdateTestProposal{
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 2},
	})
	require.NoError(t, network.apply(t, 21, 2, byron.ByronBlockVersion{}, &software))
}

func TestByronUpdateProposalSizeBoundsOnlyProtocolUpdates(t *testing.T) {
	t.Parallel()

	genesis := mainnetByronBlockVersionData()
	genesis.MaxProposalSize = 100
	network := newByronUpdateTestNetwork(t, genesis)
	software := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 1},
	})
	require.Greater(t, len(software.Cbor()), genesis.MaxProposalSize)
	require.NoError(t, network.apply(t, 10, 0, byron.ByronBlockVersion{}, &software))

	protocol := makeByronUpdateTestProposal(t, network.delegates[1], byronUpdateTestProposal{
		version:  byronUpdateTestV1,
		software: byron.ByronSoftwareVersion{Name: "other", Version: 1},
	})
	require.ErrorContains(
		t,
		network.apply(t, 11, 1, byron.ByronBlockVersion{}, &protocol),
		"exceeds maxProposalSize",
	)
}

func TestByronUpdateRejectsNullProposal(t *testing.T) {
	t.Parallel()

	network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
	release := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 1},
	})
	require.NoError(t, network.apply(
		t, 10, 0, byron.ByronBlockVersion{}, &release,
		network.votes(t, release, 0, 1, 2, 3)...,
	))
	mod := emptyByronUpdateTestMod()
	mod[12] = byronUpdateTestFeePolicy(t, 155_381_000_000_000, 43_946_000_000)
	restated := makeByronUpdateTestProposal(t, network.delegates[1], byronUpdateTestProposal{
		mod:      mod,
		software: byron.ByronSoftwareVersion{Name: "cardano-sl", Version: 1},
	})
	require.ErrorContains(
		t,
		network.apply(t, 11, 1, byron.ByronBlockVersion{}, &restated),
		"changes neither protocol nor software version",
	)
}

func TestApplyByronBlockVersionModKeepsNanoFeeScale(t *testing.T) {
	t.Parallel()

	genesis, err := byronProtocolParametersFromGenesis(mainnetByronBlockVersionData())
	require.NoError(t, err)
	summand, multiplier, err := genesis.feePolicyNano()
	require.NoError(t, err)
	require.Equal(t, int64(155_381_000_000_000), summand)
	require.Equal(t, int64(43_946_000_000), multiplier)

	decodeMod := func(mod []any) byron.ByronUpdateProposalBlockVersionMod {
		raw, err := cbor.Encode(mod)
		require.NoError(t, err)
		var decoded byron.ByronUpdateProposalBlockVersionMod
		_, err = cbor.Decode(raw, &decoded)
		require.NoError(t, err)
		return decoded
	}
	for _, tc := range []struct {
		name           string
		summandNano    uint64
		multiplierNano uint64
		wantSummand    int64
	}{
		{"restated mainnet policy", 155_381_000_000_000, 43_946_000_000, 155_381_000_000_000},
		{"summand half rounds to even upward", 155_381_500_000_000, 43_946_000_001, 155_382_000_000_000},
		{"summand half rounds to even downward", 155_382_500_000_000, 1, 155_382_000_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mod := emptyByronUpdateTestMod()
			mod[12] = byronUpdateTestFeePolicy(t, tc.summandNano, tc.multiplierNano)
			updated, err := applyByronBlockVersionMod(genesis, decodeMod(mod))
			require.NoError(t, err)
			summand, multiplier, err := updated.feePolicyNano()
			require.NoError(t, err)
			require.Equal(t, tc.wantSummand, summand)
			require.Equal(t, int64(tc.multiplierNano), multiplier)
		})
	}

	mod := emptyByronUpdateTestMod()
	mod[12] = byronUpdateTestFeePolicy(t, 155_381_000_000_000, 43_946_000_000)
	restated, err := applyByronBlockVersionMod(genesis, decodeMod(mod))
	require.NoError(t, err)
	require.True(t, restated.equal(genesis))
	require.Equal(t, big.NewInt(43_946_000_000), genesis.feeMultiplierNano)
}

func TestByronUpdateThresholdUsesSoftforkRuleOfAdoptedParameters(t *testing.T) {
	t.Parallel()

	genesis := mainnetByronBlockVersionData()
	params, err := byronProtocolParametersFromGenesis(genesis)
	require.NoError(t, err)
	for _, tc := range []struct {
		minThd uint64
		want   int
	}{
		{600_000_000_000_000, 4},
		{571_428_571_428_571, 3},
		{571_428_571_428_572, 4},
		{1_000_000_000_000_000, 7},
		{0, 0},
	} {
		params.softforkMinThd = tc.minThd
		require.Equal(t, tc.want, params.adoptionThreshold(7), "minThd %d", tc.minThd)
	}
}

// TestLedgerProcessBlockByronUsesAdoptedParameters drives Byron block
// application with parameters adopted above genesis: ppMaxTxSize and the fee
// policy of the block's epoch decide, not the genesis values.
func TestLedgerProcessBlockByronUsesAdoptedParameters(t *testing.T) {
	t.Parallel()

	nodeConfig := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, nodeConfig, strings.NewReader(`{
		"blockVersionData": {
			"slotDuration": "20000",
			"maxBlockSize": "2000000",
			"maxHeaderSize": "2000000",
			"maxProposalSize": "700",
			"maxTxSize": "600",
			"txFeePolicy": {"summand": "0", "multiplier": "0"}
		},
		"protocolConsts": {"k": 2160, "protocolMagic": 764824073}
	}`)),
	)
	const protocolMagic = 764824073
	genesis, err := byronProtocolParametersFromGenesis(
		nodeConfig.ByronGenesis().BlockVersionData,
	)
	require.NoError(t, err)
	decodeMod := func(mod []any) byron.ByronUpdateProposalBlockVersionMod {
		raw, err := cbor.Encode(mod)
		require.NoError(t, err)
		var decoded byron.ByronUpdateProposalBlockVersionMod
		_, err = cbor.Decode(raw, &decoded)
		require.NoError(t, err)
		return decoded
	}
	sizeMod := emptyByronUpdateTestMod()
	sizeMod[4] = []any{uint64(4096)}
	largerTx, err := applyByronBlockVersionMod(genesis, decodeMod(sizeMod))
	require.NoError(t, err)
	feeMod := emptyByronUpdateTestMod()
	feeMod[12] = byronUpdateTestFeePolicy(t, 155_381_000_000_000, 43_946_000_000)
	mainnetFee, err := applyByronBlockVersionMod(genesis, decodeMod(feeMod))
	require.NoError(t, err)

	key := newByronBlockTestKey(t, 0x71)
	payTo := newByronBlockTestKey(t, 0x72).address
	largeTx := func(t *testing.T, db *database.Database) *byron.ByronTransaction {
		input := seedByronUtxoWithAmount(t, db, 0x01, key.address, 1_000)
		outputs := make([]byronBlockTestOutput, 0, 20)
		for range 20 {
			outputs = append(outputs, byronBlockTestOutput{payTo, 1})
		}
		tx := buildByronBlockTestTx(t, protocolMagic,
			[]byronBlockTestInput{{input, 0}},
			outputs, nil, []byronBlockTestKey{key})
		require.Greater(t, len(tx.Cbor()), 600)
		return tx
	}

	t.Run("genesis maxTxSize rejects", func(t *testing.T) {
		t.Parallel()
		db := newTestDB(t)
		var tooLarge eras.TxTooLargeByronError
		require.ErrorAs(t, processByronReferenceRuleBlock(
			t, db, nodeConfig, largeTx(t, db), &genesis,
		), &tooLarge)
	})
	t.Run("adopted maxTxSize admits", func(t *testing.T) {
		t.Parallel()
		db := newTestDB(t)
		require.NoError(t, processByronReferenceRuleBlock(
			t, db, nodeConfig, largeTx(t, db), &largerTx,
		))
	})
	t.Run("adopted fee policy keeps its nano scale", func(t *testing.T) {
		t.Parallel()
		db := newTestDB(t)
		input := seedByronUtxoWithAmount(t, db, 0x01, key.address, 1_000_000)
		tx := buildByronBlockTestTx(t, protocolMagic,
			[]byronBlockTestInput{{input, 0}},
			[]byronBlockTestOutput{{payTo, 1_000_000 - 100}},
			nil, []byronBlockTestKey{key})
		err := processByronReferenceRuleBlock(t, db, nodeConfig, tx, &mainnetFee)
		var feeErr eras.FeeTooLowByronError
		require.ErrorAs(t, err, &feeErr)
		size := int64(len(tx.Cbor()))
		want := 155_381 + (43_946*size+999)/1000
		require.Equal(t, big.NewInt(want), feeErr.Required)
		require.NoError(t, processByronReferenceRuleBlock(t, db, nodeConfig, tx, &genesis))
	})
}

// TestLedgerStateByronParametersTickToSlotAfterTip checks the parameters a
// transaction submitted outside a block is validated against: the tip's
// update state ticked to the slot after the tip, as the reference mempool
// ticks its ledger. A candidate stable for the next epoch is already in force
// when the tip is the last slot of the current one.
func TestLedgerStateByronParametersTickToSlotAfterTip(t *testing.T) {
	t.Parallel()

	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preprod/config.json",
	)
	require.NoError(t, err)
	ls := &LedgerState{config: LedgerStateConfig{
		CardanoNodeConfig: nodeConfig,
	}}
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(
		config,
		nodeConfig.ByronGenesis().BlockVersionData,
	)
	require.NoError(t, err)
	adopted := state.updateState.params
	adopted.maxTxSize = big.NewInt(8192)
	adopted.feeSummand = 7
	adopted.feeMultiplierNano = big.NewInt(3)
	state.updateState.candidates = []byronProtocolAdoption{{
		slot:    0,
		version: byron.ByronBlockVersion{Major: 1},
		params:  adopted,
	}}
	for _, tc := range []struct {
		tipSlot   uint64
		maxTxSize uint64
		summand   int64
	}{
		{tipSlot: config.SlotsPerEpoch - 2, maxTxSize: 4096, summand: 155_381_000_000_000},
		{tipSlot: config.SlotsPerEpoch - 1, maxTxSize: 8192, summand: 7_000_000_000},
	} {
		tip := ocommon.Point{Slot: tc.tipSlot, Hash: []byte{0x01}}
		ls.currentTip = ochainsync.Tip{Point: tip, BlockNumber: 1}
		ls.byronPBFT.state = state
		ls.byronPBFT.tip = tip
		ls.byronPBFT.initialized = true
		maxTxSize, err := ls.ByronMaxTxSize()
		require.NoError(t, err)
		require.Equal(t, tc.maxTxSize, maxTxSize, "tip slot %d", tc.tipSlot)
		summand, _, err := ls.ByronFeePolicy()
		require.NoError(t, err)
		require.Equal(t, tc.summand, summand, "tip slot %d", tc.tipSlot)
	}
}

// TestByronUpdateUsesDelegationMapBeforeBlock pins the delegation map the
// update rules read. The reference registers a block's endorsement against
// the delegation state before that block's tick, so a delegate whose
// delegation activates at the block's own slot endorses from the next block.
func TestByronUpdateUsesDelegationMapBeforeBlock(t *testing.T) {
	t.Parallel()

	const (
		protocolMagic = uint32(42)
		securityParam = uint64(100)
	)
	template := loadRealByronMainBlock(t)
	issuer := newByronPBFTTestKey(0x71)
	initialDelegate := newByronPBFTTestKey(0x72)
	replacementDelegate := newByronPBFTTestKey(0x73)
	genesisCertificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, initialDelegate,
	)
	activationCertificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 1, issuer, replacementDelegate,
	)
	ls := &LedgerState{config: LedgerStateConfig{
		CardanoNodeConfig: newGeneratedByronPBFTTestNodeConfig(
			t, protocolMagic, securityParam, issuer, initialDelegate,
			genesisCertificate,
		),
	}}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-50_000*time.Second), time.Second, 1_000,
		),
		DefaultSlotClockConfig(),
	)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(
		config,
		ls.config.CardanoNodeConfig.ByronGenesis().BlockVersionData,
	)
	require.NoError(t, err)

	var origin common.Blake2b256
	schedule := newSignedByronPBFTBlock(
		t, template, protocolMagic, 1, 1, 1, origin,
		issuer, initialDelegate, genesisCertificate,
		[]any{activationCertificate},
	)
	state, err = ls.advanceByronPBFTState(state, schedule, true)
	require.NoError(t, err)

	// A confirmed, stable protocol proposal for the version the blocks
	// endorse, so each endorsement is either recorded or ignored by lookup.
	version := schedule.BlockHeader.ExtraData.BlockVersion
	proposalID := byronUpdateProposalID{0x01}
	state.updateState.protocolProposals[proposalID] = byronProtocolProposal{
		version: version,
		params:  state.updateState.params,
	}
	state.updateState.confirmed[proposalID] = 0
	state.updateState.registeredAt[proposalID] = 0
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(issuer.verificationKey)
	require.NoError(t, err)
	endorsement := byronEndorsement{version: version, genesis: issuerHash}

	activated := newSignedByronPBFTBlock(
		t, template, protocolMagic, 1, 201, 2, schedule.Hash(),
		issuer, replacementDelegate, activationCertificate, nil,
	)
	state, err = ls.advanceByronPBFTState(state, activated, true)
	require.NoError(t, err)
	require.NotContains(t, state.updateState.endorsements, endorsement)

	next := newSignedByronPBFTBlock(
		t, template, protocolMagic, 1, 202, 3, activated.Hash(),
		issuer, replacementDelegate, activationCertificate, nil,
	)
	state, err = ls.advanceByronPBFTState(state, next, true)
	require.NoError(t, err)
	require.Contains(t, state.updateState.endorsements, endorsement)
}

// TestByronUpdateRejectsEachRegistrationPredicate drives one rejection vector
// per Registration and Voting predicate of the reference update interface,
// beside the boundary value each predicate still accepts.
func TestByronUpdateRejectsEachRegistrationPredicate(t *testing.T) {
	t.Parallel()

	sizeMod := func(index int, value uint64) []any {
		mod := emptyByronUpdateTestMod()
		mod[index] = []any{value}
		return mod
	}
	v := func(major, minor uint16) byron.ByronBlockVersion {
		return byron.ByronBlockVersion{Major: major, Minor: minor}
	}
	sw := func(name string, version uint32) byron.ByronSoftwareVersion {
		return byron.ByronSoftwareVersion{Name: name, Version: version}
	}
	type proposalCase struct {
		name    string
		spec    byronUpdateTestProposal
		wantErr string
	}
	for _, tc := range []proposalCase{
		{"next major accepted", byronUpdateTestProposal{version: v(1, 0), software: sw("a", 1)}, ""},
		{"next minor accepted", byronUpdateTestProposal{version: v(0, 1), software: sw("a", 1)}, ""},
		{"major skip rejected", byronUpdateTestProposal{version: v(2, 0), software: sw("a", 1)}, "cannot follow"},
		{"minor skip rejected", byronUpdateTestProposal{version: v(0, 2), software: sw("a", 1)}, "cannot follow"},
		{"major bump with minor rejected", byronUpdateTestProposal{version: v(1, 1), software: sw("a", 1)}, "cannot follow"},
		{"maxBlockSize doubled accepted", byronUpdateTestProposal{version: v(1, 0), mod: sizeMod(2, 4_000_000), software: sw("a", 1)}, ""},
		{"maxBlockSize above double rejected", byronUpdateTestProposal{version: v(1, 0), mod: sizeMod(2, 4_000_001), software: sw("a", 1)}, "exceeds twice"},
		{"maxTxSize equal to maxBlockSize rejected", byronUpdateTestProposal{version: v(1, 0), mod: sizeMod(4, 2_000_000), software: sw("a", 1)}, "must be less than maxBlockSize"},
		{"scriptVersion step accepted", byronUpdateTestProposal{version: v(1, 0), mod: sizeMod(0, 1), software: sw("a", 1)}, ""},
		{"scriptVersion skip rejected", byronUpdateTestProposal{version: v(1, 0), mod: sizeMod(0, 2), software: sw("a", 1)}, "scriptVersion"},
		{"new application at 0 accepted", byronUpdateTestProposal{software: sw("a", 0)}, ""},
		{"new application at 2 rejected", byronUpdateTestProposal{software: sw("a", 2)}, "must be 0 or 1"},
		{"twelve character name accepted", byronUpdateTestProposal{software: sw("abcdefghijkl", 1)}, ""},
		{"thirteen character name rejected", byronUpdateTestProposal{software: sw("abcdefghijklm", 1)}, "too long"},
		{"non-ASCII name rejected", byronUpdateTestProposal{software: sw("caf\u00e9", 1)}, "not ASCII"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
			proposal := makeByronUpdateTestProposal(t, network.delegates[0], tc.spec)
			err := network.apply(t, 10, 0, byron.ByronBlockVersion{}, &proposal)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Contains(t, network.state.registeredAt, byronUpdateTestProposalID(proposal))
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	t.Run("proposer is not a delegate", func(t *testing.T) {
		t.Parallel()
		network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
		proposal := makeByronUpdateTestProposal(t, newByronUpdateTestDelegate(t), byronUpdateTestProposal{
			version: v(1, 0), software: sw("a", 1),
		})
		require.ErrorContains(t, network.apply(t, 10, 0, byron.ByronBlockVersion{}, &proposal), "not a genesis delegate")
	})
	t.Run("proposal signature", func(t *testing.T) {
		t.Parallel()
		network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
		proposal := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
			version: v(1, 0), software: sw("a", 1), signer: &network.delegates[1],
		})
		require.ErrorIs(t, network.apply(t, 10, 0, byron.ByronBlockVersion{}, &proposal), byron.ErrInvalidSignature)
	})
	t.Run("duplicate protocol version", func(t *testing.T) {
		t.Parallel()
		network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
		first := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
			version: v(1, 0), software: sw("a", 1),
		})
		require.NoError(t, network.apply(t, 10, 0, byron.ByronBlockVersion{}, &first))
		second := makeByronUpdateTestProposal(t, network.delegates[1], byronUpdateTestProposal{
			version: v(1, 0), software: sw("b", 1),
		})
		require.ErrorContains(t, network.apply(t, 11, 1, byron.ByronBlockVersion{}, &second), "already proposed")
	})
	t.Run("expired proposal can be proposed again", func(t *testing.T) {
		t.Parallel()
		network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
		first := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
			version: v(1, 0), software: sw("a", 1),
		})
		require.NoError(t, network.apply(t, 10, 0, byron.ByronBlockVersion{}, &first))
		require.NoError(t, network.apply(t, 10+10_000, 0, byron.ByronBlockVersion{}, nil))
		require.Contains(t, network.state.registeredAt, byronUpdateTestProposalID(first))
		require.NoError(t, network.apply(t, 10+10_001, 0, byron.ByronBlockVersion{}, nil))
		require.Empty(t, network.state.registeredAt)
		again := makeByronUpdateTestProposal(t, network.delegates[1], byronUpdateTestProposal{
			version: v(1, 0), software: sw("b", 1),
		})
		require.NoError(t, network.apply(t, 10+10_002, 1, byron.ByronBlockVersion{}, &again))
		require.ErrorContains(t, network.apply(
			t, 10+10_003, 1, byron.ByronBlockVersion{}, nil,
			network.votes(t, first, 0)...,
		), "unregistered proposal")
	})

	voteNetwork := func(t *testing.T) (byronUpdateTestNetwork, byron.ByronUpdateProposal) {
		t.Helper()
		network := newByronUpdateTestNetwork(t, mainnetByronBlockVersionData())
		proposal := makeByronUpdateTestProposal(t, network.delegates[0], byronUpdateTestProposal{
			version: v(1, 0), software: sw("a", 1),
		})
		require.NoError(t, network.apply(t, 10, 0, byron.ByronBlockVersion{}, &proposal))
		return network, proposal
	}
	t.Run("vote for unregistered proposal", func(t *testing.T) {
		t.Parallel()
		network, _ := voteNetwork(t)
		vote := makeByronUpdateTestVote(t, byronUpdateProposalID{0xaa}, network.delegates[1])
		require.ErrorContains(t, network.apply(t, 11, 1, byron.ByronBlockVersion{}, nil, vote), "unregistered proposal")
	})
	t.Run("voter is not a delegate", func(t *testing.T) {
		t.Parallel()
		network, proposal := voteNetwork(t)
		vote := makeByronUpdateTestVote(t, byronUpdateTestProposalID(proposal), newByronUpdateTestDelegate(t))
		require.ErrorContains(t, network.apply(t, 11, 1, byron.ByronBlockVersion{}, nil, vote), "not a genesis delegate")
	})
	t.Run("vote signature", func(t *testing.T) {
		t.Parallel()
		network, proposal := voteNetwork(t)
		vote := makeByronUpdateTestVoteSignedBy(
			t, byronUpdateTestProposalID(proposal), network.delegates[1], network.delegates[2],
		)
		require.ErrorIs(t, network.apply(t, 11, 1, byron.ByronBlockVersion{}, nil, vote), byron.ErrInvalidSignature)
	})
}

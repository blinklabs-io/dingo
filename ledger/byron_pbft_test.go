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
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type byronPBFTTestKey struct {
	verificationKey []byte
	privateKey      ed25519.PrivateKey
}

func newByronPBFTTestKey(seedByte byte) byronPBFTTestKey {
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat(
		[]byte{seedByte},
		ed25519.SeedSize,
	))
	verificationKey := make([]byte, 64)
	copy(verificationKey, privateKey.Public().(ed25519.PublicKey))
	copy(verificationKey[32:], bytes.Repeat([]byte{seedByte ^ 0xff}, 32))
	return byronPBFTTestKey{
		verificationKey: verificationKey,
		privateKey:      privateKey,
	}
}

func newSignedByronPBFTDelegationCertificate(
	t *testing.T,
	protocolMagic uint32,
	epoch uint64,
	issuer byronPBFTTestKey,
	delegate byronPBFTTestKey,
) []any {
	t.Helper()
	epochCbor, err := cbor.Encode(epoch)
	require.NoError(t, err)
	inner := make([]byte, 0, 2+len(delegate.verificationKey)+len(epochCbor))
	inner = append(inner, '0', '0')
	inner = append(inner, delegate.verificationKey...)
	inner = append(inner, epochCbor...)
	innerCbor, err := cbor.Encode(inner)
	require.NoError(t, err)
	protocolMagicCbor, err := cbor.Encode(protocolMagic)
	require.NoError(t, err)
	signed := []byte{0x0a} // Byron SignCertificate tag.
	signed = append(signed, protocolMagicCbor...)
	signed = append(signed, innerCbor...)
	return []any{
		epoch,
		append([]byte(nil), issuer.verificationKey...),
		append([]byte(nil), delegate.verificationKey...),
		ed25519.Sign(issuer.privateKey, signed),
	}
}

func newSignedByronPBFTBlock(
	t *testing.T,
	template models.Block,
	protocolMagic uint32,
	epoch uint64,
	slot uint16,
	difficulty uint64,
	previousHash lcommon.Blake2b256,
	issuer byronPBFTTestKey,
	delegate byronPBFTTestKey,
	proxyCertificate []any,
	delegationPayload []any,
) *byron.ByronMainBlock {
	t.Helper()
	decoded, err := template.Decode()
	require.NoError(t, err)
	block, ok := decoded.(*byron.ByronMainBlock)
	require.True(t, ok)
	header := block.BlockHeader
	header.ProtocolMagic = protocolMagic
	header.PrevBlock = previousHash
	header.ConsensusData.SlotId.Epoch = epoch
	header.ConsensusData.SlotId.Slot = slot
	header.ConsensusData.PubKey = append(
		[]byte(nil),
		issuer.verificationKey...,
	)
	header.ConsensusData.Difficulty.Value = difficulty
	header.ConsensusData.BlockSig = []any{
		uint64(2),
		[]any{proxyCertificate, make([]byte, ed25519.SignatureSize)},
	}
	if delegationPayload == nil {
		delegationPayload = []any{}
	}
	delegationPayloadCbor, err := cbor.Encode(delegationPayload)
	require.NoError(t, err)
	bodyProof, ok := header.BodyProof.([]any)
	require.True(t, ok)
	require.Len(t, bodyProof, 4)
	bodyProof = append([]any(nil), bodyProof...)
	bodyProof[2] = lcommon.Blake2b256Hash(delegationPayloadCbor).Bytes()
	header.BodyProof = bodyProof

	epochSlot := struct {
		cbor.StructAsArray
		Epoch uint64
		Slot  uint16
	}{Epoch: epoch, Slot: slot}
	chainDifficulty := struct {
		cbor.StructAsArray
		Value uint64
	}{Value: difficulty}
	extraData := struct {
		cbor.StructAsArray
		BlockVersion    byron.ByronBlockVersion
		SoftwareVersion byron.ByronSoftwareVersion
		Attributes      any
		ExtraProof      lcommon.Blake2b256
	}{
		BlockVersion:    header.ExtraData.BlockVersion,
		SoftwareVersion: header.ExtraData.SoftwareVersion,
		Attributes:      header.ExtraData.Attributes,
		ExtraProof:      header.ExtraData.ExtraProof,
	}
	toSign := struct {
		cbor.StructAsArray
		PrevHash    lcommon.Blake2b256
		BodyProof   any
		EpochSlot   any
		Difficulty  any
		ExtraHeader any
	}{
		PrevHash:    previousHash,
		BodyProof:   header.BodyProof,
		EpochSlot:   epochSlot,
		Difficulty:  chainDifficulty,
		ExtraHeader: extraData,
	}
	toSignCbor, err := cbor.Encode(toSign)
	require.NoError(t, err)
	protocolMagicCbor, err := cbor.Encode(protocolMagic)
	require.NoError(t, err)
	signed := []byte{'0', '1'}
	signed = append(signed, issuer.verificationKey...)
	signed = append(signed, 0x09) // Byron heavyweight main-block tag.
	signed = append(signed, protocolMagicCbor...)
	signed = append(signed, toSignCbor...)
	header.ConsensusData.BlockSig[1].([]any)[1] = ed25519.Sign(
		delegate.privateKey,
		signed,
	)
	header.SetCbor(nil)
	headerCbor, err := cbor.Encode(header)
	require.NoError(t, err)
	var decodedHeader byron.ByronMainBlockHeader
	_, err = cbor.Decode(headerCbor, &decodedHeader)
	require.NoError(t, err)

	var blockParts []cbor.RawMessage
	_, err = cbor.Decode(template.Cbor, &blockParts)
	require.NoError(t, err)
	require.Len(t, blockParts, 3)
	var bodyParts []cbor.RawMessage
	_, err = cbor.Decode(blockParts[1], &bodyParts)
	require.NoError(t, err)
	require.Len(t, bodyParts, 4)
	blockParts[0] = cbor.RawMessage(headerCbor)
	bodyParts[2] = cbor.RawMessage(delegationPayloadCbor)
	bodyCbor, err := cbor.Encode(bodyParts)
	require.NoError(t, err)
	blockParts[1] = cbor.RawMessage(bodyCbor)
	blockCbor, err := cbor.Encode(blockParts)
	require.NoError(t, err)
	rebuilt, err := byron.NewByronMainBlockFromCbor(blockCbor)
	require.NoError(t, err)
	require.Equal(t, previousHash, rebuilt.PrevHash())
	require.Equal(t, delegationPayload, rebuilt.Body.DlgPayload)
	return rebuilt
}

func rawByronPBFTBlock(
	t *testing.T,
	block *byron.ByronMainBlock,
) chain.RawBlock {
	t.Helper()
	require.NotNil(t, block)
	require.NotEmpty(t, block.Cbor())
	decoded, err := gledger.NewBlockFromCbor(
		gledger.BlockTypeByronMain,
		block.Cbor(),
	)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), decoded.Hash())
	require.Equal(t, block.PrevHash(), decoded.PrevHash())
	return chain.RawBlock{
		Slot:        block.SlotNumber(),
		Hash:        block.Hash().Bytes(),
		PrevHash:    block.PrevHash().Bytes(),
		BlockNumber: block.BlockNumber(),
		Type:        gledger.BlockTypeByronMain,
		Cbor:        append([]byte(nil), block.Cbor()...),
	}
}

func newGeneratedByronPBFTTestNodeConfig(
	t *testing.T,
	protocolMagic uint32,
	securityParam uint64,
	issuer byronPBFTTestKey,
	initialDelegate byronPBFTTestKey,
	genesisCertificate []any,
) *cardano.CardanoNodeConfig {
	t.Helper()
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		issuer.verificationKey,
	)
	require.NoError(t, err)
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"mainnet/config.json",
	)
	require.NoError(t, err)
	require.NoError(t, nodeConfig.LoadByronGenesisFromReader(strings.NewReader(
		fmt.Sprintf(`{
			"avvmDistr": {},
			"blockVersionData": {"slotDuration": "20000"},
			"ftsSeed": null,
			"protocolConsts": {"k": %d, "protocolMagic": %d},
			"startTime": 1506203091,
			"bootStakeholders": {},
			"heavyDelegation": {
				%q: {"cert": %q, "delegatePk": %q, "issuerPk": %q, "omega": 0}
			},
			"nonAvvmBalances": {},
			"vssCerts": {}
		}`,
			securityParam,
			protocolMagic,
			issuerHash.String(),
			hex.EncodeToString(genesisCertificate[3].([]byte)),
			base64.StdEncoding.EncodeToString(initialDelegate.verificationKey),
			base64.StdEncoding.EncodeToString(issuer.verificationKey),
		),
	)))
	return nodeConfig
}

func newByronPBFTTestNodeConfig(
	t *testing.T,
	block gledger.Block,
	securityParam uint64,
) *cardano.CardanoNodeConfig {
	t.Helper()
	header, ok := block.Header().(*byron.ByronMainBlockHeader)
	require.True(t, ok)
	proxySignature, ok := header.ConsensusData.BlockSig[1].([]any)
	require.True(t, ok)
	certificate, ok := proxySignature[0].([]any)
	require.True(t, ok)
	delegateKey, ok := certificate[2].([]byte)
	require.True(t, ok)
	certificateSignature, ok := certificate[3].([]byte)
	require.True(t, ok)
	omega, ok := certificate[0].(uint64)
	require.True(t, ok)
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		header.ConsensusData.PubKey,
	)
	require.NoError(t, err)

	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"mainnet/config.json",
	)
	require.NoError(t, err)
	require.NoError(t, nodeConfig.LoadByronGenesisFromReader(strings.NewReader(
		fmt.Sprintf(`{
			"avvmDistr": {},
			"blockVersionData": {"slotDuration": "20000"},
			"ftsSeed": null,
			"protocolConsts": {"k": %d, "protocolMagic": %d},
			"startTime": 1506203091,
			"bootStakeholders": {},
			"heavyDelegation": {
				%q: {"cert": %q, "delegatePk": %q, "issuerPk": %q, "omega": %d}
			},
			"nonAvvmBalances": {},
			"vssCerts": {}
		}`,
			securityParam,
			header.ProtocolMagic,
			issuerHash.String(),
			hex.EncodeToString(certificateSignature),
			base64.StdEncoding.EncodeToString(delegateKey),
			base64.StdEncoding.EncodeToString(header.ConsensusData.PubKey),
			omega,
		),
	)))
	return nodeConfig
}

func TestAdvanceByronPBFTStateEnforcesIssuerWindow(t *testing.T) {
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	const securityParam = 10
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newByronPBFTTestNodeConfig(
				t,
				block,
				securityParam,
			),
		},
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(config)
	require.NoError(t, err)

	state, err = ls.advanceByronPBFTState(state, block, true)
	require.NoError(t, err)
	state, err = ls.advanceByronPBFTState(state, block, true)
	require.NoError(t, err)
	_, err = ls.advanceByronPBFTState(state, block, true)
	require.ErrorContains(t, err, "signature threshold")
	require.Len(t, state.issuerState.SignatureHistory(), 2)
}

func TestAdvanceByronPBFTStateTracksDelegationActivationAndRevocation(
	t *testing.T,
) {
	const (
		protocolMagic = uint32(42)
		securityParam = uint64(100)
	)
	template := loadRealByronMainBlock(t)
	issuer := newByronPBFTTestKey(0x61)
	initialDelegate := newByronPBFTTestKey(0x62)
	replacementDelegate := newByronPBFTTestKey(0x63)
	genesisCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		0,
		issuer,
		initialDelegate,
	)
	activationCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		1,
		issuer,
		replacementDelegate,
	)
	revocationCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		2,
		issuer,
		issuer,
	)
	ls := &LedgerState{config: LedgerStateConfig{
		CardanoNodeConfig: newGeneratedByronPBFTTestNodeConfig(
			t,
			protocolMagic,
			securityParam,
			issuer,
			initialDelegate,
			genesisCertificate,
		),
	}}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-50_000*time.Second),
			time.Second,
			1_000,
		),
		DefaultSlotClockConfig(),
	)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(config)
	require.NoError(t, err)

	var origin lcommon.Blake2b256
	scheduleActivation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		1,
		1,
		origin,
		issuer,
		initialDelegate,
		genesisCertificate,
		[]any{activationCertificate},
	)
	require.Equal(t, origin, scheduleActivation.PrevHash())
	state, err = ls.advanceByronPBFTState(state, scheduleActivation, true)
	require.NoError(t, err)

	beforeActivation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		200,
		2,
		scheduleActivation.Hash(),
		issuer,
		initialDelegate,
		genesisCertificate,
		nil,
	)
	require.Greater(
		t,
		beforeActivation.SlotNumber(),
		scheduleActivation.SlotNumber(),
	)
	require.Equal(t, scheduleActivation.Hash(), beforeActivation.PrevHash())
	state, err = ls.advanceByronPBFTState(state, beforeActivation, true)
	require.NoError(t, err)

	staleAtActivation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		201,
		3,
		beforeActivation.Hash(),
		issuer,
		initialDelegate,
		genesisCertificate,
		nil,
	)
	_, err = ls.advanceByronPBFTState(state, staleAtActivation, true)
	require.ErrorContains(t, err, "active delegate mismatch")

	activated := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		201,
		3,
		beforeActivation.Hash(),
		issuer,
		replacementDelegate,
		activationCertificate,
		nil,
	)
	require.Equal(t, beforeActivation.Hash(), activated.PrevHash())
	state, err = ls.advanceByronPBFTState(state, activated, true)
	require.NoError(t, err)

	scheduleRevocation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		2,
		1,
		4,
		activated.Hash(),
		issuer,
		replacementDelegate,
		activationCertificate,
		[]any{revocationCertificate},
	)
	require.Greater(t, scheduleRevocation.SlotNumber(), activated.SlotNumber())
	require.Equal(t, activated.Hash(), scheduleRevocation.PrevHash())
	state, err = ls.advanceByronPBFTState(state, scheduleRevocation, true)
	require.NoError(t, err)

	beforeRevocation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		2,
		200,
		5,
		scheduleRevocation.Hash(),
		issuer,
		replacementDelegate,
		activationCertificate,
		nil,
	)
	require.Equal(t, scheduleRevocation.Hash(), beforeRevocation.PrevHash())
	state, err = ls.advanceByronPBFTState(state, beforeRevocation, true)
	require.NoError(t, err)

	staleAfterRevocation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		2,
		201,
		6,
		beforeRevocation.Hash(),
		issuer,
		replacementDelegate,
		activationCertificate,
		nil,
	)
	_, err = ls.advanceByronPBFTState(state, staleAfterRevocation, true)
	require.ErrorContains(t, err, "active delegate mismatch")

	revoked := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		2,
		201,
		6,
		beforeRevocation.Hash(),
		issuer,
		issuer,
		revocationCertificate,
		nil,
	)
	require.Equal(t, beforeRevocation.Hash(), revoked.PrevHash())
	state, err = ls.advanceByronPBFTState(state, revoked, true)
	require.NoError(t, err)
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		issuer.verificationKey,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		issuerHash,
		state.delegationState.ActiveDelegations()[issuerHash],
	)
}

func TestAdvanceByronPBFTStateRevocationRejectsSupersededDelegate(
	t *testing.T,
) {
	const (
		protocolMagic = uint32(43)
		securityParam = uint64(100)
	)
	template := loadRealByronMainBlock(t)
	issuer := newByronPBFTTestKey(0x71)
	initialDelegate := newByronPBFTTestKey(0x72)
	genesisCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		0,
		issuer,
		initialDelegate,
	)
	revocationCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		1,
		issuer,
		issuer,
	)
	ls := &LedgerState{config: LedgerStateConfig{
		CardanoNodeConfig: newGeneratedByronPBFTTestNodeConfig(
			t,
			protocolMagic,
			securityParam,
			issuer,
			initialDelegate,
			genesisCertificate,
		),
	}}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-50_000*time.Second),
			time.Second,
			1_000,
		),
		DefaultSlotClockConfig(),
	)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(config)
	require.NoError(t, err)

	var origin lcommon.Blake2b256
	scheduleRevocation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		1,
		1,
		origin,
		issuer,
		initialDelegate,
		genesisCertificate,
		[]any{revocationCertificate},
	)
	state, err = ls.advanceByronPBFTState(state, scheduleRevocation, true)
	require.NoError(t, err)
	beforeRevocation := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		200,
		2,
		scheduleRevocation.Hash(),
		issuer,
		initialDelegate,
		genesisCertificate,
		nil,
	)
	require.Equal(t, scheduleRevocation.Hash(), beforeRevocation.PrevHash())
	state, err = ls.advanceByronPBFTState(state, beforeRevocation, true)
	require.NoError(t, err)
	staleDelegate := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		201,
		3,
		beforeRevocation.Hash(),
		issuer,
		initialDelegate,
		genesisCertificate,
		nil,
	)
	_, err = ls.advanceByronPBFTState(state, staleDelegate, true)
	require.ErrorContains(t, err, "active delegate mismatch")
	revoked := newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		201,
		3,
		beforeRevocation.Hash(),
		issuer,
		issuer,
		revocationCertificate,
		nil,
	)
	require.Equal(t, beforeRevocation.Hash(), revoked.PrevHash())
	_, err = ls.advanceByronPBFTState(state, revoked, true)
	require.NoError(t, err)
}

func TestByronPBFTStateAtOriginDoesNotRequireChain(t *testing.T) {
	block, err := loadRealByronMainBlock(t).Decode()
	require.NoError(t, err)
	ls := &LedgerState{config: LedgerStateConfig{
		CardanoNodeConfig: newByronPBFTTestNodeConfig(t, block, 10),
	}}

	state, err := ls.byronPBFTStateAtTip(context.Background(), ocommon.Tip{})
	require.NoError(t, err)
	require.Empty(t, state.issuerState.SignatureHistory())
	require.NotEmpty(t, state.delegationState.ActiveDelegations())
}

func TestByronPBFTStateAtTipRebuildsAfterRestartAndRollback(t *testing.T) {
	const (
		protocolMagic = uint32(44)
		securityParam = 10
	)
	template := loadRealByronMainBlock(t)
	issuer := newByronPBFTTestKey(0x81)
	initialDelegate := newByronPBFTTestKey(0x82)
	replacementDelegate := newByronPBFTTestKey(0x83)
	genesisCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		0,
		issuer,
		initialDelegate,
	)
	activationCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		1,
		issuer,
		replacementDelegate,
	)
	revocationCertificate := newSignedByronPBFTDelegationCertificate(
		t,
		protocolMagic,
		2,
		issuer,
		issuer,
	)
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		issuer.verificationKey,
	)
	require.NoError(t, err)
	initialDelegateHash, err := byronconsensus.PBFTVerificationKeyHash(
		initialDelegate.verificationKey,
	)
	require.NoError(t, err)
	replacementDelegateHash, err := byronconsensus.PBFTVerificationKeyHash(
		replacementDelegate.verificationKey,
	)
	require.NoError(t, err)

	var origin lcommon.Blake2b256
	blocks := make([]*byron.ByronMainBlock, 0, 6)
	blocks = append(blocks, newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		1,
		1,
		origin,
		issuer,
		initialDelegate,
		genesisCertificate,
		[]any{activationCertificate},
	))
	blocks = append(blocks, newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		20,
		2,
		blocks[0].Hash(),
		issuer,
		initialDelegate,
		genesisCertificate,
		nil,
	))
	blocks = append(blocks, newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		1,
		21,
		3,
		blocks[1].Hash(),
		issuer,
		replacementDelegate,
		activationCertificate,
		nil,
	))
	blocks = append(blocks, newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		2,
		1,
		4,
		blocks[2].Hash(),
		issuer,
		replacementDelegate,
		activationCertificate,
		[]any{revocationCertificate},
	))
	blocks = append(blocks, newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		2,
		20,
		5,
		blocks[3].Hash(),
		issuer,
		replacementDelegate,
		activationCertificate,
		nil,
	))
	blocks = append(blocks, newSignedByronPBFTBlock(
		t,
		template,
		protocolMagic,
		2,
		21,
		6,
		blocks[4].Hash(),
		issuer,
		issuer,
		revocationCertificate,
		nil,
	))
	rawBlocks := make([]chain.RawBlock, len(blocks))
	for i, block := range blocks {
		rawBlocks[i] = rawByronPBFTBlock(t, block)
		if i > 0 {
			require.Equal(t, blocks[i-1].Hash(), block.PrevHash())
			require.Greater(t, block.SlotNumber(), blocks[i-1].SlotNumber())
		}
	}

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{
		securityParam: securityParam,
	}))
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(rawBlocks))
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newGeneratedByronPBFTTestNodeConfig(
				t,
				protocolMagic,
				securityParam,
				issuer,
				initialDelegate,
				genesisCertificate,
			),
		},
	}
	finalTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			rawBlocks[5].Slot,
			rawBlocks[5].Hash,
		),
		BlockNumber: rawBlocks[5].BlockNumber,
	}
	state, err := ls.byronPBFTStateAtTip(context.Background(), finalTip)
	require.NoError(t, err)
	require.Equal(
		t,
		[]lcommon.Blake2b224{
			issuerHash,
			issuerHash,
			issuerHash,
			issuerHash,
			issuerHash,
			issuerHash,
		},
		state.issuerState.SignatureHistory(),
		"delegate rotation must continue charging the genesis issuer",
	)
	require.Equal(
		t,
		issuerHash,
		state.delegationState.ActiveDelegations()[issuerHash],
		"restart reconstruction must activate the revocation",
	)

	ls.Lock()
	ls.byronPBFT = byronPBFTCache{
		state:       state,
		tip:         finalTip.Point,
		initialized: true,
	}
	ls.Unlock()
	beforeRevocation := ocommon.NewPoint(rawBlocks[4].Slot, rawBlocks[4].Hash)
	state, err = ls.byronPBFTStateAtTip(context.Background(), ochainsync.Tip{
		Point:       beforeRevocation,
		BlockNumber: rawBlocks[4].BlockNumber,
	})
	require.NoError(t, err)
	require.Equal(
		t,
		replacementDelegateHash,
		state.delegationState.ActiveDelegations()[issuerHash],
		"rollback must discard a cached revocation",
	)
	require.Equal(
		t,
		[]lcommon.Blake2b224{
			issuerHash,
			issuerHash,
			issuerHash,
			issuerHash,
			issuerHash,
		},
		state.issuerState.SignatureHistory(),
		"rollback reconstruction must ignore a cache from the abandoned tip",
	)

	beforeActivation := ocommon.NewPoint(rawBlocks[1].Slot, rawBlocks[1].Hash)
	state, err = ls.byronPBFTStateAtTip(context.Background(), ochainsync.Tip{
		Point:       beforeActivation,
		BlockNumber: rawBlocks[1].BlockNumber,
	})
	require.NoError(t, err)
	require.Equal(
		t,
		initialDelegateHash,
		state.delegationState.ActiveDelegations()[issuerHash],
		"rollback must discard a cached delegate activation",
	)

	var cachedMarker lcommon.Blake2b224
	cachedMarker[0] = 0xff
	state.issuerState, err = byronconsensus.NewPBFTState(
		[]lcommon.Blake2b224{cachedMarker},
		securityParam,
	)
	require.NoError(t, err)
	ls.Lock()
	ls.byronPBFT = byronPBFTCache{
		state:       state,
		tip:         beforeActivation,
		initialized: true,
	}
	ls.Unlock()
	state, err = ls.byronPBFTStateAtTip(context.Background(), finalTip)
	require.NoError(t, err)
	require.Equal(
		t,
		[]lcommon.Blake2b224{
			cachedMarker,
			issuerHash,
			issuerHash,
			issuerHash,
			issuerHash,
		},
		state.issuerState.SignatureHistory(),
		"forward reconstruction must continue from the cached ancestor",
	)
}

func TestValidateByronPBFTSlotRejectsFuture(t *testing.T) {
	require.NoError(t, validateByronPBFTSlot(42, 42))
	require.ErrorContains(t, validateByronPBFTSlot(43, 42), "current slot")
}

func TestByronPBFTCurrentSlotFailureIsNotAHeaderRejection(t *testing.T) {
	ls := &LedgerState{}
	err := ls.validateByronPBFTCurrentSlot(&mockByronBlock{})
	require.ErrorIs(t, err, errByronPBFTCurrentSlotUnavailable)

	err = classifyByronPBFTApplyError(
		ocommon.NewPoint(100, []byte{0x01}),
		err,
		true,
	)
	var validationErr *headerValidationError
	require.False(t, errors.As(err, &validationErr))
}

func TestByronPBFTConsensusFailureIsAHeaderRejection(t *testing.T) {
	cause := errors.New("invalid signature")
	err := classifyByronPBFTApplyError(
		ocommon.NewPoint(100, []byte{0x01}),
		cause,
		true,
	)
	var validationErr *headerValidationError
	require.ErrorAs(t, err, &validationErr)
	require.ErrorIs(t, err, cause)
}

func TestValidateByronPBFTHeaderRejectsFutureEbb(t *testing.T) {
	ls := &LedgerState{}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-100*time.Second),
			time.Second,
			100,
		),
		DefaultSlotClockConfig(),
	)
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{},
	}
	ebb.BlockHeader.ConsensusData.Epoch = 1

	err := ls.validateByronPBFTHeaderCrypto(ebb)
	require.ErrorContains(t, err, "current slot")
}

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
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewByronPBFTCacheDevnetEmptyGenesisIssuers reproduces
// blinklabs-io/dingo's devnet startup regression: a Byron genesis with no
// boot stakeholders (and therefore no heavy delegation, since a heavy
// delegation certificate must name an existing boot stakeholder as its
// issuer) has no possible PBFT signer, so no valid Byron main block can ever
// be produced on that chain. internal/test/devnet/configurator.sh generates
// exactly this genesis shape for a network that hard-forks away from Byron
// at genesis (testnet.yaml sets every TestXHardForkAtEpoch to 0). Ledger
// state construction must tolerate it rather than failing node startup.
func TestNewByronPBFTCacheDevnetEmptyGenesisIssuers(t *testing.T) {
	t.Parallel()

	const byronGenesisJSON = `{
		"protocolConsts": {"k": 60, "protocolMagic": 42},
		"blockVersionData": {"slotDuration": "1000"},
		"bootStakeholders": {},
		"heavyDelegation": {}
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)

	cache, err := newByronPBFTCache(LedgerStateConfig{CardanoNodeConfig: cfg})
	require.NoError(
		t,
		err,
		"a Byron genesis with no possible PBFT issuers must not fail ledger "+
			"state construction",
	)
	assert.Nil(
		t,
		cache.config,
		"no PBFT config should be cached when Byron has no eligible issuers",
	)
	assert.True(t, cache.noGenesisIssuers)
}

// TestNewByronPBFTCacheRealByronGenesis is the control: a Byron genesis that
// does declare a boot stakeholder (every real chain -- mainnet, preprod,
// preview -- always has at least one) must still build and cache a full PBFT
// config, so this fix does not relax validation for a chain with real Byron
// history.
func TestNewByronPBFTCacheRealByronGenesis(t *testing.T) {
	t.Parallel()

	const byronGenesisJSON = `{
		"protocolConsts": {"k": 60, "protocolMagic": 42},
		"blockVersionData": {"slotDuration": "1000"}
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)

	cache, err := newByronPBFTCache(LedgerStateConfig{CardanoNodeConfig: cfg})
	require.NoError(t, err)
	require.NotNil(
		t,
		cache.config,
		"a genesis with a real boot stakeholder must still build a PBFT config",
	)
	assert.Len(t, cache.config.GenesisKeyHashes, 1)
}

const noByronIssuersRule = "declares no boot stakeholders"

// newNoByronIssuersTestLedger builds a LedgerState over the devnet genesis
// shape: a Byron genesis with no boot stakeholders, its PBFT cache built the
// way NewLedgerState builds it, a real primary chain and a working slot clock.
// The Byron genesis hash is set explicitly because LoadByronGenesisFromReader
// does not derive it; production config loading does.
func newNoByronIssuersTestLedger(
	t *testing.T,
	genesisHash lcommon.Blake2b256,
) (*LedgerState, *chain.Chain) {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {"k": 60, "protocolMagic": 42},
		"blockVersionData": {"slotDuration": "1000"},
		"bootStakeholders": {},
		"heavyDelegation": {}
	}`)))
	cfg.ByronGenesisHash = genesisHash.String()
	lsConfig := LedgerStateConfig{CardanoNodeConfig: cfg}
	cache, err := newByronPBFTCache(lsConfig)
	require.NoError(t, err)

	cm, err := chain.NewManager(context.Background(), newTestDB(t), nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 10}),
	)
	ls := &LedgerState{
		chain:     cm.PrimaryChain(),
		config:    lsConfig,
		byronPBFT: cache,
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-100*time.Second),
			time.Second,
			100,
		),
		DefaultSlotClockConfig(),
	)
	return ls, cm.PrimaryChain()
}

// TestByronHeaderRejectedWithoutGenesisIssuers feeds peer-shaped Byron
// headers and blocks to a node whose Byron genesis has no boot stakeholders,
// through every header entry point that routes Byron to
// validateByronPBFTHeaderCrypto. An epoch-boundary block carries no PBFT
// signature, so without a rule of its own a genesis-anchored EBB at origin,
// or any EBB at or below the current slot after it, would pass every
// entry point on this chain.
func TestByronHeaderRejectedWithoutGenesisIssuers(t *testing.T) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("devnet byron genesis"))
	entryPoints := []struct {
		name   string
		verify func(*LedgerState, gledger.Block) error
	}{
		{"chainsync header", func(ls *LedgerState, b gledger.Block) error {
			return ls.verifyBlockHeaderOnlyCrypto(b.Header())
		}},
		{"chain selection header", func(ls *LedgerState, b gledger.Block) error {
			return ls.ValidateChainSelectionHeaderCrypto(b.Header())
		}},
		{"announced header", func(ls *LedgerState, b gledger.Block) error {
			return ls.ValidateBlockHeaderCrypto(b.Header())
		}},
		{"fetched block", func(ls *LedgerState, b gledger.Block) error {
			return ls.verifyBlockHeaderCryptoBeforeApply(b)
		}},
	}
	inputs := []struct {
		name     string
		atOrigin bool
		block    gledger.Block
	}{
		{
			name:     "genesis-anchored EBB at origin",
			atOrigin: true,
			block: &byron.ByronEpochBoundaryBlock{
				BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
					PrevBlock: genesisHash,
				},
			},
		},
		{
			name: "EBB after a post-Byron tip",
			block: &byron.ByronEpochBoundaryBlock{
				BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
					PrevBlock: lcommon.Blake2b256Hash([]byte("conway tip")),
				},
			},
		},
		{
			name: "main block after a post-Byron tip",
			block: &byron.ByronMainBlock{
				BlockHeader: &byron.ByronMainBlockHeader{},
			},
		},
	}
	for _, input := range inputs {
		for _, entry := range entryPoints {
			t.Run(input.name+"/"+entry.name, func(t *testing.T) {
				t.Parallel()
				ls, primaryChain := newNoByronIssuersTestLedger(
					t,
					genesisHash,
				)
				if !input.atOrigin {
					require.NoError(
						t,
						primaryChain.AddRawBlocks(
							context.Background(),
							[]chain.RawBlock{{
								Slot:        1,
								Hash:        bytes.Repeat([]byte{0xcc}, 32),
								BlockNumber: 0,
								Type:        gledger.BlockTypeConway,
								Cbor:        []byte{0x80},
							}},
						),
					)
				}
				require.Equal(
					t,
					input.atOrigin,
					len(primaryChain.Tip().Point.Hash) == 0,
				)
				err := entry.verify(ls, input.block)
				require.ErrorContains(t, err, noByronIssuersRule)
			})
		}
	}
}

// TestByronPBFTStateAtTipRejectsChainWithoutGenesisIssuers pins the ledger
// apply path: reconstructing Byron PBFT state for a batch containing a Byron
// block must fail on the same rule rather than on missing configuration.
func TestByronPBFTStateAtTipRejectsChainWithoutGenesisIssuers(t *testing.T) {
	t.Parallel()

	ls, _ := newNoByronIssuersTestLedger(
		t,
		lcommon.Blake2b256Hash([]byte("devnet byron genesis")),
	)
	_, err := ls.byronPBFTStateAtTip(
		context.Background(),
		ocommon.Tip{},
	)
	require.ErrorContains(t, err, noByronIssuersRule)
}

// TestByronHeaderGateReadsOnlyConstructionTimeFields runs the no-issuer
// header gate while another goroutine commits Byron PBFT state under the
// ledger lock, as ledger apply does concurrently with chainsync header
// validation. The gate takes no lock, so under -race it must read only the
// fields NewLedgerState sets once.
func TestByronHeaderGateReadsOnlyConstructionTimeFields(t *testing.T) {
	t.Parallel()

	ls, _ := newNoByronIssuersTestLedger(
		t,
		lcommon.Blake2b256Hash([]byte("devnet byron genesis")),
	)
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{},
	}
	const iterations = 200
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range iterations {
			ls.Lock()
			ls.byronPBFT.tip = ocommon.NewPoint(uint64(i), nil)
			ls.byronPBFT.initialized = true
			ls.Unlock()
		}
	}()
	for range iterations {
		require.ErrorContains(
			t,
			ls.validateByronPBFTHeaderCrypto(context.Background(), ebb),
			noByronIssuersRule,
		)
	}
	<-done
}

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
	t testing.TB,
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
	slot uint64,
	difficulty uint64,
	previousHash lcommon.Blake2b256,
	issuer byronPBFTTestKey,
	delegate byronPBFTTestKey,
	proxyCertificate []any,
	delegationPayload []any,
) *byron.ByronMainBlock {
	t.Helper()
	return newSignedByronPBFTBlockWithBody(
		t,
		template,
		protocolMagic,
		epoch,
		slot,
		difficulty,
		previousHash,
		issuer,
		delegate,
		proxyCertificate,
		delegationPayload,
		nil,
	)
}

// byronPBFTBodyOverride replaces parts of the template block's body. The
// header's body proof is recomputed over the replacement before the header is
// signed, so the block is internally consistent.
type byronPBFTBodyOverride struct {
	// emptyTransactions drops the template's transactions.
	emptyTransactions bool
	// updatePayload is the raw CBOR of the block's update payload.
	updatePayload []byte
	// blockVersion replaces the version the header declares, which is the
	// protocol version its issuer endorses.
	blockVersion *byron.ByronBlockVersion
}

func newSignedByronPBFTBlockWithBody(
	t *testing.T,
	template models.Block,
	protocolMagic uint32,
	epoch uint64,
	slot uint64,
	difficulty uint64,
	previousHash lcommon.Blake2b256,
	issuer byronPBFTTestKey,
	delegate byronPBFTTestKey,
	proxyCertificate []any,
	delegationPayload []any,
	override *byronPBFTBodyOverride,
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
	if override != nil && override.blockVersion != nil {
		header.ExtraData.BlockVersion = *override.blockVersion
	}
	header.ConsensusData.BlockSig = []any{
		uint64(2),
		[]any{proxyCertificate, make([]byte, ed25519.SignatureSize)},
	}
	if delegationPayload == nil {
		delegationPayload = []any{}
	}
	delegationPayloadCbor, err := cbor.Encode(
		cbor.IndefLengthList(delegationPayload),
	)
	require.NoError(t, err)
	bodyProof, ok := header.BodyProof.([]any)
	require.True(t, ok)
	require.Len(t, bodyProof, 4)
	bodyProof = append([]any(nil), bodyProof...)
	bodyProof[2] = lcommon.Blake2b256Hash(delegationPayloadCbor).Bytes()
	emptyTransactionsCbor := []byte{0x9f, 0xff}
	if override != nil && override.emptyTransactions {
		bodyProof[0] = []any{
			uint64(0),
			byron.MerkleRoot(nil).Bytes(),
			lcommon.Blake2b256Hash(emptyTransactionsCbor).Bytes(),
		}
	}
	if override != nil && override.updatePayload != nil {
		bodyProof[3] = lcommon.Blake2b256Hash(override.updatePayload).Bytes()
	}
	header.BodyProof = bodyProof

	epochSlot := struct {
		cbor.StructAsArray
		Epoch uint64
		Slot  uint64
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
		ExtraProof      []byte
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
	if override != nil && override.emptyTransactions {
		bodyParts[0] = cbor.RawMessage(emptyTransactionsCbor)
	}
	if override != nil && override.updatePayload != nil {
		bodyParts[3] = cbor.RawMessage(override.updatePayload)
	}
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

func encodeIndefiniteByronList(t *testing.T, values []any) []byte {
	t.Helper()
	encoded := []byte{0x9f}
	for _, value := range values {
		item, err := cbor.Encode(value)
		require.NoError(t, err)
		encoded = append(encoded, item...)
	}
	return append(encoded, 0xff)
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
	require.NoError(t, loadByronGenesisForTest(t, nodeConfig, strings.NewReader(
		fmt.Sprintf(`{
			"avvmDistr": {},
			"blockVersionData": {
				"heavyDelThd": "0", "maxBlockSize": "1",
				"maxHeaderSize": "1", "maxProposalSize": "1",
				"maxTxSize": "1", "mpcThd": "0", "scriptVersion": 0,
				"slotDuration": "20000",
				"softforkRule": {"initThd": "0", "minThd": "0", "thdDecrement": "0"},
				"txFeePolicy": {"multiplier": "0", "summand": "0"},
				"unlockStakeEpoch": "0", "updateImplicit": "0",
				"updateProposalThd": "0", "updateVoteThd": "0"
			},
			"ftsSeed": null,
			"protocolConsts": {"k": %d, "protocolMagic": %d},
			"startTime": 1506203091,
			"bootStakeholders": {%q: 1},
			"heavyDelegation": {
				%q: {"cert": %q, "delegatePk": %q, "issuerPk": %q, "omega": 0}
			},
			"nonAvvmBalances": {},
			"vssCerts": {}
		}`,
			securityParam,
			protocolMagic,
			issuerHash.String(),
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
	require.NoError(t, loadByronGenesisForTest(t, nodeConfig, strings.NewReader(
		fmt.Sprintf(`{
			"avvmDistr": {},
			"blockVersionData": {
				"heavyDelThd": "0", "maxBlockSize": "1",
				"maxHeaderSize": "1", "maxProposalSize": "1",
				"maxTxSize": "1", "mpcThd": "0", "scriptVersion": 0,
				"slotDuration": "20000",
				"softforkRule": {"initThd": "0", "minThd": "0", "thdDecrement": "0"},
				"txFeePolicy": {"multiplier": "0", "summand": "0"},
				"unlockStakeEpoch": "0", "updateImplicit": "0",
				"updateProposalThd": "0", "updateVoteThd": "0"
			},
			"ftsSeed": null,
			"protocolConsts": {"k": %d, "protocolMagic": %d},
			"startTime": 1506203091,
			"bootStakeholders": {%q: 1},
			"heavyDelegation": {
				%q: {"cert": %q, "delegatePk": %q, "issuerPk": %q, "omega": %d}
			},
			"nonAvvmBalances": {},
			"vssCerts": {}
		}`,
			securityParam,
			header.ProtocolMagic,
			issuerHash.String(),
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
	t.Parallel()

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
	state, err := newByronPBFTState(config, nil)
	require.NoError(t, err)

	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		block,
		true,
	)
	require.NoError(t, err)
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		block,
		true,
	)
	require.NoError(t, err)
	_, err = ls.advanceByronPBFTState(context.Background(), state, block, true)
	require.ErrorContains(t, err, "signature threshold")
	require.Len(t, state.issuerState.SignatureHistory(), 2)
}

func TestByronPBFTStateUsesCardanoNodeThreshold(t *testing.T) {
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	for _, tc := range []struct {
		name        string
		threshold   float64
		k           uint64
		maxAllowed  uint64
		shouldBlock bool
	}{
		{name: "default", k: 10, maxAllowed: 2, shouldBlock: true},
		{name: "0.10", threshold: 0.10, k: 10, maxAllowed: 1, shouldBlock: true},
		{name: "0.22", threshold: 0.22, k: 10, maxAllowed: 2, shouldBlock: true},
		{name: "0.50", threshold: 0.50, k: 10, maxAllowed: 5, shouldBlock: true},
		{name: "1.1", threshold: 1.1, k: 10, maxAllowed: 10},
		// 0.57 * 100 is 56.99999999999999 in Double.
		{name: "0.57", threshold: 0.57, k: 100, maxAllowed: 56, shouldBlock: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeConfig := newByronPBFTTestNodeConfig(t, block, tc.k)
			if tc.threshold != 0 {
				threshold := cardano.CardanoNodeDouble(tc.threshold)
				nodeConfig.PBftSignatureThreshold = &threshold
			}
			ls := &LedgerState{config: LedgerStateConfig{
				CardanoNodeConfig: nodeConfig,
			}}

			state, err := ls.byronPBFTStateAtTip(context.Background(), ocommon.Tip{})
			require.NoError(t, err)
			issuer := lcommon.Blake2b224Hash([]byte("configured issuer"))
			for range tc.maxAllowed {
				state.issuerState, err = state.issuerState.Transition(issuer)
				require.NoError(t, err)
			}
			_, err = state.issuerState.Transition(issuer)
			if tc.shouldBlock {
				require.ErrorContains(t, err, "signature threshold")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAdvanceByronPBFTStateTracksDelegationActivationAndRevocation(
	t *testing.T,
) {
	t.Parallel()

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
	state, err := newByronPBFTState(config, nil)
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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		scheduleActivation,
		true,
	)
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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		beforeActivation,
		true,
	)
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
	_, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		staleAtActivation,
		true,
	)
	require.ErrorContains(t, err, "does not authorize delegate")

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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		activated,
		true,
	)
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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		scheduleRevocation,
		true,
	)
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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		beforeRevocation,
		true,
	)
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
	_, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		staleAfterRevocation,
		true,
	)
	require.ErrorContains(t, err, "does not authorize delegate")

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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		revoked,
		true,
	)
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
	t.Parallel()

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
	state, err := newByronPBFTState(config, nil)
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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		scheduleRevocation,
		true,
	)
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
	state, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		beforeRevocation,
		true,
	)
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
	_, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		staleDelegate,
		true,
	)
	require.ErrorContains(t, err, "does not authorize delegate")
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
	_, err = ls.advanceByronPBFTState(
		context.Background(),
		state,
		revoked,
		true,
	)
	require.NoError(t, err)
}

func TestByronPBFTStateAtOriginDoesNotRequireChain(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{
		securityParam: securityParam,
	}))
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), rawBlocks),
	)
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

// TestByronPBFTCurrentSlotPastHorizonIsDeferred: a header past the known epoch
// horizon (from-genesis sync reaching epoch 1 before its epoch is applied) is
// not a peer fault. Reporting it as a hard failure recycles every peer at the
// first epoch boundary.
func TestByronPBFTCurrentSlotPastHorizonIsDeferred(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.slotClock = NewSlotClock(
		pastHorizonSlotTimeProvider{
			SlotTimeProvider: newMockSlotTimeProvider(
				time.Now().Add(-100*time.Second),
				time.Second,
				100,
			),
			rejectedSlot: 21600,
		},
		DefaultSlotClockConfig(),
	)
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{},
	}
	ebb.BlockHeader.ConsensusData.Epoch = 1

	err := ls.validateByronPBFTCurrentSlot(ebb)
	require.ErrorIs(t, err, errByronPBFTCurrentSlotUnavailable)
	require.True(t, IsHeaderVerificationDeferred(err))
}

func TestByronPBFTCurrentSlotFailureIsNotAHeaderRejection(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

	err := ls.validateByronPBFTHeaderCrypto(context.Background(), ebb)
	require.ErrorContains(t, err, "current slot")
}

// newByronGenesisAnchorTestLedger builds a LedgerState wired to a fresh,
// real *chain.Chain and a Byron genesis hash, for testing
// validateByronPBFTHeaderCrypto's origin-anchor checks.
// The chain starts at origin unless the caller adds blocks to it first.
func newByronGenesisAnchorTestLedger(
	t *testing.T,
	genesisHash string,
) (*LedgerState, *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 10}),
	)
	primaryChain := cm.PrimaryChain()

	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"mainnet/config.json",
	)
	require.NoError(t, err)
	nodeConfig.ByronGenesisHash = genesisHash

	ls := &LedgerState{
		chain:  primaryChain,
		config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-100*time.Second),
			time.Second,
			100,
		),
		DefaultSlotClockConfig(),
	)
	return ls, primaryChain
}

// TestValidateByronPBFTHeaderAcceptsGenesisAnchoredEbb is the
// positive case: at origin, an epoch-boundary block
// whose previous hash matches the configured Byron genesis hash is accepted.
func TestValidateByronPBFTHeaderAcceptsGenesisAnchoredEbb(t *testing.T) {
	t.Parallel()

	genesisHashValue := lcommon.Blake2b256Hash([]byte("configured genesis"))
	ls, primaryChain := newByronGenesisAnchorTestLedger(
		t,
		genesisHashValue.String(),
	)
	require.Zero(t, primaryChain.Tip().Point.Slot)
	require.Empty(t, primaryChain.Tip().Point.Hash)

	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
			PrevBlock: genesisHashValue,
		},
	}

	require.NoError(
		t,
		ls.validateByronPBFTHeaderCrypto(context.Background(), ebb),
	)
}

// TestValidateByronPBFTHeaderRejectsGenesisHashMismatch is the
// regression itself: at origin, an epoch-boundary
// block whose previous hash does not match the configured Byron genesis
// hash must be rejected, even though it is otherwise correctly placed and
// sized. The reference rejects this with ChainValidationGenesisHashMismatch.
func TestValidateByronPBFTHeaderRejectsGenesisHashMismatch(t *testing.T) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis")).
		String()
	ls, primaryChain := newByronGenesisAnchorTestLedger(t, genesisHash)
	require.Zero(t, primaryChain.Tip().Point.Slot)
	require.Empty(t, primaryChain.Tip().Point.Hash)

	wrongPrevBlock := lcommon.Blake2b256Hash([]byte("wrong prev hash"))
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
			PrevBlock: wrongPrevBlock,
		},
	}

	err := ls.validateByronPBFTHeaderCrypto(context.Background(), ebb)
	require.ErrorContains(t, err, "genesis hash")
}

// TestValidateByronPBFTHeaderRejectsNonZeroEpochEbbAtOrigin verifies that an
// EBB's block number (Difficulty.Value) and slot (derived from
// ConsensusData.Epoch) are independent fields.
// chain.firstBlockNumberValid only constrains the former, and
// validateByronPBFTCurrentSlot only rejects a future slot, not a past one.
// Without the epoch-0 check, an EBB with Difficulty 0, PrevBlock equal to
// the configured genesis hash, and any past nonzero epoch would pass every
// other check here despite skipping every epoch before it -- the first EBB
// of any Byron chain is always epoch 0, unconditionally.
func TestValidateByronPBFTHeaderRejectsNonZeroEpochEbbAtOrigin(t *testing.T) {
	t.Parallel()

	genesisHashValue := lcommon.Blake2b256Hash([]byte("configured genesis"))
	ls, primaryChain := newByronGenesisAnchorTestLedger(
		t,
		genesisHashValue.String(),
	)
	require.Zero(t, primaryChain.Tip().Point.Slot)
	require.Empty(t, primaryChain.Tip().Point.Hash)
	// Epoch 1 is slot 21600 (byron.ByronSlotsPerEpoch); push the mock clock's
	// current slot well past that so this is a genuinely past epoch, not one
	// that would incidentally also fail the future-slot check instead.
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-30000*time.Second),
			time.Second,
			100,
		),
		DefaultSlotClockConfig(),
	)

	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
			PrevBlock: genesisHashValue,
		},
	}
	ebb.BlockHeader.ConsensusData.Epoch = 1
	ebb.BlockHeader.ConsensusData.Difficulty.Value = 0

	err := ls.validateByronPBFTHeaderCrypto(context.Background(), ebb)
	require.ErrorContains(t, err, "epoch 0")
}

// TestValidateByronPBFTHeaderRejectsMainBlockAtOrigin is the
// acceptance criterion that a PBFT-signed regular
// Byron block must never be accepted as the first block of a from-genesis
// chain, even one that (like the genuine first block) carries block number
// and difficulty 0. Only an epoch-boundary block may open the chain. This
// must be rejected before any PBFT signature verification runs -- the
// header below carries no signature at all, and still must be rejected.
func TestValidateByronPBFTHeaderRejectsMainBlockAtOrigin(t *testing.T) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis")).
		String()
	ls, primaryChain := newByronGenesisAnchorTestLedger(t, genesisHash)
	require.Zero(t, primaryChain.Tip().Point.Slot)
	require.Empty(t, primaryChain.Tip().Point.Hash)

	mainBlock := &byron.ByronMainBlock{
		BlockHeader: &byron.ByronMainBlockHeader{},
	}

	err := ls.validateByronPBFTHeaderCrypto(context.Background(), mainBlock)
	require.ErrorContains(t, err, "epoch-boundary block")
}

// TestValidateByronPBFTHeaderSkipsGenesisAnchorAwayFromOrigin confirms the
// anchor check is scoped to the chain's first block only. An epoch-boundary
// block at a later epoch boundary chains onto the previous block, not
// genesis, and a ledger started from a trusted snapshot or bulk import at a
// non-origin point has the same shape: its primary chain tip is that
// trusted point, not origin. Both must reach the ordinary current-slot
// check unaffected by the genesis hash, matching the earlier behavior.
func TestValidateByronPBFTHeaderSkipsGenesisAnchorAwayFromOrigin(t *testing.T) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis")).
		String()
	ls, primaryChain := newByronGenesisAnchorTestLedger(t, genesisHash)
	require.NoError(
		t,
		primaryChain.AddRawBlocks(context.Background(), []chain.RawBlock{
			{
				Slot:        0,
				Hash:        bytes.Repeat([]byte{0xaa}, 32),
				BlockNumber: 0,
				Type:        gledger.BlockTypeByronEbb,
				Cbor:        []byte{0x80},
			},
		}),
	)
	require.NotZero(t, primaryChain.Tip().Point.Hash)

	// A prev hash that matches neither genesis nor the block just added:
	// away from origin, neither should matter to this check.
	wrongPrevBlock := lcommon.Blake2b256Hash([]byte("neither genesis nor tip"))
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
			PrevBlock: wrongPrevBlock,
		},
	}
	ebb.BlockHeader.ConsensusData.Epoch = 1

	err := ls.validateByronPBFTHeaderCrypto(context.Background(), ebb)
	require.ErrorContains(t, err, "current slot")
	require.NotContains(t, err.Error(), "genesis hash")
}

// TestValidateByronPBFTHeaderAppliesGenesisAnchorAfterRollbackToOrigin is
// the acceptance criterion that the EBB-only
// anchor rule applies identically after a rollback empties the chain back
// to origin, not only on a chain that has never been touched.
// chain.Chain.atOriginAfterMutation documents the equivalent chain-layer
// distinction for the block-number half of this same anchor.
func TestValidateByronPBFTHeaderAppliesGenesisAnchorAfterRollbackToOrigin(
	t *testing.T,
) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis")).
		String()
	ls, primaryChain := newByronGenesisAnchorTestLedger(t, genesisHash)
	require.NoError(
		t,
		primaryChain.AddRawBlocks(context.Background(), []chain.RawBlock{
			{
				Slot:        0,
				Hash:        bytes.Repeat([]byte{0xaa}, 32),
				BlockNumber: 0,
				Type:        gledger.BlockTypeByronEbb,
				Cbor:        []byte{0x80},
			},
		}),
	)
	require.NotZero(t, primaryChain.Tip().Point.Hash)

	require.NoError(
		t,
		primaryChain.RollbackUnbounded(context.Background(), ocommon.Point{}),
	)
	require.Zero(t, primaryChain.Tip().Point.Slot)
	require.Empty(t, primaryChain.Tip().Point.Hash)

	wrongPrevBlock := lcommon.Blake2b256Hash([]byte("wrong prev hash"))
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
			PrevBlock: wrongPrevBlock,
		},
	}

	err := ls.validateByronPBFTHeaderCrypto(context.Background(), ebb)
	require.ErrorContains(t, err, "genesis hash")
}

// queueByronOriginEbbHeader queues a genesis-anchored epoch 0 EBB header on
// the chain's header queue without applying any block, the state chainsync
// leaves behind while it batches headers ahead of blockfetch.
func queueByronOriginEbbHeader(
	t *testing.T,
	primaryChain *chain.Chain,
	genesisHash lcommon.Blake2b256,
) *byron.ByronEpochBoundaryBlockHeader {
	t.Helper()
	ebbHeader := &byron.ByronEpochBoundaryBlockHeader{PrevBlock: genesisHash}
	require.NoError(
		t,
		primaryChain.AddBlockHeader(context.Background(), ebbHeader),
	)
	require.Equal(t, 1, primaryChain.HeaderCount())
	// Only headers are queued: the primary chain tip is still origin.
	require.Zero(t, primaryChain.Tip().Point.Slot)
	require.Empty(t, primaryChain.Tip().Point.Hash)
	return ebbHeader
}

// TestValidateByronPBFTHeaderAcceptsMainBlockAfterQueuedOriginEbb covers a
// from-genesis sync: chainsync verifies each header before blockfetch applies
// any block, so the main block after the first EBB is verified while the
// primary tip is still origin. It chains onto the queued EBB and is not the
// first block of the chain.
func TestValidateByronPBFTHeaderAcceptsMainBlockAfterQueuedOriginEbb(
	t *testing.T,
) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis"))
	ls, primaryChain := newByronGenesisAnchorTestLedger(
		t,
		genesisHash.String(),
	)
	ebbHeader := queueByronOriginEbbHeader(t, primaryChain, genesisHash)

	mainBlock := &byron.ByronMainBlock{
		BlockHeader: &byron.ByronMainBlockHeader{
			PrevBlock: ebbHeader.Hash(),
		},
	}

	// The unsigned header still fails PBFT verification, but past the
	// first-block gate.
	err := ls.validateByronPBFTHeaderCrypto(context.Background(), mainBlock)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "epoch-boundary block")
	require.NotContains(t, err.Error(), "first block")
}

// TestValidateByronPBFTHeaderKeepsAnchorForQueuedOriginEbb confirms the queued
// first EBB is still the first block when blockfetch verifies it again: it
// keeps its epoch 0 and genesis-hash checks.
func TestValidateByronPBFTHeaderKeepsAnchorForQueuedOriginEbb(t *testing.T) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis"))
	ls, primaryChain := newByronGenesisAnchorTestLedger(
		t,
		genesisHash.String(),
	)
	ebbHeader := queueByronOriginEbbHeader(t, primaryChain, genesisHash)
	require.NoError(
		t,
		ls.validateByronPBFTHeaderCrypto(
			context.Background(),
			&byron.ByronEpochBoundaryBlock{BlockHeader: ebbHeader},
		),
	)

	badHeader := &byron.ByronEpochBoundaryBlockHeader{
		PrevBlock: lcommon.Blake2b256Hash([]byte("wrong prev hash")),
	}
	err := ls.validateByronPBFTHeaderCrypto(
		context.Background(),
		&byron.ByronEpochBoundaryBlock{BlockHeader: badHeader},
	)
	require.ErrorContains(t, err, "genesis hash")
}

// TestValidateByronPBFTHeaderRejectsMainBlockAfterRollbackDropsQueuedEbb
// confirms a rollback to origin drops the queued EBB, so a main block is the
// first block of the chain again and is rejected.
func TestValidateByronPBFTHeaderRejectsMainBlockAfterRollbackDropsQueuedEbb(
	t *testing.T,
) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis"))
	ls, primaryChain := newByronGenesisAnchorTestLedger(
		t,
		genesisHash.String(),
	)
	ebbHeader := queueByronOriginEbbHeader(t, primaryChain, genesisHash)
	require.NoError(
		t,
		primaryChain.RollbackUnbounded(context.Background(), ocommon.Point{}),
	)
	require.Zero(t, primaryChain.HeaderCount())

	mainBlock := &byron.ByronMainBlock{
		BlockHeader: &byron.ByronMainBlockHeader{
			PrevBlock: ebbHeader.Hash(),
		},
	}
	err := ls.validateByronPBFTHeaderCrypto(context.Background(), mainBlock)
	require.ErrorContains(t, err, "epoch-boundary block")
}

// TestValidateChainSelectionHeaderCryptoSkipsByronFirstBlockGate covers the
// peer-relative ingress path: a peer's headers are verified before they reach
// the local header queue (the ChainsyncEvent that queues the EBB is delivered
// asynchronously), so neither the primary tip nor the queue can say whether a
// peer's header is the chain's first. The gate must not fire there; the ledger
// header-queue path still enforces it.
func TestValidateChainSelectionHeaderCryptoSkipsByronFirstBlockGate(
	t *testing.T,
) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("configured genesis"))
	ls, primaryChain := newByronGenesisAnchorTestLedger(
		t,
		genesisHash.String(),
	)
	require.Zero(t, primaryChain.HeaderCount())
	ebbHeader := &byron.ByronEpochBoundaryBlockHeader{PrevBlock: genesisHash}
	mainHeader := &byron.ByronMainBlockHeader{PrevBlock: ebbHeader.Hash()}

	err := ls.ValidateChainSelectionHeaderCrypto(mainHeader)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "epoch-boundary block")
	err = ls.ValidateBlockHeaderCrypto(mainHeader)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "epoch-boundary block")

	// The ledger's own header-queue verification keeps the gate.
	err = ls.verifyBlockHeaderOnlyCrypto(mainHeader)
	require.ErrorContains(t, err, "epoch-boundary block")
}

// TestValidateByronPBFTHeaderCryptoRejectsNilHeaders calls the Byron header
// validator with typed-nil headers, the shape a Byron block type can carry
// in process. It must return an error rather than read through the header.
func TestValidateByronPBFTHeaderCryptoRejectsNilHeaders(t *testing.T) {
	t.Parallel()

	blocks := map[string]gledger.Block{
		"header-only main": headerOnlyBlock{
			header: (*byron.ByronMainBlockHeader)(nil),
		},
		"header-only EBB": headerOnlyBlock{
			header: (*byron.ByronEpochBoundaryBlockHeader)(nil),
		},
		"main block":     &byron.ByronMainBlock{},
		"boundary block": &byron.ByronEpochBoundaryBlock{},
	}
	for name, block := range blocks {
		for _, atOrigin := range []bool{true, false} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				ls, primaryChain := newByronGenesisAnchorTestLedger(
					t,
					lcommon.Blake2b256Hash([]byte("genesis")).String(),
				)
				if !atOrigin {
					require.NoError(
						t,
						primaryChain.AddBlock(
							context.Background(),
							loadBoundaryBlock(
								t,
								"mainnet-byron-last-4492799.cbor",
								gledger.BlockTypeByronMain,
							),
							nil,
						),
					)
				}
				var err error
				require.NotPanics(t, func() {
					err = ls.validateByronPBFTHeaderCrypto(
						context.Background(),
						block,
					)
				})
				require.ErrorContains(t, err, "nil header")
			})
		}
	}
}

// byronParamUpdate is the protocol parameters a test proposal changes; a nil
// field is left unchanged.
type byronParamUpdate struct {
	maxBlockSize, maxHeaderSize *uint64
	// feeSummandNano and feeMultiplierNano are set together.
	feeSummandNano, feeMultiplierNano *uint64
}

// newByronParamUpdateProposal returns a signed proposal for version that
// changes the parameters in update, and the proposal's id.
func newByronParamUpdateProposal(
	t *testing.T,
	protocolMagic uint32,
	proposer byronPBFTTestKey,
	version [3]uint64,
	update byronParamUpdate,
) ([]byte, []byte) {
	t.Helper()
	optional := func(value *uint64) []any {
		if value == nil {
			return []any{}
		}
		return []any{*value}
	}
	feePolicy := []any{}
	if update.feeSummandNano != nil {
		inner, err := cbor.Encode([]any{
			*update.feeSummandNano, *update.feeMultiplierNano,
		})
		require.NoError(t, err)
		feePolicy = []any{[]any{uint8(0), cbor.WrappedCbor(inner)}}
	}
	fields := make([]cbor.RawMessage, 0, 5)
	for _, value := range []any{
		[]any{uint16(version[0]), uint16(version[1]), uint8(version[2])},
		[]any{
			[]any{}, []any{}, optional(update.maxBlockSize),
			optional(update.maxHeaderSize), []any{}, []any{}, []any{},
			[]any{}, []any{}, []any{}, []any{}, []any{}, feePolicy, []any{},
		},
		[]any{"dingo-test", uint32(1)},
		map[string]any{},
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
		proposer.privateKey,
		byronUpdateSigned(
			t,
			byron.SignTagUSProposal,
			protocolMagic,
			signedBody,
		),
	)
	raw, err := cbor.Encode([]any{
		fields[0], fields[1], fields[2], fields[3], fields[4],
		proposer.verificationKey, signature,
	})
	require.NoError(t, err)
	var proposal byron.ByronUpdateProposal
	_, err = cbor.Decode(raw, &proposal)
	require.NoError(t, err)
	return proposal.Cbor(), lcommon.Blake2b256Hash(proposal.Cbor()).Bytes()
}

// byronAdoptionChain is a Byron chain, k = 10 so an epoch is 100 slots, that
// adopts two updates in turn: update A at the first block of epoch 1 and
// update B at the first block of epoch 2. Each changes the block and header
// limits and the fee policy from what came before.
type byronAdoptionChain struct {
	magic    uint32
	issuer   byronPBFTTestKey
	delegate byronPBFTTestKey
	cert     []any
	blocks   []*byron.ByronMainBlock
	raw      []chain.RawBlock
	// probe is an empty main block; probeBlock and probeHeader are its sizes.
	probe       *byron.ByronMainBlock
	probeBlock  uint64
	probeHeader uint64
	genesis     wantByronParams
	adoptedA    wantByronParams
	adoptedB    wantByronParams
}

// Indexes into byronAdoptionChain.blocks.
const (
	blockGenesisOnlyLast = 3 // last block before update A is adopted
	blockFirstUnderA     = 4 // first block of epoch 1
	blockLastUnderA      = 7 // last block before update B is adopted
	blockFirstUnderB     = 8 // first block of epoch 2
	blockTip             = 9
)

// wantByronParams are the adopted parameters a test asserts.
type wantByronParams struct {
	maxBlockSize, maxHeaderSize uint64
	feeSummand                  uint64
	feeMultiplierNano           int64
}

func newByronAdoptionChain(t *testing.T) *byronAdoptionChain {
	t.Helper()
	c := &byronAdoptionChain{
		magic:    uint32(44),
		issuer:   newByronPBFTTestKey(0x91),
		delegate: newByronPBFTTestKey(0x92),
	}
	c.cert = newSignedByronPBFTDelegationCertificate(
		t, c.magic, 0, c.issuer, c.delegate,
	)
	template := loadRealByronMainBlock(t)
	newBlock := func(
		epoch, slot, number uint64,
		prev lcommon.Blake2b256,
		payload []byte,
		version byron.ByronBlockVersion,
	) *byron.ByronMainBlock {
		return newSignedByronPBFTBlockWithBody(
			t, template, c.magic, epoch, slot, number, prev,
			c.issuer, c.delegate, c.cert, nil,
			&byronPBFTBodyOverride{
				emptyTransactions: true,
				updatePayload:     payload,
				blockVersion:      &version,
			},
		)
	}
	c.probe = newBlock(
		2, 10, 99, lcommon.Blake2b256{}, byronUpdatePayload(nil),
		byron.ByronBlockVersion{Minor: 2},
	)
	c.probeBlock = uint64(len(c.probe.Cbor()))
	c.probeHeader = uint64(len(c.probe.Header().Cbor()))

	c.genesis = wantByronParams{
		maxBlockSize: 2_000_000, maxHeaderSize: 2_000_000,
		feeSummand: 155_381, feeMultiplierNano: 43_946_000_000,
	}
	c.adoptedA = wantByronParams{
		maxBlockSize: c.probeBlock, maxHeaderSize: c.probeHeader,
		feeSummand: 200_000, feeMultiplierNano: 100_000_000_000,
	}
	c.adoptedB = wantByronParams{
		maxBlockSize: 2 * c.probeBlock, maxHeaderSize: c.probeHeader - 1,
		feeSummand: 120_000, feeMultiplierNano: 20_000_000_000,
	}
	update := func(want wantByronParams) byronParamUpdate {
		summand := want.feeSummand * 1_000_000_000
		multiplier := uint64(want.feeMultiplierNano)
		return byronParamUpdate{
			maxBlockSize:      &want.maxBlockSize,
			maxHeaderSize:     &want.maxHeaderSize,
			feeSummandNano:    &summand,
			feeMultiplierNano: &multiplier,
		}
	}
	proposalA, idA := newByronParamUpdateProposal(
		t, c.magic, c.delegate, [3]uint64{0, 1, 0}, update(c.adoptedA),
	)
	proposalB, idB := newByronParamUpdateProposal(
		t, c.magic, c.delegate, [3]uint64{0, 2, 0}, update(c.adoptedB),
	)
	voteA := newByronUpdateVote(t, c.magic, c.delegate, idA, false)
	voteB := newByronUpdateVote(t, c.magic, c.delegate, idB, false)
	v0 := byron.ByronBlockVersion{}
	vA := byron.ByronBlockVersion{Minor: 1}
	vB := byron.ByronBlockVersion{Minor: 2}
	for i, spec := range []struct {
		epoch, slot uint64
		payload     []byte
		version     byron.ByronBlockVersion
	}{
		{0, 0, byronUpdatePayload(nil), v0},
		{0, 6, byronUpdatePayload(proposalA), v0},
		{0, 7, byronUpdatePayload(nil, voteA), v0},
		{0, 30, byronUpdatePayload(nil), vA},
		{1, 0, byronUpdatePayload(nil), vA},
		{1, 6, byronUpdatePayload(proposalB), vA},
		{1, 7, byronUpdatePayload(nil, voteB), vA},
		{1, 30, byronUpdatePayload(nil), vB},
		{2, 0, byronUpdatePayload(nil), vB},
		{2, 10, byronUpdatePayload(nil), vB},
	} {
		var prev lcommon.Blake2b256
		if i > 0 {
			prev = c.blocks[i-1].Hash()
		}
		c.blocks = append(c.blocks, newBlock(
			spec.epoch, spec.slot, uint64(i), prev, spec.payload, spec.version,
		))
		c.raw = append(c.raw, rawByronPBFTBlock(t, c.blocks[i]))
	}
	return c
}

// nodeConfig returns a Byron genesis whose limits and fee policy are the
// chain's genesis ones, except for the block and header limits, which are
// blockLimit when it is not zero.
func (c *byronAdoptionChain) nodeConfig(
	t *testing.T,
	blockLimit int,
) *cardano.CardanoNodeConfig {
	t.Helper()
	nodeConfig := newGeneratedByronPBFTTestNodeConfig(
		t, c.magic, 10, c.issuer, c.delegate, c.cert,
	)
	data := &nodeConfig.ByronGenesis().BlockVersionData
	data.MaxBlockSize = int(c.genesis.maxBlockSize)
	data.MaxHeaderSize = int(c.genesis.maxHeaderSize)
	if blockLimit != 0 {
		data.MaxBlockSize = blockLimit
		data.MaxHeaderSize = blockLimit
	}
	data.MaxTxSize = 100
	data.MaxProposalSize = 4_000
	data.TxFeePolicy.Summand = int64(c.genesis.feeSummand) * 1_000_000_000
	data.TxFeePolicy.Multiplier = c.genesis.feeMultiplierNano
	return nodeConfig
}

// newLedger returns a ledger with no cached Byron state, as after a restart,
// over a chain holding raw.
func (c *byronAdoptionChain) newLedger(
	t *testing.T,
	nodeConfig *cardano.CardanoNodeConfig,
	raw []chain.RawBlock,
) (*LedgerState, *chain.Chain) {
	t.Helper()
	cm, err := chain.NewManager(context.Background(), newTestDB(t), nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 10}))
	primary := cm.PrimaryChain()
	require.NoError(t, primary.AddRawBlocks(context.Background(), raw))
	return &LedgerState{
		chain:  primary,
		config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
	}, primary
}

func rawTip(raw chain.RawBlock) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(raw.Slot, raw.Hash),
		BlockNumber: raw.BlockNumber,
	}
}

// requireByronParams asserts the adopted limits and fee policy of params.
func requireByronParams(
	t *testing.T,
	params *eras.ByronProtocolParameters,
	want wantByronParams,
	msg string,
) {
	t.Helper()
	require.NotNil(t, params, msg)
	require.False(t, params.AdoptionUnknown, msg)
	require.Zero(
		t,
		params.MaxBlockSize.Cmp(new(big.Int).SetUint64(want.maxBlockSize)),
		"%s: maxBlockSize %s",
		msg,
		params.MaxBlockSize,
	)
	require.Zero(
		t,
		params.MaxHeaderSize.Cmp(new(big.Int).SetUint64(want.maxHeaderSize)),
		"%s: maxHeaderSize %s",
		msg,
		params.MaxHeaderSize,
	)
	require.Equal(
		t,
		want.feeSummand,
		params.TxFeeSummand,
		"%s: fee summand",
		msg,
	)
	require.Zero(
		t,
		params.TxFeeMultiplierNano.Cmp(big.NewInt(want.feeMultiplierNano)),
		"%s: fee multiplier %s",
		msg,
		params.TxFeeMultiplierNano,
	)
}

// stateAt rebuilds the update state at raw[index] and returns the parameters
// block application would validate the block at index with.
func stateAt(
	t *testing.T,
	ls *LedgerState,
	raw chain.RawBlock,
) (byronPBFTState, *eras.ByronProtocolParameters) {
	t.Helper()
	state, err := ls.byronPBFTStateAtTip(context.Background(), rawTip(raw))
	require.NoError(t, err)
	decoded, err := byron.NewByronMainBlockFromCbor(raw.Cbor)
	require.NoError(t, err)
	params, ok := byronBlockPParams(decoded, state, nil).(*eras.ByronProtocolParameters)
	require.True(t, ok)
	return state, params
}

// TestByronAdoptedParamsRestoredFromStoredChain covers and for a
// node that synced from genesis: a ledger with no cached state, as after a
// restart, replays the stored chain to the adopted limits and fee policy at
// every tip, and each of two successive adoptions replaces the previous one.
func TestByronAdoptedParamsRestoredFromStoredChain(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	nodeConfig := c.nodeConfig(t, 0)
	for _, test := range []struct {
		name string
		tip  int
		want wantByronParams
	}{
		{"genesis before any adoption", 0, c.genesis},
		{"proposal registered but not adopted", 2, c.genesis},
		{"last block before update A", blockGenesisOnlyLast, c.genesis},
		{"first block under update A", blockFirstUnderA, c.adoptedA},
		{"update B registered, A still adopted", blockLastUnderA, c.adoptedA},
		{"first block under update B", blockFirstUnderB, c.adoptedB},
		{"tip under update B", blockTip, c.adoptedB},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ls, _ := c.newLedger(t, nodeConfig, c.raw)
			state, params := stateAt(t, ls, c.raw[test.tip])
			require.True(t, state.update.Complete())
			requireByronParams(t, params, test.want, test.name)
		})
	}

	// After a restart at the tip the restored limits govern the next block.
	ls, _ := c.newLedger(t, nodeConfig, c.raw)
	_, params := stateAt(t, ls, c.raw[blockTip])
	err := validateByronBlockSizes(c.probe, params, nodeConfig)
	require.ErrorContains(t, err, "exceeds maxHeaderSize")
	fee, err := params.MinFee(200)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(124_000), fee)
}

// TestByronAdoptedParamsRollbackAcrossAdoptions covers the case where a
// rollback to before an adoption restores the parameters adopted before it,
// whether the ledger cached the state at the abandoned tip or not, and
// replaying forward adopts them again.
func TestByronAdoptedParamsRollbackAcrossAdoptions(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	nodeConfig := c.nodeConfig(t, 0)
	ls, primary := c.newLedger(t, nodeConfig, c.raw)

	cache := func(state byronPBFTState, raw chain.RawBlock) {
		ls.Lock()
		ls.byronPBFT = byronPBFTCache{
			state:       state,
			tip:         rawTip(raw).Point,
			initialized: true,
		}
		ls.Unlock()
	}
	state, params := stateAt(t, ls, c.raw[blockTip])
	requireByronParams(t, params, c.adoptedB, "tip")
	cache(state, c.raw[blockTip])

	// Roll back across update B's adoption.
	require.NoError(
		t,
		primary.RollbackUnbounded(
			context.Background(),
			rawTip(c.raw[blockLastUnderA]).Point,
		),
	)
	state, params = stateAt(t, ls, c.raw[blockLastUnderA])
	requireByronParams(t, params, c.adoptedA, "rolled back across B")
	cache(state, c.raw[blockLastUnderA])

	// Roll back across update A's adoption as well.
	require.NoError(
		t,
		primary.RollbackUnbounded(
			context.Background(),
			rawTip(c.raw[blockGenesisOnlyLast]).Point,
		),
	)
	state, params = stateAt(t, ls, c.raw[blockGenesisOnlyLast])
	requireByronParams(t, params, c.genesis, "rolled back across A")
	cache(state, c.raw[blockGenesisOnlyLast])

	// Replaying forward from the cached ancestor adopts both again.
	require.NoError(
		t,
		primary.AddRawBlocks(
			context.Background(),
			c.raw[blockGenesisOnlyLast+1:],
		),
	)
	_, params = stateAt(t, ls, c.raw[blockTip])
	requireByronParams(t, params, c.adoptedB, "replayed forward")
}

// TestByronAdoptedParamsForkDoesNotInheritAbandonedAdoption covers
// fork switching: when the chain switches to a fork that never endorsed update A, a
// cached state from the abandoned fork's adoption is not reused, though the
// new tip is later than the cached one.
func TestByronAdoptedParamsForkDoesNotInheritAbandonedAdoption(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	nodeConfig := c.nodeConfig(t, 0)
	ls, primary := c.newLedger(t, nodeConfig, c.raw[:blockFirstUnderA+1])
	state, params := stateAt(t, ls, c.raw[blockFirstUnderA])
	requireByronParams(t, params, c.adoptedA, "abandoned fork")
	ls.Lock()
	ls.byronPBFT = byronPBFTCache{
		state:       state,
		tip:         rawTip(c.raw[blockFirstUnderA]).Point,
		initialized: true,
	}
	ls.Unlock()

	// The fork keeps the confirmed proposal but never endorses it, and its
	// first epoch-1 block is one slot later than the abandoned one.
	require.NoError(
		t,
		primary.RollbackUnbounded(context.Background(), rawTip(c.raw[2]).Point),
	)
	template := loadRealByronMainBlock(t)
	unendorsed := newSignedByronPBFTBlockWithBody(
		t, template, c.magic, 0, 30, 3, c.blocks[2].Hash(),
		c.issuer, c.delegate, c.cert, nil,
		&byronPBFTBodyOverride{
			emptyTransactions: true,
			updatePayload:     byronUpdatePayload(nil),
			blockVersion:      &byron.ByronBlockVersion{},
		},
	)
	nextEpoch := newSignedByronPBFTBlockWithBody(
		t, template, c.magic, 1, 1, 4, unendorsed.Hash(),
		c.issuer, c.delegate, c.cert, nil,
		&byronPBFTBodyOverride{
			emptyTransactions: true,
			updatePayload:     byronUpdatePayload(nil),
			blockVersion:      &byron.ByronBlockVersion{},
		},
	)
	forkRaw := []chain.RawBlock{
		rawByronPBFTBlock(t, unendorsed), rawByronPBFTBlock(t, nextEpoch),
	}
	require.NoError(t, primary.AddRawBlocks(context.Background(), forkRaw))
	_, params = stateAt(t, ls, forkRaw[1])
	requireByronParams(t, params, c.genesis, "fork without an adoption")
}

// TestByronTrustedMidByronStartHasUnknownAdoption covers the contract for a
// node whose stored chain starts inside Byron: the updates adopted before its
// first block are unknowable, so the state is marked incomplete and the size
// and fee limits, both those of block application and those of the tip the
// mempool validates against, are not enforced from genesis values.
func TestByronTrustedMidByronStartHasUnknownAdoption(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	// A genesis that would reject the probe block on either limit.
	nodeConfig := c.nodeConfig(t, 1)
	ls, _ := c.newLedger(t, nodeConfig, c.raw[blockFirstUnderA:])

	state, params := stateAt(t, ls, c.raw[blockTip])
	require.False(t, state.update.Complete())
	require.True(t, params.AdoptionUnknown)
	require.NoError(t, validateByronBlockSizes(c.probe, params, nodeConfig))

	// The same parameters, were they treated as known, reject the block.
	known := params.Clone()
	known.AdoptionUnknown = false
	require.ErrorContains(
		t,
		validateByronBlockSizes(c.probe, known, nodeConfig),
		"exceeds maxHeaderSize",
	)

	ls.Lock()
	ls.byronPBFT = byronPBFTCache{
		state:       state,
		tip:         rawTip(c.raw[blockTip]).Point,
		initialized: true,
	}
	ls.Unlock()
	atTip, err := ls.ByronProtocolParameters()
	require.NoError(t, err)
	require.True(t, atTip.AdoptionUnknown)

	// A chain that starts at genesis enforces the same limits.
	full, _ := c.newLedger(t, nodeConfig, c.raw[:1])
	fullState, fullParams := stateAt(t, full, c.raw[0])
	require.True(t, fullState.update.Complete())
	require.False(t, fullParams.AdoptionUnknown)
	require.ErrorContains(
		t,
		validateByronBlockSizes(c.probe, fullParams, nodeConfig),
		"exceeds maxHeaderSize",
	)
}

// TestByronAdoptedUpdateChangesSizeLimitsAtAdoptionPoint covers the
// criterion that an adopted update's ppMaxBlockSize and ppMaxHeaderSize
// govern inbound regular-block validation from the adoption point. A real
// proposal is registered, voted, endorsed and stable through the update
// state; with k = 10 the epoch is 100 slots, so it is adopted by the first
// block of epoch 1. The block in the last slot of epoch 0 is measured against
// the limits before adoption and the first block of epoch 1 against the
// adopted ones, using the parameters block application takes from the state.
func TestByronAdoptedUpdateChangesSizeLimitsAtAdoptionPoint(t *testing.T) {
	t.Parallel()
	const (
		protocolMagic = uint32(44)
		securityParam = 10
		wide          = uint64(2_000_000)
	)
	issuer := newByronPBFTTestKey(0x91)
	delegate := newByronPBFTTestKey(0x92)
	certificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, delegate,
	)
	template := loadRealByronMainBlock(t)
	newBlock := func(
		epoch, slot, number uint64,
		payload []byte,
		version byron.ByronBlockVersion,
	) *byron.ByronMainBlock {
		return newSignedByronPBFTBlockWithBody(
			t, template, protocolMagic, epoch, slot, number,
			lcommon.Blake2b256{}, issuer, delegate, certificate, nil,
			&byronPBFTBodyOverride{
				emptyTransactions: true,
				updatePayload:     payload,
				blockVersion:      &version,
			},
		)
	}
	current := byron.ByronBlockVersion{}
	adoptedVersion := byron.ByronBlockVersion{Minor: 1}
	lastBefore := newBlock(0, 99, 4, byronUpdatePayload(nil), current)
	firstAfter := newBlock(1, 0, 4, byronUpdatePayload(nil), current)
	blockSizes := [2]uint64{
		uint64(len(lastBefore.Cbor())), uint64(len(firstAfter.Cbor())),
	}
	headerSizes := [2]uint64{
		uint64(len(lastBefore.Header().Cbor())),
		uint64(len(firstAfter.Header().Cbor())),
	}
	lowest := func(sizes [2]uint64) uint64 { return min(sizes[0], sizes[1]) }
	highest := func(sizes [2]uint64) uint64 { return max(sizes[0], sizes[1]) }
	ptr := func(v uint64) *uint64 { return &v }

	tests := []struct {
		name string
		// genesis and adopted are {maxBlockSize, maxHeaderSize}.
		genesis, adopted [2]uint64
		// beforeErr and afterErr are the substrings the last block of
		// epoch 0 and the first block of epoch 1 must be rejected with, or
		// empty when they must be accepted.
		beforeErr, afterErr string
	}{
		{
			"lower the block limit below the block",
			[2]uint64{wide, wide},
			[2]uint64{blockSizes[1] - 1, wide},
			"", "exceeds maxBlockSize",
		},
		{
			"lower the block limit to exactly the block",
			[2]uint64{wide, wide},
			[2]uint64{blockSizes[1], wide},
			"", "",
		},
		{
			"lower the header limit below the header",
			[2]uint64{wide, wide},
			[2]uint64{wide, headerSizes[1] - 1},
			"", "exceeds maxHeaderSize",
		},
		{
			"lower the header limit to exactly the header",
			[2]uint64{wide, wide},
			[2]uint64{wide, headerSizes[1]},
			"", "",
		},
		{
			"raise the block limit to admit the block",
			[2]uint64{lowest(blockSizes) - 1, wide},
			[2]uint64{highest(blockSizes), wide},
			"exceeds maxBlockSize", "",
		},
		{
			"raise the header limit to admit the header",
			[2]uint64{wide, lowest(headerSizes) - 1},
			[2]uint64{wide, highest(headerSizes)},
			"exceeds maxHeaderSize", "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			nodeConfig := newGeneratedByronPBFTTestNodeConfig(
				t, protocolMagic, securityParam, issuer, delegate, certificate,
			)
			limits := &nodeConfig.ByronGenesis().BlockVersionData
			limits.MaxBlockSize = int(test.genesis[0])
			limits.MaxHeaderSize = int(test.genesis[1])
			// A transaction and a proposal must fit the smallest limits.
			limits.MaxTxSize = 100
			limits.MaxProposalSize = 700
			ls := &LedgerState{
				config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
			}
			ls.slotClock = NewSlotClock(
				newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
				DefaultSlotClockConfig(),
			)
			config, err := ls.byronPBFTConfig()
			require.NoError(t, err)
			genesisParams, err := ls.byronGenesisProtocolParameters()
			require.NoError(t, err)
			state, err := newByronPBFTState(config, genesisParams)
			require.NoError(t, err)
			state.update = state.update.Advance(0, 0)

			proposal, proposalId := newByronUpdateProposal(
				t, protocolMagic, delegate, [3]uint64{0, 1, 0},
				ptr(test.adopted[0]), ptr(test.adopted[1]),
			)
			vote := newByronUpdateVote(
				t, protocolMagic, delegate, proposalId, false,
			)
			// Header validation is off: one genesis key signs every block
			// here and would exceed the PBFT signature window. A rejected
			// update payload is then only logged, so the adopted version
			// asserted below is what shows the proposal registered.
			// Registered and confirmed in epoch 0, endorsed once confirmation
			// is 2k slots old, and stable for 4k slots by the next epoch.
			for _, block := range []*byron.ByronMainBlock{
				newBlock(0, 6, 1, byronUpdatePayload(proposal), current),
				newBlock(0, 7, 2, byronUpdatePayload(nil, vote), current),
				newBlock(0, 30, 3, byronUpdatePayload(nil), adoptedVersion),
			} {
				state, err = ls.advanceByronPBFTState(context.Background(), state, block, false)
				require.NoError(t, err)
			}
			require.Zero(t, state.update.AdoptedVersion().Minor)

			check := func(
				block *byron.ByronMainBlock,
				wantVersion uint16,
				wantErr string,
			) {
				t.Helper()
				next, err := ls.advanceByronPBFTState(context.Background(), state, block, false)
				require.NoError(t, err)
				require.Equal(t, wantVersion, next.update.AdoptedVersion().Minor)
				params, ok := byronBlockPParams(block, next, nil).(*eras.ByronProtocolParameters)
				require.True(t, ok)
				err = validateByronBlockSizes(block, params, nodeConfig)
				if wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, wantErr)
			}
			check(lastBefore, 0, test.beforeErr)
			check(firstAfter, 1, test.afterErr)
		})
	}
}

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
	"crypto/ed25519"
	"crypto/sha3"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

// byronBootstrapKey is a Byron key whose address can be spent with a
// bootstrap witness.
type byronBootstrapKey struct {
	key       ed25519.PrivateKey
	chainCode []byte
	attrBytes []byte
	addr      lcommon.Address
}

// newByronBootstrapKey derives a Byron address whose root commits to the
// key, chain code and attributes, as the bootstrap witness check recomputes:
// blake2b_224(sha3_256([0, [0, pubkey || chainCode], attributes])).
func newByronBootstrapKey(
	t *testing.T,
	attrs lcommon.ByronAddressAttributes,
) byronBootstrapKey {
	t.Helper()
	k := byronBootstrapKey{
		key: ed25519.NewKeyFromSeed(
			bytes.Repeat([]byte{0xb5}, ed25519.SeedSize),
		),
		chainCode: bytes.Repeat([]byte{0xcc}, 32),
	}
	var err error
	k.attrBytes, err = attrs.MarshalCBOR()
	require.NoError(t, err)
	spendingData := append(
		[]byte(k.key.Public().(ed25519.PublicKey)), k.chainCode...,
	)
	rootCbor, err := cbor.Encode([]any{
		uint64(0),
		[]any{uint64(0), spendingData},
		cbor.RawMessage(k.attrBytes),
	})
	require.NoError(t, err)
	sum := sha3.Sum256(rootCbor)
	k.addr, err = lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		lcommon.Blake2b224Hash(sum[:]).Bytes(),
		attrs,
	)
	require.NoError(t, err)
	return k
}

// seedByronTxOut stores an unspent output at a Byron address.
func seedByronTxOut(
	t *testing.T,
	db *database.Database,
	txId []byte,
	byronAddr lcommon.Address,
) {
	t.Helper()
	addrBytes, err := byronAddr.Bytes()
	require.NoError(t, err)
	encoded, err := cbor.Encode(
		map[uint]any{0: addrBytes, 1: uint64(10_000_000)},
	)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
			if err := db.CreateUtxo(t.Context(), txn, &models.Utxo{
				TxId: txId, OutputIdx: 0, AddedSlot: 1,
			}); err != nil {
				return err
			}
			return db.Blob().SetUtxo(txn.Blob(), txId, 0, encoded)
		}),
	)
}

// byronReplayTxCbor returns a signed, balanced V1 script spend whose output
// is at outputAddr. When refTxId is set it names refTxId as a reference
// input; when byronSpend is set it also spends output 0 of byronTxId, a
// Byron UTxO holding 10 ADA, under a bootstrap witness. The script accepts
// any context, so only the Plutus context construction can reject the
// transaction.
func byronReplayTxCbor(
	t *testing.T,
	f *requiredDatumFixture,
	costModels map[uint][]int64,
	outputAddr lcommon.Address,
	refTxId []byte,
	byronSpend *byronBootstrapKey,
	byronTxId []byte,
	valid bool,
	era eras.EraDesc,
) []byte {
	t.Helper()
	taggedSets := era.Id == eras.ConwayEraDesc.Id
	outputBytes, err := outputAddr.Bytes()
	require.NoError(t, err)
	// Conway encodes redeemers as a map; Babbage as an array.
	var redeemers any = []any{[]any{
		uint64(lcommon.RedeemerTagSpend), uint64(0),
		f.datum, []uint64{1_000_000, 1_000_000_000},
	}}
	if taggedSets {
		redeemers = map[any]any{
			[2]uint64{uint64(lcommon.RedeemerTagSpend), 0}: []any{
				f.datum, []uint64{1_000_000, 1_000_000_000},
			},
		}
	}
	redeemersCbor, err := cbor.Encode(redeemers)
	require.NoError(t, err)
	langViews, err := lcommon.EncodeLangViews(
		map[uint]struct{}{0: {}}, costModels,
	)
	require.NoError(t, err)
	datumsCbor, err := cbor.Encode([]any{f.datum})
	require.NoError(t, err)
	integrity := lcommon.Blake2b256Hash(
		bytes.Join([][]byte{redeemersCbor, datumsCbor, langViews}, nil),
	)
	// Babbage and earlier encode input sets as plain arrays; Conway tags them.
	inputSet := func(txIds ...[]byte) any {
		set := make([]any, 0, len(txIds))
		for _, txId := range txIds {
			set = append(set, []any{txId, uint(0)})
		}
		if taggedSets {
			return cbor.Tag{Number: 258, Content: set}
		}
		return set
	}
	// The Byron TxId sorts after the script input's, so the spend redeemer
	// keeps index 0.
	spendIds := [][]byte{f.spendTxId}
	outputValue := uint64(9_999_998)
	if byronSpend != nil {
		require.Positive(t, bytes.Compare(byronTxId, f.spendTxId))
		spendIds = append(spendIds, byronTxId)
		outputValue += 10_000_000
	}
	// Alonzo outputs are arrays; later eras use the map form.
	var output any = map[uint]any{0: outputBytes, 1: outputValue}
	if era.Id == eras.AlonzoEraDesc.Id {
		output = []any{outputBytes, outputValue}
	}
	body := map[uint]any{
		0:  inputSet(spendIds...),
		1:  []any{output},
		2:  uint64(2),
		11: integrity.Bytes(),
		13: inputSet(f.collateralTxId),
	}
	if refTxId != nil {
		body[18] = inputSet(refTxId)
	}
	bodyCbor, err := cbor.Encode(body)
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(bodyCbor)
	witnessSet := f.witnessSetMap(true)
	publicKey := f.key.Public().(ed25519.PublicKey)
	witnessSet[0] = []any{[]any{
		[]byte(publicKey),
		ed25519.Sign(f.key, bodyHash.Bytes()),
	}}
	if byronSpend != nil {
		witnessSet[2] = []any{[]any{
			[]byte(byronSpend.key.Public().(ed25519.PublicKey)),
			ed25519.Sign(byronSpend.key, bodyHash.Bytes()),
			byronSpend.chainCode,
			byronSpend.attrBytes,
		}}
	}
	witnessSet[5] = cbor.RawMessage(redeemersCbor)
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(bodyCbor), witnessSet, valid, nil},
	)
	require.NoError(t, err)
	return txCbor
}

// babbageTestBlock wraps a single Babbage transaction in a block that
// replays at the given protocol version.
func babbageTestBlock(
	t *testing.T,
	txCbor []byte,
	major uint64,
	slot uint64,
) *babbage.BabbageBlock {
	t.Helper()
	tx, err := babbage.NewBabbageTransactionFromCbor(txCbor)
	require.NoError(t, err)
	block := &babbage.BabbageBlock{
		BlockHeader: &babbage.BabbageBlockHeader{},
		TransactionBodies: []babbage.BabbageTransactionBody{
			tx.Body,
		},
		TransactionWitnessSets: []babbage.BabbageTransactionWitnessSet{
			tx.WitnessSet,
		},
	}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = slot
	block.BlockHeader.Body.ProtoVersion.Major = major
	if !tx.IsValid() {
		block.InvalidTransactions = []uint{0}
	}
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = bodySize
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	return block
}

// alonzoTestBlock wraps a single Alonzo transaction in a block that replays
// at the given protocol version.
func alonzoTestBlock(
	t *testing.T,
	txCbor []byte,
	major uint64,
	slot uint64,
) *alonzo.AlonzoBlock {
	t.Helper()
	tx, err := alonzo.NewAlonzoTransactionFromCbor(txCbor)
	require.NoError(t, err)
	block := &alonzo.AlonzoBlock{
		BlockHeader: &alonzo.AlonzoBlockHeader{
			ShelleyBlockHeader: shelley.ShelleyBlockHeader{
				Body: shelley.ShelleyBlockHeaderBody{
					BlockNumber:       1,
					Slot:              slot,
					ProtoMajorVersion: major,
				},
			},
		},
		TransactionBodies: []alonzo.AlonzoTransactionBody{tx.Body},
		TransactionWitnessSets: []alonzo.AlonzoTransactionWitnessSet{
			tx.WitnessSet,
		},
	}
	if !tx.IsValid() {
		block.InvalidTransactions = []uint{0}
	}
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = bodySize
	block.SetCbor(nil)
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	return block
}

// TestLedgerReplayRejectsByronTxOutInPlutusV1ContextBeforeStateMutation
// replays Alonzo, Babbage and Conway blocks whose Plutus V1 spend places a
// Byron address on a spent input, an output or a reference input. The
// reference rejects a Byron TxOut in a Babbage or later context for every
// language version, so those blocks must fail in transaction validation for
// either isValid value, before the ledger tip or any input moves. Alonzo
// drops Byron TxOuts from a V1 context instead, so its blocks apply. The
// controls carry no Byron address and apply, so the fixture fails on nothing
// else.
func TestLedgerReplayRejectsByronTxOutInPlutusV1ContextBeforeStateMutation(
	t *testing.T,
) {
	t.Parallel()

	// The test ledger runs on a testnet, and a Byron address names its
	// network only through a network magic attribute.
	magic := uint32(1)
	byronKey := newByronBootstrapKey(
		t, lcommon.ByronAddressAttributes{Network: &magic},
	)
	byronAddr := byronKey.addr
	refTxId := bytes.Repeat([]byte{0xd4}, 32)
	byronSpendTxId := bytes.Repeat([]byte{0xd5}, 32)

	for _, era := range []eras.EraDesc{
		eras.AlonzoEraDesc, eras.BabbageEraDesc, eras.ConwayEraDesc,
	} {
		for _, tc := range []struct {
			name      string
			byronIn   bool
			byronOut  bool
			byronRef  bool
			valid     bool
			wantApply bool
		}{
			{name: "byron spent input/isValid=true", byronIn: true, valid: true},
			{name: "byron spent input/isValid=false", byronIn: true},
			{name: "byron output/isValid=true", byronOut: true, valid: true},
			{name: "byron output/isValid=false", byronOut: true},
			{name: "byron reference input/isValid=true", byronRef: true, valid: true},
			{name: "byron reference input/isValid=false", byronRef: true},
			{name: "no byron address/isValid=true", valid: true, wantApply: true},
		} {
			// Reference inputs do not exist in Alonzo, and Babbage refuses a V1
			// script alongside them, so the reference case reaches the context
			// only from Conway.
			if tc.byronRef && era.Id != eras.ConwayEraDesc.Id {
				continue
			}
			// Alonzo drops a Byron input or output from a V1 context, so the
			// transaction applies. The script ignores its context, so the
			// dropped TxOut shows only as the absence of a rejection.
			wantApply := tc.wantApply
			if era.Id == eras.AlonzoEraDesc.Id && (tc.byronOut || tc.byronIn) {
				if !tc.valid {
					continue
				}
				wantApply = true
			}
			t.Run(era.Name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				f := newRequiredDatumFixture(t, alwaysSucceedsV1(t), true)
				costModels := map[uint][]int64{
					0: blockV3MachineCostModel(t, lang.LanguageVersionV1),
				}
				keyAddr, err := lcommon.NewAddressFromParts(
					lcommon.AddressTypeKeyNone,
					lcommon.AddressNetworkTestnet,
					lcommon.Blake2b224Hash(
						f.key.Public().(ed25519.PublicKey),
					).Bytes(),
					nil,
				)
				require.NoError(t, err)
				outputAddr := keyAddr
				if tc.byronOut {
					outputAddr = byronAddr
				}
				var refId []byte
				if tc.byronRef {
					refId = refTxId
					seedByronTxOut(t, f.db, refId, byronAddr)
				}
				var byronSpend *byronBootstrapKey
				if tc.byronIn {
					byronSpend = &byronKey
					seedByronTxOut(t, f.db, byronSpendTxId, byronAddr)
				}
				txCbor := byronReplayTxCbor(
					t, f, costModels, outputAddr, refId, byronSpend,
					byronSpendTxId, tc.valid, era,
				)
				execCosts := lcommon.ExUnitPrice{
					MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1_000_000_000)},
					StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1_000_000_000)},
				}
				maxTx := lcommon.ExUnits{
					Memory: 10_000_000,
					Steps:  10_000_000_000,
				}
				maxBlock := lcommon.ExUnits{
					Memory: 50_000_000, Steps: 50_000_000_000,
				}
				var pparams lcommon.ProtocolParameters
				var block gledger.Block
				var blockType uint
				switch era.Id {
				case eras.AlonzoEraDesc.Id:
					pparams = &alonzo.AlonzoProtocolParameters{
						ProtocolMajor:        6,
						MaxBlockBodySize:     100_000,
						MaxBlockHeaderSize:   100_000,
						MaxTxSize:            16_384,
						MaxValueSize:         5_000,
						MaxCollateralInputs:  3,
						CollateralPercentage: 150,
						CostModels:           costModels,
						ExecutionCosts:       execCosts,
						MaxTxExUnits:         maxTx,
						MaxBlockExUnits:      maxBlock,
					}
					block = alonzoTestBlock(
						t, txCbor, 6, dijkstraCollateralReturnTestSlot,
					)
					blockType = uint(gledger.BlockTypeAlonzo)
				case eras.BabbageEraDesc.Id:
					pparams = &babbage.BabbageProtocolParameters{
						ProtocolMajor:        8,
						MaxBlockBodySize:     100_000,
						MaxBlockHeaderSize:   100_000,
						MaxTxSize:            16_384,
						MaxValueSize:         5_000,
						MaxCollateralInputs:  3,
						CollateralPercentage: 150,
						CostModels:           costModels,
						ExecutionCosts:       execCosts,
						MaxTxExUnits:         maxTx,
						MaxBlockExUnits:      maxBlock,
					}
					block = babbageTestBlock(
						t, txCbor, 8, dijkstraCollateralReturnTestSlot,
					)
					blockType = uint(gledger.BlockTypeBabbage)
				default:
					pparams = &conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: lcommon.ProtocolVersionPlomin,
						},
						MaxBlockBodySize:     100_000,
						MaxBlockHeaderSize:   100_000,
						MaxTxSize:            16_384,
						MaxValueSize:         5_000,
						MaxCollateralInputs:  3,
						CollateralPercentage: 150,
						CostModels:           costModels,
						ExecutionCosts:       execCosts,
						MaxTxExUnits:         maxTx,
						MaxBlockExUnits:      maxBlock,
					}
					block, _ = conwayTestBlock(
						t, txCbor, uint(lcommon.ProtocolVersionPlomin),
						dijkstraCollateralReturnTestSlot,
					)
					blockType = uint(gledger.BlockTypeConway)
				}
				ls := newReplayTestLedger(
					t, f.db, block, blockType, era, pparams,
				)
				inputIds := [][]byte{f.spendTxId, f.collateralTxId}
				if tc.byronRef {
					inputIds = append(inputIds, refId)
				}
				if tc.byronIn {
					inputIds = append(inputIds, byronSpendTxId)
				}
				before := make([]*models.Utxo, 0, len(inputIds))
				for _, id := range inputIds {
					utxo, err := f.db.UtxoByRef(t.Context(), id, 0, nil)
					require.NoError(t, err)
					before = append(before, utxo)
				}

				err = replayTestBlock(ls, block)
				tip, tipErr := f.db.GetTip(nil)
				require.NoError(t, tipErr)
				if wantApply {
					require.NoError(t, err)
					require.Equal(t, block.Hash().Bytes(), tip.Point.Hash)
					return
				}
				var ctxErr conway.ScriptContextConstructionError
				require.ErrorAs(t, err, &ctxErr)
				require.ErrorContains(t, err, "Byron TxOut")
				require.Equal(t, ochainsync.Tip{}, tip)
				for i, id := range inputIds {
					utxo, err := f.db.UtxoByRef(t.Context(), id, 0, nil)
					require.NoError(t, err)
					require.Equal(t, before[i], utxo)
				}
			})
		}
	}
}

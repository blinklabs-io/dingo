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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type ppupProductionTx struct {
	*mockledger.MockTransaction
	epoch   uint64
	updates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate
}

func (tx ppupProductionTx) ProtocolParameterUpdates() (
	uint64,
	map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	return tx.epoch, tx.updates
}

type ppupProductionBlock struct {
	header lcommon.BlockHeader
	tx     lcommon.Transaction
}

func (b *ppupProductionBlock) Type() int { return shelley.BlockTypeShelley }

func (b *ppupProductionBlock) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256Hash([]byte("classic-ppup-production-path-block"))
}

func (b *ppupProductionBlock) Header() lcommon.BlockHeader { return b.header }
func (b *ppupProductionBlock) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}
func (b *ppupProductionBlock) BlockNumber() uint64 { return 1 }
func (b *ppupProductionBlock) SlotNumber() uint64  { return 4_492_800 }
func (b *ppupProductionBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *ppupProductionBlock) BlockBodySize() uint64 { return 1 }
func (b *ppupProductionBlock) Era() lcommon.Era      { return shelley.EraShelley }
func (b *ppupProductionBlock) Transactions() []lcommon.Transaction {
	return []lcommon.Transaction{b.tx}
}
func (b *ppupProductionBlock) Cbor() []byte { return []byte{0x82, 0x80, 0x80} }
func (b *ppupProductionBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}
func (b *ppupProductionBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

// TestClassicPPUPValidationPrecedesBlockApply proves that the registered era
// validator rejects unauthorized PPUP proposals before block application can
// persist them, while an authorized proposal is stored by the apply path.
func TestClassicPPUPValidationPrecedesBlockApply(t *testing.T) {
	t.Parallel()

	const epoch = uint64(208)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x17
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var genesisKey lcommon.Blake2b224
	copy(genesisKey[:], bytes.Repeat([]byte{0x11}, lcommon.Blake2b224Size))
	delegate := lcommon.Blake2b224Hash(publicKey)
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId:       epoch,
			StartSlot:     4_492_800,
			LengthInSlots: 432_000,
		},
		config: LedgerStateConfig{
			Logger: testLogger(),
			CardanoNodeConfig: newGenesisDelegateShelleyGenesisCfg(
				t,
				hex.EncodeToString(delegate[:]),
				strings.Repeat("bb", lcommon.Blake2b256Size),
			),
		},
	}
	ls.config.CardanoNodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.publishSnapshotsLocked()
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		delegate[:],
		nil,
	)
	require.NoError(t, err)
	inputTxID := seedBabbageUtxo(t, db, 0x31, 0, address, 1_000_000)
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId(inputTxID).
		WithIndex(0).
		Build()
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(address.String()).
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	ls.activeEras = []eras.EraDesc{eras.ShelleyEraDesc}
	ls.currentEra = eras.ShelleyEraDesc
	pparams := &shelley.ShelleyProtocolParameters{
		MaxTxSize:          100_000,
		MaxBlockBodySize:   100_000,
		MaxBlockHeaderSize: 100_000,
	}
	updateCbor := []byte{0xa1, 0x00, 0x01} // {minFeeA: 1}
	var update shelley.ShelleyProtocolParameterUpdate
	require.NoError(t, update.UnmarshalCBOR(updateCbor))

	newTx := func(key lcommon.Blake2b224) ppupProductionTx {
		id := lcommon.Blake2b256Hash([]byte("classic-ppup-production-path"))
		base := mockledger.NewTransactionBuilder()
		base.WithId(id[:])
		base.WithType(shelley.TxTypeShelley)
		base.WithValid(true)
		base.WithInputs(input)
		base.WithOutputs(output)
		base.WithWitnesses(mockledger.NewMockTransactionWitnessSet().WithVkeyWitnesses(
			lcommon.VkeyWitness{
				Vkey:      publicKey,
				Signature: ed25519.Sign(privateKey, id[:]),
			},
		))
		return ppupProductionTx{
			MockTransaction: base,
			epoch:           epoch,
			updates: map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate{
				key: update,
			},
		}
	}

	unknownKey := lcommon.Blake2b224Hash([]byte("unknown-ppup-genesis-key"))
	unauthorized := newTx(unknownKey)
	process := func(tx ppupProductionTx) error {
		block := &ppupProductionBlock{
			header: &shelley.ShelleyBlockHeader{},
			tx:     tx,
		}
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		offsets := &database.BlockIngestionResult{
			TxOffsets:   map[[32]byte]database.CborOffset{txHash: {}},
			UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
		}
		for _, produced := range tx.Produced() {
			var producedTxId [32]byte
			copy(producedTxId[:], produced.Id.Id().Bytes())
			offsets.UtxoOffsets[database.UtxoRef{
				TxId:      producedTxId,
				OutputIdx: uint32(produced.Id.Index()), //nolint:gosec // fixture output index is zero
			}] = database.CborOffset{BlockSlot: block.SlotNumber(), ByteLength: 1}
		}
		return db.Transaction(true).Do(func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(
				txn,
				ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
				block,
				true,
				false,
				false,
				nil,
				envelopeParent{origin: true},
				offsets,
				eras.ShelleyEraDesc,
				pparams,
				nil,
				0,
				0,
				false,
			)
			return err
		})
	}
	var delegateErr lcommon.ProtocolParameterUpdateDelegateError
	require.ErrorAs(
		t,
		process(unauthorized),
		&delegateErr,
	)
	stored, err := db.Metadata().GetPParamUpdates(epoch, nil)
	require.NoError(t, err)
	require.Empty(t, stored, "a rejected proposal must not reach the vote store")

	authorized := newTx(genesisKey)
	require.NoError(t, process(authorized))
	stored, err = db.Metadata().GetPParamUpdates(epoch, nil)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.True(t, bytes.Equal(genesisKey[:], stored[0].GenesisHash))
	require.Equal(t, updateCbor, stored[0].Cbor)
	require.Equal(t, uint64(4_492_800), stored[0].AddedSlot)
	require.Equal(t, epoch, stored[0].Epoch)
}

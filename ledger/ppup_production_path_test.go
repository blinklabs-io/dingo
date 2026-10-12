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
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type ppupProductionBlock struct {
	blockType int
	era       lcommon.Era
	slot      uint64
	tx        lcommon.Transaction
}

func (b *ppupProductionBlock) Type() int { return b.blockType }
func (b *ppupProductionBlock) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256Hash([]byte("classic-ppup-production-path-block"))
}
func (b *ppupProductionBlock) Header() lcommon.BlockHeader {
	return &shelley.ShelleyBlockHeader{}
}
func (b *ppupProductionBlock) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}
func (b *ppupProductionBlock) BlockNumber() uint64 { return 1 }
func (b *ppupProductionBlock) SlotNumber() uint64  { return b.slot }
func (b *ppupProductionBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *ppupProductionBlock) BlockBodySize() uint64 { return 1 }
func (b *ppupProductionBlock) Era() lcommon.Era      { return b.era }
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

// TestClassicPPUPRejectedBeforeBlockApply drives PPUP proposals through
// ledgerProcessBlock for every classic era: each era's registered validator
// must refuse an unauthorized or mistimed proposal before block application
// writes a vote row, and must still let an authorized proposal be stored.
func TestClassicPPUPRejectedBeforeBlockApply(t *testing.T) {
	t.Parallel()
	const (
		epoch  = uint64(208)
		start  = uint64(4_492_800)
		length = uint64(432_000)
		// 2 * ceiling(3k/f) for the test genesis: k=432, f=0.99.
		noReturn = start + length - 2_620
		quorum   = 5
	)
	eraCases := []struct {
		desc      eras.EraDesc
		blockType int
		txType    int
		era       lcommon.Era
		pparams   lcommon.ProtocolParameters
	}{
		{
			eras.ShelleyEraDesc, shelley.BlockTypeShelley, shelley.TxTypeShelley,
			shelley.EraShelley,
			&shelley.ShelleyProtocolParameters{
				MaxTxSize: 100_000, MaxBlockBodySize: 200_000, MaxBlockHeaderSize: 1_000,
			},
		},
		{
			eras.AllegraEraDesc, allegra.BlockTypeAllegra, allegra.TxTypeAllegra,
			allegra.EraAllegra,
			&allegra.AllegraProtocolParameters{
				MaxTxSize: 100_000, MaxBlockBodySize: 200_000, MaxBlockHeaderSize: 1_000,
			},
		},
		{
			eras.MaryEraDesc, mary.BlockTypeMary, mary.TxTypeMary,
			mary.EraMary,
			&mary.MaryProtocolParameters{
				MaxTxSize: 100_000, MaxBlockBodySize: 200_000, MaxBlockHeaderSize: 1_000,
			},
		},
		{
			eras.AlonzoEraDesc, alonzo.BlockTypeAlonzo, alonzo.TxTypeAlonzo,
			alonzo.EraAlonzo,
			&alonzo.AlonzoProtocolParameters{
				MaxTxSize: 100_000, MaxBlockBodySize: 200_000, MaxBlockHeaderSize: 1_000,
				MaxValueSize: 5_000, ProtocolMajor: 6,
			},
		},
		{
			eras.BabbageEraDesc, babbage.BlockTypeBabbage, babbage.TxTypeBabbage,
			babbage.EraBabbage,
			&babbage.BabbageProtocolParameters{
				MaxTxSize: 100_000, MaxBlockBodySize: 200_000, MaxBlockHeaderSize: 1_000,
				MaxValueSize: 5_000, ProtocolMajor: 7,
			},
		},
	}
	for _, ec := range eraCases {
		t.Run(ec.desc.Name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(
				t,
				&database.Config{DataDir: t.TempDir()},
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

			newKey := func(b byte) ed25519.PrivateKey {
				seed := make([]byte, ed25519.SeedSize)
				seed[0] = b
				return ed25519.NewKeyFromSeed(seed)
			}
			// The payer owns the spent output, so its witness satisfies the
			// UTxO witness rule and only the delegate's witness is optional.
			payerKey, delegateKey := newKey(0x18), newKey(0x17)
			vkey := func(k ed25519.PrivateKey) []byte {
				return k.Public().(ed25519.PublicKey)
			}
			// The only genDelegs key of newGenesisDelegateShelleyGenesisCfg.
			var genesisKey lcommon.Blake2b224
			copy(
				genesisKey[:],
				bytes.Repeat([]byte{0x11}, lcommon.Blake2b224Size),
			)
			delegate := lcommon.Blake2b224Hash(vkey(delegateKey))
			payer := lcommon.Blake2b224Hash(vkey(payerKey))
			ls := &LedgerState{
				db: db,
				currentEpoch: models.Epoch{
					EpochId:       epoch,
					StartSlot:     start,
					LengthInSlots: uint(length),
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
			ls.activeEras = []eras.EraDesc{ec.desc}
			ls.currentEra = ec.desc
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				lcommon.AddressNetworkTestnet,
				payer[:],
				nil,
			)
			require.NoError(t, err)
			addressBytes, err := address.Bytes()
			require.NoError(t, err)
			inputTxId := seedBabbageUtxo(t, db, 0x31, 0, address, 1_000_000)
			updateCbor := []byte{0xa1, 0x00, 0x01} // {minFeeA: 1}

			// newTx encodes a real era transaction spending the seeded
			// output and proposing updateCbor for target from each key,
			// witnessed by the payer and, when delegateSigns, the delegate.
			newTx := func(
				target uint64,
				delegateSigns bool,
				keys ...lcommon.Blake2b224,
			) lcommon.Transaction {
				require.Less(t, len(keys), 24)
				proposals := []byte{0xa0 + byte(len(keys))}
				for _, key := range keys {
					proposals = append(proposals, 0x58, lcommon.Blake2b224Size)
					proposals = append(proposals, key[:]...)
					proposals = append(proposals, updateCbor...)
				}
				body, err := cbor.Encode(map[uint]any{
					0: []any{[]any{inputTxId, uint(0)}},
					1: []any{[]any{addressBytes, uint64(1_000_000)}},
					2: uint64(0),
					3: start + length,
					6: []any{cbor.RawMessage(proposals), target},
				})
				require.NoError(t, err)
				id := lcommon.Blake2b256Hash(body)
				signers := []ed25519.PrivateKey{payerKey}
				if delegateSigns {
					signers = append(signers, delegateKey)
				}
				vkeyWitnesses := make([]any, 0, len(signers))
				for _, k := range signers {
					vkeyWitnesses = append(
						vkeyWitnesses,
						[]any{vkey(k), ed25519.Sign(k, id[:])},
					)
				}
				witnesses := map[uint]any{0: vkeyWitnesses}
				fields := []any{cbor.RawMessage(body), witnesses}
				if ec.txType >= alonzo.TxTypeAlonzo {
					fields = append(fields, true)
				}
				fields = append(fields, nil)
				txCbor, err := cbor.Encode(fields)
				require.NoError(t, err)
				tx, err := gledger.NewTransactionFromCbor(
					uint(ec.txType),
					txCbor,
				) //nolint:gosec // era tx types are small
				require.NoError(t, err)
				return tx
			}
			process := func(slot uint64, tx lcommon.Transaction) error {
				block := &ppupProductionBlock{
					blockType: ec.blockType,
					era:       ec.era,
					slot:      slot,
					tx:        tx,
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
					}] = database.CborOffset{BlockSlot: slot, ByteLength: 1}
				}
				return db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(
						context.Background(),
						txn,
						ocommon.NewPoint(slot, block.Hash().Bytes()),
						block,
						true,
						false,
						false,
						nil,
						envelopeParent{origin: true},
						offsets,
						ec.desc,
						ec.pparams,
						nil,
						0,
						0,
						false,
					)
					return err
				})
			}
			storedRows := func() []models.PParamUpdate {
				rows, err := db.Metadata().GetPParamUpdates(epoch, nil)
				require.NoError(t, err)
				return rows
			}
			// enactsFromStore reports whether the boundary after submission
			// enacts an update from the rows stored for submission.
			enactsFromStore := func(submission uint64) bool {
				pp := ec.pparams
				before, err := cbor.Encode(pp)
				require.NoError(t, err)
				require.NoError(t, db.ApplyPParamUpdates(
					context.Background(),
					0, submission+1, ec.desc.Id, quorum, &pp,
					ec.desc.DecodePParamsUpdateFunc,
					ec.desc.PParamsUpdateFunc,
					nil,
				))
				after, err := cbor.Encode(pp)
				require.NoError(t, err)
				return !bytes.Equal(before, after)
			}

			var delegateErr lcommon.ProtocolParameterUpdateDelegateError
			unknownKey := lcommon.Blake2b224Hash(
				[]byte("unknown-ppup-genesis-key"),
			)
			require.ErrorAs(
				t,
				process(start, newTx(epoch, true, unknownKey)),
				&delegateErr,
			)
			require.Equal(t, unknownKey, delegateErr.Delegate)
			require.Empty(
				t,
				storedRows(),
				"an unknown genesis key must not reach the vote store",
			)

			fabricated := make([]lcommon.Blake2b224, quorum)
			for i := range fabricated {
				fabricated[i] = lcommon.Blake2b224Hash([]byte{0xfa, byte(i)})
			}
			require.ErrorAs(
				t,
				process(
					start,
					newTx(epoch, true, append(fabricated, genesisKey)...),
				),
				&delegateErr,
			)
			require.Empty(t, storedRows())
			require.False(
				t,
				enactsFromStore(epoch),
				"fabricated keys must not reach quorum",
			)

			var witnessErr lcommon.ProtocolParameterUpdateWitnessError
			require.ErrorAs(
				t,
				process(start, newTx(epoch, false, genesisKey)),
				&witnessErr,
			)
			require.Equal(t, genesisKey, witnessErr.Delegate)
			require.Empty(
				t,
				storedRows(),
				"an unwitnessed proposal must not reach the vote store",
			)

			var epochErr lcommon.ProtocolParameterUpdateEpochError
			require.ErrorAs(
				t,
				process(start, newTx(epoch+1, true, genesisKey)),
				&epochErr,
			)
			require.False(t, epochErr.ForNextEpoch)
			require.ErrorAs(
				t,
				process(noReturn, newTx(epoch, true, genesisKey)),
				&epochErr,
			)
			require.True(t, epochErr.ForNextEpoch)
			require.Empty(
				t,
				storedRows(),
				"a mistimed proposal must not reach the vote store",
			)

			require.NoError(t, process(start, newTx(epoch, true, genesisKey)))
			stored := storedRows()
			require.Len(t, stored, 1)
			require.Equal(t, genesisKey[:], stored[0].GenesisHash)
			require.Equal(t, updateCbor, stored[0].Cbor)
			require.Equal(t, start, stored[0].AddedSlot)

			// Control, last because enactment can update the parameters in
			// place: the same keys stored without validation reach quorum.
			const unvalidated = epoch + 10
			for _, key := range fabricated {
				require.NoError(
					t,
					db.SetPParamUpdate(
						key[:],
						updateCbor,
						start,
						unvalidated,
						nil,
					),
				)
			}
			require.True(t, enactsFromStore(unvalidated))
		})
	}
}

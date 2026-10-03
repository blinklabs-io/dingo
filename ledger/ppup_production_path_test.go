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

type ppupProductionLedgerState struct {
	ppupWindowRuleState
	output lcommon.TransactionOutput
}

func (s ppupProductionLedgerState) UtxoById(
	input lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	return lcommon.Utxo{Id: input, Output: s.output}, nil
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
	genesisKey := lcommon.Blake2b224Hash([]byte("classic-ppup-genesis-key"))
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
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.publishSnapshotsLocked()
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		delegate[:],
		nil,
	)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(address.String()).
		WithLovelace(0).
		Build()
	require.NoError(t, err)
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId([]byte("classic-ppup-input")).
		WithIndex(0).
		Build()
	require.NoError(t, err)
	state := ppupProductionLedgerState{ppupWindowRuleState: ppupWindowRuleState{
		LedgerView: &LedgerView{ls: ls},
		genesisKey: genesisKey,
		delegate:   delegate,
	}, output: output}
	validator := eras.ShelleyEraDesc.ValidateTxFunc
	pparams := &shelley.ShelleyProtocolParameters{
		MaxTxSize: 100_000,
	}
	update := shelley.ShelleyProtocolParameterUpdate{}

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
	var delegateErr lcommon.ProtocolParameterUpdateDelegateError
	require.ErrorAs(
		t,
		validator(unauthorized, 4_492_800, state, pparams),
		&delegateErr,
	)
	assertPParamUpdateCount := func(want int64) {
		raw, err := dbtest.RawSQLiteMetadata(t, db)
		require.NoError(t, err)
		var count int64
		require.NoError(t, raw.QueryRow(`SELECT COUNT(*) FROM pparam_update`).Scan(&count))
		require.Equal(t, want, count)
	}
	assertPParamUpdateCount(0)

	authorized := newTx(genesisKey)
	// Signatures are checked by the era validator; PPUP authorization maps the
	// genesis key to this exact witness's delegate hash.
	state.delegate = delegate
	require.NoError(t, validator(authorized, 4_492_800, state, pparams))
	delta := NewLedgerDelta(
		ocommon.NewPoint(4_492_800, make([]byte, lcommon.Blake2b256Size)),
		uint(shelley.EraIdShelley),
		0,
	)
	delta.addTransaction(authorized, 0)
	var txHash [32]byte
	copy(txHash[:], authorized.Hash().Bytes())
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets:   map[[32]byte]database.CborOffset{txHash: {}},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	for _, produced := range authorized.Produced() {
		var producedTxId [32]byte
		copy(producedTxId[:], produced.Id.Id().Bytes())
		delta.Offsets.UtxoOffsets[database.UtxoRef{
			TxId:      producedTxId,
			OutputIdx: uint32(produced.Id.Index()), //nolint:gosec // fixture output index is zero
		}] = database.CborOffset{BlockSlot: 4_492_800, ByteLength: 1}
	}
	defer delta.Release()
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))
	assertPParamUpdateCount(1)
}

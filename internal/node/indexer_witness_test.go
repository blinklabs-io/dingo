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

package node

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// A block's valid transactions renew the accounts they witness to the epoch of
// their block plus the window, and a transaction that failed phase-2 witnesses
// nothing, as the ledger's renewal does.
func TestIndexedWitnessesRenewAccountExpirations(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const window = uint64(5)
	db := newIndexerTestDB(t)
	bf := NewBackfill(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bf.SetDelegatorInactivityEnabled(true)
	bf.SetDelegatorInactivityWindow(window)
	bf.witnessed = &witnessedAccounts{
		epoch: map[string]uint64{},
		refs:  map[string]models.StakeCredentialRef{},
	}
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(ctx, true)
	defer txn.Release()

	registered := bytes.Repeat([]byte{0x51}, lcommon.AddressHashSize)
	skipped := bytes.Repeat([]byte{0x53}, lcommon.AddressHashSize)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x52}, lcommon.AddressHashSize), registered,
	)
	require.NoError(t, err)
	credential := func(key []byte) lcommon.Credential {
		return lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(key),
		}
	}
	process := func(
		id byte,
		epoch, slot uint64,
		valid bool,
		certs ...lcommon.Certificate,
	) {
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(bytes.Repeat([]byte{id + 1}, 32)).WithIndex(0).Build()
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(address.String()).WithLovelace(1_000_000).Build()
		require.NoError(t, err)
		tx := mockledger.NewTransactionBuilder().WithCertificates(certs...)
		tx.WithId(bytes.Repeat([]byte{id}, 32))
		tx.WithInputs(input)
		tx.WithOutputs(output)
		tx.WithValid(valid)
		built, err := tx.Build()
		require.NoError(t, err)
		var txHash [32]byte
		copy(txHash[:], built.Hash().Bytes())
		offsets := &database.BlockIngestionResult{
			TxOffsets:   map[[32]byte]database.CborOffset{txHash: {BlockSlot: slot, ByteLength: 1}},
			UtxoOffsets: map[database.UtxoRef]database.CborOffset{},
		}
		for _, produced := range built.Produced() {
			var producedID [32]byte
			copy(producedID[:], produced.Id.Id().Bytes())
			offsets.UtxoOffsets[database.UtxoRef{
				TxId: producedID, OutputIdx: produced.Id.Index(),
			}] = database.CborOffset{BlockSlot: slot, ByteLength: 1}
		}
		require.NoError(t, bf.processBlockTxsBatched(
			ctx, []lcommon.Transaction{built},
			ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{byte(slot)}, 32)},
			epoch, eras.ConwayEraDesc.Id, nil, offsets, acc, txn, nil, true,
		))
	}
	registration := func(key []byte) lcommon.Certificate {
		return &lcommon.StakeRegistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: credential(key),
		}
	}
	process(0x10, 3, 100, true, registration(registered), registration(skipped))
	process(0x20, 4, 200, true, &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: credential(registered),
	})
	process(0x30, 8, 300, false, &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: credential(skipped),
	})
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, bf.renewWitnessed(ctx, txn))

	expiration := func(key []byte) uint64 {
		account, err := db.GetAccountByCredential(ctx, 0, key, true, txn)
		require.NoError(t, err)
		return account.ExpirationEpoch
	}
	require.Equal(t, 4+window, expiration(registered))
	require.Equal(t, 3+window, expiration(skipped))
}

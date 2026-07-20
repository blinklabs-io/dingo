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

package database

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func testPoolRegistrationCertificate(seed byte) *lcommon.PoolRegistrationCertificate {
	return &lcommon.PoolRegistrationCertificate{
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
		Operator:      lcommon.PoolKeyHash(lcommon.NewBlake2b224([]byte{seed, 0x01})),
		VrfKeyHash:    lcommon.VrfKeyHash(lcommon.NewBlake2b256([]byte{seed, 0x02})),
		Pledge:        1_000_000,
		Cost:          340_000_000,
		Margin:        cbor.Rat{Rat: big.NewRat(1, 20)},
		RewardAccount: lcommon.AddrKeyHash(lcommon.NewBlake2b224([]byte{seed, 0x03})),
		PoolOwners: []lcommon.AddrKeyHash{
			lcommon.AddrKeyHash(lcommon.NewBlake2b224([]byte{seed, 0x04})),
		},
	}
}

func TestSetTransactionMetadataOnlyRecordsCertificatesWithoutUtxos(t *testing.T) {
	db := openTestDB(t)
	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok)

	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{0x88}, lcommon.Blake2b256Size),
		0,
	)
	require.NoError(t, err)
	cert := testPoolRegistrationCertificate(0x42)
	txBuilder := mockledger.NewTransactionBuilder()
	txBuilder.WithId(bytes.Repeat([]byte{0x42}, lcommon.Blake2b256Size))
	txBuilder.WithType(gledger.TxTypeDijkstra)
	txBuilder.WithValid(true)
	txBuilder.WithInputs(input)
	txBuilder.WithOutputs(output)
	txBuilder.WithCertificates(cert)
	tx, err := txBuilder.Build()
	require.NoError(t, err)

	point := ocommon.Point{
		Slot: 12_345,
		Hash: bytes.Repeat([]byte{0x24}, lcommon.Blake2b256Size),
	}
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		point,
		7,
		map[int]uint64{0: 500_000_000},
		nil,
	))

	gotTx, err := db.Metadata().GetTransactionByHash(tx.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, gotTx)
	require.Equal(t, point.Slot, gotTx.Slot)
	require.Equal(t, uint32(7), gotTx.BlockIndex)

	regs, err := db.Metadata().GetPoolRegistrations(cert.Operator, nil)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, cert.VrfKeyHash, regs[0].VrfKeyHash)

	var certCount int64
	require.NoError(t, store.DB().
		Model(&models.Certificate{}).
		Where("transaction_id = ?", gotTx.ID).
		Count(&certCount).Error)
	require.Equal(t, int64(1), certCount)

	produced := tx.Produced()
	require.NotEmpty(t, produced)
	gotUtxo, err := db.Metadata().GetUtxo(
		tx.Hash().Bytes(),
		produced[0].Id.Index(),
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, gotUtxo)

	var addressTxCount int64
	require.NoError(t, store.DB().
		Model(&models.AddressTransaction{}).
		Where("transaction_id = ?", gotTx.ID).
		Count(&addressTxCount).Error)
	require.Equal(t, int64(0), addressTxCount)
}

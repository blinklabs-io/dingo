// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	pdata "github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

func TestPlutusDataToCardano_IntegerInt64(t *testing.T) {
	pd := pdata.NewInteger(big.NewInt(-42))
	proto := plutusDataToCardano(pd)
	require.NotNil(t, proto)
	intv, ok := proto.GetPlutusData().(*cardano.PlutusData_BigInt)
	require.True(t, ok)
	iv, ok := intv.BigInt.GetBigInt().(*cardano.BigInt_Int)
	require.True(t, ok)
	require.Equal(t, int64(-42), iv.Int)
}

func TestPlutusDataToCardano_IntegerBigUInt(t *testing.T) {
	b := new(big.Int).Lsh(big.NewInt(1), 70) // > int64
	pd := pdata.NewInteger(b)
	proto := plutusDataToCardano(pd)
	require.NotNil(t, proto)
	intv, ok := proto.GetPlutusData().(*cardano.PlutusData_BigInt)
	require.True(t, ok)
	uv, ok := intv.BigInt.GetBigInt().(*cardano.BigInt_BigUInt)
	require.True(t, ok)
	require.Equal(t, b.Bytes(), uv.BigUInt)
}

func TestPlutusDataToCardano_ByteString(t *testing.T) {
	pd := pdata.NewByteString([]byte{0xab, 0xcd})
	proto := plutusDataToCardano(pd)
	require.NotNil(t, proto)
	bs, ok := proto.GetPlutusData().(*cardano.PlutusData_BoundedBytes)
	require.True(t, ok)
	require.Equal(t, []byte{0xab, 0xcd}, bs.BoundedBytes)
}

func TestPlutusDataToCardano_ConstrAndList(t *testing.T) {
	inner := pdata.NewList(pdata.NewInteger(big.NewInt(7)))
	pd := pdata.NewConstr(0, inner)
	proto := plutusDataToCardano(pd)
	require.NotNil(t, proto)
	cv, ok := proto.GetPlutusData().(*cardano.PlutusData_Constr)
	require.True(t, ok)
	require.Equal(t, uint32(0), cv.Constr.Tag)
	require.Len(t, cv.Constr.Fields, 1)
	arr, ok := cv.Constr.Fields[0].GetPlutusData().(*cardano.PlutusData_Array)
	require.True(t, ok)
	require.Len(t, arr.Array.Items, 1)
}

func TestPlutusDataToCardano_Map(t *testing.T) {
	pd := pdata.NewMap(
		[][2]pdata.PlutusData{
			{pdata.NewInteger(big.NewInt(1)), pdata.NewByteString([]byte{9})},
		},
	)
	proto := plutusDataToCardano(pd)
	mv, ok := proto.GetPlutusData().(*cardano.PlutusData_Map)
	require.True(t, ok)
	require.Len(t, mv.Map.Pairs, 1)
}

func TestPlutusDataToCardano_ConstrLargeTagUsesAnyConstructor(t *testing.T) {
	pd := pdata.NewConstr(200, pdata.NewInteger(big.NewInt(0)))
	proto := plutusDataToCardano(pd)
	cv, ok := proto.GetPlutusData().(*cardano.PlutusData_Constr)
	require.True(t, ok)
	require.Equal(t, uint32(0), cv.Constr.Tag)
	require.Equal(t, uint64(200), cv.Constr.AnyConstructor)
}

func TestPlutusDatumCBORToCardano_Integer(t *testing.T) {
	raw, err := pdata.Encode(pdata.NewInteger(big.NewInt(42)))
	require.NoError(t, err)
	proto, err := plutusDatumCBORToCardano(raw)
	require.NoError(t, err)
	require.NotNil(t, proto)
	intv, ok := proto.GetPlutusData().(*cardano.PlutusData_BigInt)
	require.True(t, ok)
	iv, ok := intv.BigInt.GetBigInt().(*cardano.BigInt_Int)
	require.True(t, ok)
	require.Equal(t, int64(42), iv.Int)
}

func TestPlutusDatumCBORToCardano_EmptyRaw(t *testing.T) {
	proto, err := plutusDatumCBORToCardano(nil)
	require.NoError(t, err)
	require.Nil(t, proto)
}

// TestRedeemerPlutusDataByKey_DecodedWitness verifies that redeemer
// Plutus data from a decoded Conway transaction is keyed identically to
// ledger evaluation (tag + index).
func TestRedeemerPlutusDataByKey_DecodedWitness(t *testing.T) {
	txHash := make([]byte, 32)
	txHash[0] = 0x3a

	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{txHash, uint64(0)}},
		},
		2: uint64(200000),
	}

	exData := pdata.NewByteString([]byte{0x01, 0x02, 0x03})
	exCbor, err := pdata.Encode(exData)
	require.NoError(t, err)

	redeemerKeyCbor, err := cbor.Encode([]uint64{0, 0})
	require.NoError(t, err)

	// Embed Plutus CBOR as RawMessage so the array holds the datum as a
	// single CBOR item (not an extra byte-string wrapping Encode output).
	redeemerValCbor, err := cbor.Encode(
		[]any{
			cbor.RawMessage(exCbor),
			[]any{uint64(1_000_000), uint64(50_000_000)},
		},
	)
	require.NoError(t, err)

	redeemerMapCbor := []byte{0xa1}
	redeemerMapCbor = append(redeemerMapCbor, redeemerKeyCbor...)
	redeemerMapCbor = append(redeemerMapCbor, redeemerValCbor...)

	witnessSetCbor := []byte{0xa1, 0x05}
	witnessSetCbor = append(witnessSetCbor, redeemerMapCbor...)

	bodyCbor, err := cbor.Encode(bodyMap)
	require.NoError(t, err)

	txCbor := []byte{0x84}
	txCbor = append(txCbor, bodyCbor...)
	txCbor = append(txCbor, witnessSetCbor...)
	txCbor = append(txCbor, 0xF5)
	txCbor = append(txCbor, 0xF6)

	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)

	m := redeemerPlutusDataByKey(tx)
	key := lcommon.RedeemerKey{
		Tag:   lcommon.RedeemerTagSpend,
		Index: 0,
	}
	got, ok := m[key]
	require.True(t, ok)
	bs, ok := got.(*pdata.ByteString)
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, bs.Inner)

	proto := plutusDataToCardano(got)
	pay, ok := proto.GetPlutusData().(*cardano.PlutusData_BoundedBytes)
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, pay.BoundedBytes)
}

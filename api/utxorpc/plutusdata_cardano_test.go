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
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
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
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
	require.NotNil(t, proto)
	intv, ok := proto.GetPlutusData().(*cardano.PlutusData_BigInt)
	require.True(t, ok)
	uv, ok := intv.BigInt.GetBigInt().(*cardano.BigInt_BigUInt)
	require.True(t, ok)
	require.Equal(t, b.Bytes(), uv.BigUInt)
}

func TestPlutusDataToCardano_ByteString(t *testing.T) {
	pd := pdata.NewByteString([]byte{0xab, 0xcd})
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
	require.NotNil(t, proto)
	bs, ok := proto.GetPlutusData().(*cardano.PlutusData_BoundedBytes)
	require.True(t, ok)
	require.Equal(t, []byte{0xab, 0xcd}, bs.BoundedBytes)
}

func TestPlutusDataToCardano_ConstrAndList(t *testing.T) {
	inner := pdata.NewList(pdata.NewInteger(big.NewInt(7)))
	pd := pdata.NewConstr(0, inner)
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
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
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
	mv, ok := proto.GetPlutusData().(*cardano.PlutusData_Map)
	require.True(t, ok)
	require.Len(t, mv.Map.Pairs, 1)
}

func TestPlutusDataToCardano_ConstrLargeTagUsesAnyConstructor(t *testing.T) {
	pd := pdata.NewConstrFromBigInt(big.NewInt(200), pdata.NewInteger(big.NewInt(0)))
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
	cv, ok := proto.GetPlutusData().(*cardano.PlutusData_Constr)
	require.True(t, ok)
	require.Equal(t, uint32(0), cv.Constr.Tag)
	require.Equal(t, uint64(200), cv.Constr.AnyConstructor)
}

func TestPlutusDataToCardanoChecked_ConstrTag127UsesTag(t *testing.T) {
	pd := pdata.NewConstrFromBigInt(big.NewInt(127))
	proto, err := plutusDataToCardanoChecked(pd)
	require.NoError(t, err)
	require.NotNil(t, proto)
	cv, ok := proto.GetPlutusData().(*cardano.PlutusData_Constr)
	require.True(t, ok)
	require.Equal(t, uint32(127), cv.Constr.Tag)
	require.Equal(t, uint64(0), cv.Constr.AnyConstructor)
}

func TestPlutusDataToCardanoChecked_ConstrTag128UsesAnyConstructor(t *testing.T) {
	pd := pdata.NewConstrFromBigInt(big.NewInt(128))
	proto, err := plutusDataToCardanoChecked(pd)
	require.NoError(t, err)
	require.NotNil(t, proto)
	cv, ok := proto.GetPlutusData().(*cardano.PlutusData_Constr)
	require.True(t, ok)
	require.Equal(t, uint32(0), cv.Constr.Tag)
	require.Equal(t, uint64(128), cv.Constr.AnyConstructor)
}

func TestPlutusDataToCardano_ConstrMaxTagUsesAnyConstructor(t *testing.T) {
	pd := pdata.NewConstrFromBigInt(new(big.Int).SetUint64(^uint64(0)))
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
	require.NotNil(t, proto)
	cv, ok := proto.GetPlutusData().(*cardano.PlutusData_Constr)
	require.True(t, ok)
	require.Equal(t, uint32(0), cv.Constr.Tag)
	require.Equal(t, ^uint64(0), cv.Constr.AnyConstructor)
}

func TestPlutusDataToCardano_ConstrAboveMaxTagRejected(t *testing.T) {
	tag := new(big.Int).Lsh(big.NewInt(1), 64)
	pd := pdata.NewConstrFromBigInt(tag)
	proto, err := plutusDataToCardano(pd)
	require.Nil(t, proto)
	require.Error(t, err)
	_, err = plutusDataToCardanoChecked(pd)
	require.EqualError(
		t,
		err,
		"constructor tag 18446744073709551616 is outside the Word64 CBOR range",
	)
}

func TestPlutusDataToCardano_ConstrNilTagUsesZeroTag(t *testing.T) {
	pd := &pdata.Constr{}
	proto, err := plutusDataToCardano(pd)
	require.NoError(t, err)
	require.NotNil(t, proto)
	cv, ok := proto.GetPlutusData().(*cardano.PlutusData_Constr)
	require.True(t, ok)
	require.Equal(t, uint32(0), cv.Constr.Tag)
	require.Equal(t, uint64(0), cv.Constr.AnyConstructor)
}

func TestPlutusDataToCardano_ConstrNegativeTagRejected(t *testing.T) {
	pd := pdata.NewConstrFromBigInt(big.NewInt(-1))
	proto, err := plutusDataToCardano(pd)
	require.Nil(t, proto)
	require.Error(t, err)
	_, err = plutusDataToCardanoChecked(pd)
	require.EqualError(
		t,
		err,
		"constructor tag -1 is outside the Word64 CBOR range",
	)
}

func TestPlutusDataToCardanoChecked_PropagatesNestedTagErrors(t *testing.T) {
	bad := pdata.NewConstrFromBigInt(new(big.Int).Lsh(big.NewInt(1), 64))
	cases := map[string]pdata.PlutusData{
		"constructor field": pdata.NewConstrFromBigInt(big.NewInt(0), bad),
		"map key": pdata.NewMap([][2]pdata.PlutusData{{
			bad,
			pdata.NewInteger(big.NewInt(0)),
		}}),
		"map value": pdata.NewMap([][2]pdata.PlutusData{{
			pdata.NewInteger(big.NewInt(0)),
			bad,
		}}),
		"list item": pdata.NewList(bad),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			proto, err := plutusDataToCardano(input)
			require.Nil(t, proto)
			require.Error(t, err)
			_, err = plutusDataToCardanoChecked(input)
			require.EqualError(
				t,
				err,
				"constructor tag 18446744073709551616 is outside the Word64 CBOR range",
			)
		})
	}
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

	proto, err := plutusDataToCardano(got)
	require.NoError(t, err)
	pay, ok := proto.GetPlutusData().(*cardano.PlutusData_BoundedBytes)
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, pay.BoundedBytes)
}

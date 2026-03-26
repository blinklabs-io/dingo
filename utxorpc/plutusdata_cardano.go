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

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pdata "github.com/blinklabs-io/plutigo/data"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// redeemerPlutusDataByKey returns Plutus redeemer payloads from the
// transaction witness set, keyed by redeemer tag and index.
func redeemerPlutusDataByKey(
	tx gledger.Transaction,
) map[lcommon.RedeemerKey]pdata.PlutusData {
	out := make(map[lcommon.RedeemerKey]pdata.PlutusData)
	ws := tx.Witnesses()
	if ws == nil {
		return out
	}
	r := ws.Redeemers()
	if r == nil {
		return out
	}
	for k, v := range r.Iter() {
		if v.Data.Data != nil {
			out[k] = v.Data.Data
		}
	}
	return out
}

// plutusDataToCardano maps plutigo Plutus data to utxorpc.cardano PlutusData.
func plutusDataToCardano(d pdata.PlutusData) *cardano.PlutusData {
	if d == nil {
		return nil
	}
	switch v := d.(type) {
	case *pdata.Constr:
		if v == nil {
			return nil
		}
		fields := make([]*cardano.PlutusData, len(v.Fields))
		for i, f := range v.Fields {
			fields[i] = plutusDataToCardano(f)
		}
		tag := uint32(v.Tag)
		var anyAlt uint64
		if v.Tag > 127 {
			tag = 0
			anyAlt = uint64(v.Tag)
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_Constr{
				Constr: &cardano.Constr{
					Tag:            tag,
					AnyConstructor: anyAlt,
					Fields:         fields,
				},
			},
		}
	case *pdata.Map:
		if v == nil {
			return nil
		}
		pairs := make([]*cardano.PlutusDataPair, 0, len(v.Pairs))
		for _, p := range v.Pairs {
			pairs = append(
				pairs,
				&cardano.PlutusDataPair{
					Key:   plutusDataToCardano(p[0]),
					Value: plutusDataToCardano(p[1]),
				},
			)
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_Map{
				Map: &cardano.PlutusDataMap{Pairs: pairs},
			},
		}
	case *pdata.Integer:
		if v == nil {
			return nil
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_BigInt{
				BigInt: bigIntToCardano(v.Inner),
			},
		}
	case *pdata.ByteString:
		if v == nil {
			return nil
		}
		b := append([]byte(nil), v.Inner...)
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_BoundedBytes{
				BoundedBytes: b,
			},
		}
	case *pdata.List:
		if v == nil {
			return nil
		}
		items := make([]*cardano.PlutusData, len(v.Items))
		for i, it := range v.Items {
			items[i] = plutusDataToCardano(it)
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_Array{
				Array: &cardano.PlutusDataArray{Items: items},
			},
		}
	default:
		return nil
	}
}

func bigIntToCardano(i *big.Int) *cardano.BigInt {
	if i == nil {
		return nil
	}
	if i.IsInt64() {
		return &cardano.BigInt{
			BigInt: &cardano.BigInt_Int{Int: i.Int64()},
		}
	}
	if i.Sign() >= 0 {
		b := i.Bytes()
		if len(b) == 0 {
			b = []byte{0}
		}
		return &cardano.BigInt{
			BigInt: &cardano.BigInt_BigUInt{BigUInt: b},
		}
	}
	abs := new(big.Int).Neg(i)
	b := abs.Bytes()
	if len(b) == 0 {
		b = []byte{0}
	}
	return &cardano.BigInt{
		BigInt: &cardano.BigInt_BigNInt{BigNInt: b},
	}
}

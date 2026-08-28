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
	"errors"
	"fmt"
	"math/big"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pdata "github.com/blinklabs-io/plutigo/data"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// plutusDatumCBORToCardano decodes Cardano datum CBOR and maps it to
// utxorpc.cardano.PlutusData for AnyChainDatum parsed_state (cardano).
func plutusDatumCBORToCardano(raw []byte) (*cardano.PlutusData, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	pd, err := pdata.Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("decode plutus data: %w", err)
	}
	proto, err := plutusDataToCardanoChecked(pd)
	if err != nil {
		return nil, fmt.Errorf("convert plutus data: %w", err)
	}
	if proto == nil {
		return nil, errors.New(
			"unsupported PlutusData type for utxorpc mapping",
		)
	}
	return proto, nil
}

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
	proto, err := plutusDataToCardanoChecked(d)
	if err != nil {
		return nil
	}
	return proto
}

func plutusDataToCardanoChecked(
	d pdata.PlutusData,
) (*cardano.PlutusData, error) {
	if d == nil {
		return nil, nil
	}
	switch v := d.(type) {
	case *pdata.Constr:
		if v == nil {
			return nil, nil
		}
		if v.Tag != nil && !v.Tag.IsUint64() {
			return nil, fmt.Errorf(
				"constructor tag %s is outside the Word64 CBOR range",
				v.Tag,
			)
		}
		fields := make([]*cardano.PlutusData, len(v.Fields))
		for i, f := range v.Fields {
			var err error
			fields[i], err = plutusDataToCardanoChecked(f)
			if err != nil {
				return nil, err
			}
		}
		var tag uint32
		var anyAlt uint64
		if v.Tag != nil {
			constructorTag := v.Tag.Uint64()
			if constructorTag > 127 {
				tag = 0
				anyAlt = constructorTag
			} else {
				tag = uint32(constructorTag)
			}
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_Constr{
				Constr: &cardano.Constr{
					Tag:            tag,
					AnyConstructor: anyAlt,
					Fields:         fields,
				},
			},
		}, nil
	case *pdata.Map:
		if v == nil {
			return nil, nil
		}
		pairs := make([]*cardano.PlutusDataPair, 0, len(v.Pairs))
		for _, p := range v.Pairs {
			key, err := plutusDataToCardanoChecked(p[0])
			if err != nil {
				return nil, err
			}
			value, err := plutusDataToCardanoChecked(p[1])
			if err != nil {
				return nil, err
			}
			pairs = append(
				pairs,
				&cardano.PlutusDataPair{
					Key:   key,
					Value: value,
				},
			)
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_Map{
				Map: &cardano.PlutusDataMap{Pairs: pairs},
			},
		}, nil
	case *pdata.Integer:
		if v == nil {
			return nil, nil
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_BigInt{
				BigInt: bigIntToCardano(v.Inner),
			},
		}, nil
	case *pdata.ByteString:
		if v == nil {
			return nil, nil
		}
		b := append([]byte(nil), v.Inner...)
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_BoundedBytes{
				BoundedBytes: b,
			},
		}, nil
	case *pdata.List:
		if v == nil {
			return nil, nil
		}
		items := make([]*cardano.PlutusData, len(v.Items))
		for i, it := range v.Items {
			var err error
			items[i], err = plutusDataToCardanoChecked(it)
			if err != nil {
				return nil, err
			}
		}
		return &cardano.PlutusData{
			PlutusData: &cardano.PlutusData_Array{
				Array: &cardano.PlutusDataArray{Items: items},
			},
		}, nil
	default:
		return nil, nil
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

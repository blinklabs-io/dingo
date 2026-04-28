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
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
)

const (
	txBodyKeyOutputs uint64 = 1
	// TxBodyKeyCollateralReturn is the CBOR map key for the
	// collateral_return entry in a transaction body. Exported so the
	// node loader can reuse it without redeclaring the constant.
	TxBodyKeyCollateralReturn uint64 = 16
)

func checkedIntToUint32(v int) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("negative value %d", v)
	}
	if uint64(v) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("value %d overflows uint32", v)
	}
	return uint32(v), nil // #nosec G115 -- checked above
}

func absoluteBodyRange(
	bodyOffset uint32,
	valueOffset int,
	valueLen int,
) (uint32, uint32, error) {
	valueOffset32, err := checkedIntToUint32(valueOffset)
	if err != nil {
		return 0, 0, err
	}
	valueLen32, err := checkedIntToUint32(valueLen)
	if err != nil {
		return 0, 0, err
	}
	if valueOffset32 > ^uint32(0)-bodyOffset {
		return 0, 0, fmt.Errorf(
			"value offset %d overflows body offset %d",
			valueOffset32,
			bodyOffset,
		)
	}
	return bodyOffset + valueOffset32, valueLen32, nil
}

func txBodyProducedOutputRange(
	bodyCbor []byte,
	bodyOffset uint32,
	outputIndex uint32,
) (uint32, uint32, bool, error) {
	decoder, err := cbor.NewStreamDecoder(bodyCbor)
	if err != nil {
		return 0, 0, false, err
	}
	count, _, _, err := decoder.DecodeMapHeader()
	if err != nil {
		return 0, 0, false, err
	}
	if count < 0 {
		return 0, 0, false, errors.New("indefinite tx body map")
	}

	var outputCount uint32
	var haveOutputs bool
	var collateralOffset uint32
	var collateralLen uint32
	var haveCollateral bool

	for range count {
		var currentKey uint64
		if _, _, err := decoder.Decode(&currentKey); err != nil {
			return 0, 0, false, err
		}

		switch currentKey {
		case txBodyKeyOutputs:
			arrCount, _, _, err := decoder.DecodeArrayHeader()
			if err != nil {
				return 0, 0, false, err
			}
			if arrCount < 0 {
				return 0, 0, false, errors.New("indefinite output array")
			}
			outputCount64 := uint64(arrCount)
			if outputCount64 > uint64(^uint32(0)) {
				return 0, 0, false, fmt.Errorf(
					"output count %d overflows uint32",
					outputCount64,
				)
			}
			outputCount = uint32(outputCount64) // #nosec G115 -- checked above
			haveOutputs = true
			for i := range arrCount {
				valueOffset, valueLen, err := decoder.Skip()
				if err != nil {
					return 0, 0, false, err
				}
				if uint32(i) != outputIndex { // #nosec G115 -- i < outputCount
					continue
				}
				offset, length, err := absoluteBodyRange(
					bodyOffset,
					valueOffset,
					valueLen,
				)
				if err != nil {
					return 0, 0, false, err
				}
				return offset, length, true, nil
			}
		case TxBodyKeyCollateralReturn:
			valueOffset, valueLen, err := decoder.Skip()
			if err != nil {
				return 0, 0, false, err
			}
			offset, length, err := absoluteBodyRange(
				bodyOffset,
				valueOffset,
				valueLen,
			)
			if err != nil {
				return 0, 0, false, err
			}
			collateralOffset = offset
			collateralLen = length
			haveCollateral = true
		default:
			if _, _, err := decoder.Skip(); err != nil {
				return 0, 0, false, err
			}
		}
	}

	// collateral_return is the pseudo-output at index len(outputs),
	// matching lcommon.Transaction.Produced() for invalid Babbage+ txs.
	if haveOutputs &&
		haveCollateral &&
		outputIndex == outputCount {
		return collateralOffset, collateralLen, true, nil
	}
	return 0, 0, false, nil
}

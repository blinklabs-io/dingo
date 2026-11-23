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

package ledgerstate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	sclscardano "github.com/blinklabs-io/go-scls/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// CBOR major types, shifted into the initial byte.
const (
	cborUint  = 0x00
	cborBytes = 0x40
	cborArray = 0x80
	cborMap   = 0xa0
	cborTag   = 0xc0
)

// ledgerTxOut re-encodes a decoded tx_out the way cardano-ledger serializes
// a Conway UTxO entry (EncCBOR of BabbageTxOut in Babbage/TxOut.hs), which is
// what the reference utxo/v0 export emits. The set stores a transaction's own
// bytes or a MemPack import's map, and either can differ from that form:
//
//   - With no script reference and no inline datum the ledger emits the
//     2-element array, or the 3-element array for a datum hash, even when the
//     output was written as a map. Otherwise it emits a definite map with keys
//     in ascending order.
//   - A value with no assets is a bare coin.
//   - Multiasset maps are ordered by the ledger's key ordering, bytewise
//     lexicographic, and a map of more than 23 entries is indefinite-length
//     (variableMapLenEncoding in Encoder.hs).
//   - Inline datums and native scripts keep their stored bytes, which the
//     ledger memoizes; Plutus scripts are re-encoded as [language, bytes].
//   - Addresses go through ledgerAddress.
func ledgerTxOut(out sclscardano.TxOut) ([]byte, error) {
	address, err := ledgerAddress(out.Address)
	if err != nil {
		return nil, err
	}
	addr := appendBytes(nil, address)
	value := appendLedgerValue(nil, out.Value)
	inlineOrScript := out.Script != nil ||
		(out.Datum != nil && out.Datum.Inline != nil)
	if !inlineOrScript {
		n := uint64(2)
		if out.Datum != nil {
			n = 3
		}
		res := appendHead(nil, cborArray, n)
		res = append(res, addr...)
		res = append(res, value...)
		if out.Datum != nil {
			res = appendBytes(res, out.Datum.Hash[:])
		}
		return res, nil
	}
	n := uint64(2)
	if out.Datum != nil {
		n++
	}
	if out.Script != nil {
		n++
	}
	res := appendHead(nil, cborMap, n)
	res = appendHead(res, cborUint, 0)
	res = append(res, addr...)
	res = appendHead(res, cborUint, 1)
	res = append(res, value...)
	if d := out.Datum; d != nil {
		res = appendHead(res, cborUint, 2)
		res = appendHead(res, cborArray, 2)
		if d.Hash != nil {
			res = appendHead(res, cborUint, 0)
			res = appendBytes(res, d.Hash[:])
		} else {
			res = appendHead(res, cborUint, 1)
			res = appendHead(res, cborTag, 24)
			res = appendBytes(res, d.Inline)
		}
	}
	if s := out.Script; s != nil {
		var script []byte
		if s.Language == 0 {
			script = append([]byte{cborArray | 2, cborUint}, s.Script...)
		} else {
			script = appendHead(nil, cborArray, 2)
			script = appendHead(script, cborUint, uint64(s.Language))
			script = appendBytes(script, s.Script)
		}
		res = appendHead(res, cborUint, 3)
		res = appendHead(res, cborTag, 24)
		res = appendBytes(res, script)
	}
	return res, nil
}

// shelleyFormTxOut returns a stored Byron output in the Shelley form every
// later era holds it in. The set keeps a Byron output in its Byron encoding,
// [address, coin], where the address is the CBOR structure itself; the ledger
// translates it to a Shelley output whose address is a byte string holding
// that structure (translateTxOutByronToShelley in ByronTranslation.hs). Any
// other output is returned unchanged.
func shelleyFormTxOut(stored []byte) ([]byte, error) {
	if len(stored) == 0 || stored[0]&0xe0 != cborArray {
		return stored, nil
	}
	var items []cbor.RawMessage
	if _, err := cbor.Decode(stored, &items); err != nil {
		return nil, fmt.Errorf("tx_out: %w", err)
	}
	if len(items) == 0 || len(items[0]) == 0 ||
		items[0][0]&0xe0 != cborArray {
		return stored, nil
	}
	if _, err := byron.NewByronTransactionOutputFromCbor(stored); err != nil {
		return nil, err
	}
	res := appendHead(nil, cborArray, 2)
	res = appendBytes(res, items[0])
	return append(res, items[1]...), nil
}

// ledgerAddress returns the address bytes the ledger holds for an output.
// Decoding a pre-Babbage output, fromCborBothAddr (Address.hs) keeps only the
// consumed prefix of the address and rebuilds a pointer through
// mkPtrNormalized, which zeroes every component when one is out of range; the
// stored bytes are the transaction's and keep both. The lenient gouroboros
// decoder reproduces that crop and normalization. A Byron address is kept
// as stored: the ledger holds its original serialization, which re-encoding
// the decoded attributes need not reproduce.
func ledgerAddress(stored []byte) ([]byte, error) {
	if len(stored) == 0 || stored[0]>>4 == lcommon.AddressTypeByron {
		return stored, nil
	}
	addr, err := lcommon.NewAddressFromBytesLenient(stored)
	if err != nil {
		return nil, fmt.Errorf("address: %w", err)
	}
	return addr.Bytes()
}

func appendLedgerValue(b []byte, v sclscardano.TxValue) []byte {
	if v.Assets == nil || len(*v.Assets) == 0 {
		return appendHead(b, cborUint, v.Coin)
	}
	policies := slices.Clone(*v.Assets)
	slices.SortFunc(policies, func(x, y sclscardano.PolicyAssets) int {
		return bytes.Compare(x.Policy[:], y.Policy[:])
	})
	b = appendHead(b, cborArray, 2)
	b = appendHead(b, cborUint, v.Coin)
	b = appendLedgerMapHead(b, len(policies))
	for _, p := range policies {
		b = appendBytes(b, p.Policy[:])
		assets := slices.Clone(p.Assets)
		slices.SortFunc(assets, func(x, y sclscardano.Asset) int {
			return bytes.Compare(x.Name, y.Name)
		})
		b = appendLedgerMapHead(b, len(assets))
		for _, a := range assets {
			b = appendBytes(b, a.Name)
			b = appendHead(b, cborUint, a.Quantity)
		}
		b = appendLedgerMapEnd(b, len(assets))
	}
	return appendLedgerMapEnd(b, len(policies))
}

// ledgerMapThreshold is the largest map the ledger encodes with a definite
// length.
const ledgerMapThreshold = 23

func appendLedgerMapHead(b []byte, n int) []byte {
	if n > ledgerMapThreshold {
		return append(b, cborMap|0x1f)
	}
	return appendHead(b, cborMap, uint64(n)) // #nosec G115 -- 0 <= n <= 23
}

func appendLedgerMapEnd(b []byte, n int) []byte {
	if n > ledgerMapThreshold {
		return append(b, 0xff)
	}
	return b
}

func appendBytes(b, data []byte) []byte {
	b = appendHead(b, cborBytes, uint64(len(data)))
	return append(b, data...)
}

// appendHead appends a CBOR head with the shortest argument encoding.
func appendHead(b []byte, major byte, n uint64) []byte {
	switch {
	case n < 24:
		return append(b, major|byte(n))
	case n <= 0xff:
		return append(b, major|24, byte(n))
	case n <= 0xffff:
		return binary.BigEndian.AppendUint16(append(b, major|25), uint16(n))
	case n <= 0xffffffff:
		return binary.BigEndian.AppendUint32(append(b, major|26), uint32(n))
	default:
		return binary.BigEndian.AppendUint64(append(b, major|27), n)
	}
}

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
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// zeroBlake2b224 is the zero-value Blake2b224 used to check whether
// PaymentKeyHash/StakeKeyHash returned a meaningful credential.
var zeroBlake2b224 ledger.Blake2b224

// decodeTxIn extracts the transaction hash and output index from a
// TxIn encoded in various formats:
//   - CBOR array: [hash, index]
//   - CBOR tag 24 (WrappedCbor): bstr wrapping CBOR or binary TxIn
//   - Plain byte string: CBOR or binary TxIn inside
//   - Binary: 32-byte hash + 2-byte little-endian index
func decodeTxIn(
	raw cbor.RawMessage,
) (txHash []byte, outputIndex uint32, err error) {
	// Try 1: Plain byte string (MemPack TxIn in UTxO-HD tvar)
	// This is the most common case for MemPack files where each
	// key is a CBOR byte string wrapping 34 bytes of MemPack
	// TxIn (32-byte hash + 2-byte LE index).
	// decodeTxInFromBytes handles both the exact 34-byte binary
	// path and CBOR-array-inside-byte-string fallback.
	var keyBytes []byte
	if _, decErr := cbor.Decode(raw, &keyBytes); decErr == nil {
		return decodeTxInFromBytes(keyBytes)
	}

	// Try 2: Direct CBOR array [hash, index]
	var txIn []cbor.RawMessage
	if _, decErr := cbor.Decode(raw, &txIn); decErr == nil &&
		len(txIn) >= 2 {
		return decodeTxInFromArray(txIn)
	}

	// Try 3: CBOR tag 24 (WrappedCbor) wrapping bytes
	var wrapped cbor.WrappedCbor
	if _, decErr := cbor.Decode(raw, &wrapped); decErr == nil {
		return decodeTxInFromBytes(wrapped.Bytes())
	}

	// Nothing worked — return diagnostic error
	prefix := []byte(raw)
	if len(prefix) > 40 {
		prefix = prefix[:40]
	}
	return nil, 0, fmt.Errorf(
		"unrecognized TxIn encoding (len=%d, hex=%x)",
		len(raw), prefix,
	)
}

// decodeTxInFromArray decodes TxIn from a CBOR array of
// [hash, index].
func decodeTxInFromArray(
	txIn []cbor.RawMessage,
) ([]byte, uint32, error) {
	if len(txIn) < 2 {
		return nil, 0, fmt.Errorf(
			"TxIn array has %d elements, expected 2",
			len(txIn),
		)
	}
	var txHash []byte
	if _, err := cbor.Decode(txIn[0], &txHash); err != nil {
		return nil, 0, fmt.Errorf(
			"decoding TxIn hash: %w", err,
		)
	}
	var outputIndex uint32
	if _, err := cbor.Decode(txIn[1], &outputIndex); err != nil {
		return nil, 0, fmt.Errorf(
			"decoding TxIn index: %w", err,
		)
	}
	return txHash, outputIndex, nil
}

// decodeTxInFromBytes decodes TxIn from inner bytes that may be
// raw binary (32-byte hash + 2-byte LE index) or a CBOR array.
// Binary format is tried first since it's the standard MemPack
// encoding used in UTxO-HD tvar files.
func decodeTxInFromBytes(
	data []byte,
) ([]byte, uint32, error) {
	// Binary format: 32-byte hash + 2-byte little-endian index
	// This is the MemPack TxIn encoding (PackedBytes32 + Word16)
	if len(data) == 34 {
		txHash := make([]byte, 32)
		copy(txHash, data[:32])
		idx := uint32(data[32]) | uint32(data[33])<<8
		return txHash, idx, nil
	}

	// Try CBOR array inside the byte string
	var txIn []cbor.RawMessage
	if _, err := cbor.Decode(data, &txIn); err == nil &&
		len(txIn) >= 2 {
		return decodeTxInFromArray(txIn)
	}

	prefix := data
	if len(prefix) > 40 {
		prefix = prefix[:40]
	}
	return nil, 0, fmt.Errorf(
		"TxIn bytes unrecognized (len=%d, hex=%x)",
		len(data), prefix,
	)
}

// unwrapCborBytes unwraps a CBOR value that may be a tag 24
// (WrappedCbor) or a plain byte string. Returns the inner bytes
// on success, or the original data with an error if unwrapping
// is not applicable.
func unwrapCborBytes(
	raw cbor.RawMessage,
) (cbor.RawMessage, error) {
	// Try CBOR tag 24 (WrappedCbor)
	var wrapped cbor.WrappedCbor
	if _, err := cbor.Decode(raw, &wrapped); err == nil {
		return cbor.RawMessage(wrapped.Bytes()), nil
	}

	// Try plain byte string
	var bstr []byte
	if _, err := cbor.Decode(raw, &bstr); err == nil {
		return cbor.RawMessage(bstr), nil
	}

	return raw, errors.New("not a wrapped byte string")
}

// utxoBatchSize controls how many UTxOs are collected per batch
// during streaming decode.
const utxoBatchSize = 10000

// UTxOCallback is called for each batch of parsed UTxOs during
// streaming decode.
type UTxOCallback func(batch []ParsedUTxO) error

// utxoIterFunc returns the next key/value CBOR pair from a UTxO
// map. It returns done=true when no more entries remain. Callers
// must not call the function again after done=true is returned.
type utxoIterFunc func() (
	keyRaw cbor.RawMessage,
	valRaw cbor.RawMessage,
	done bool,
	err error,
)

// processBatchedUTxOs reads key/value pairs from next, parses each
// entry, and delivers them to the callback in batches of
// utxoBatchSize. This is the shared core for both definite-length
// and indefinite-length UTxO map parsing.
func processBatchedUTxOs(
	next utxoIterFunc,
	callback UTxOCallback,
) (int, error) {
	if callback == nil {
		return 0, errors.New("nil UTxO callback")
	}
	batch := make([]ParsedUTxO, 0, utxoBatchSize)
	totalCount := 0

	for {
		keyRaw, valRaw, done, err := next()
		if err != nil {
			return totalCount, err
		}
		if done {
			break
		}

		parsed, err := parseUTxOEntry(keyRaw, valRaw)
		if err != nil {
			return totalCount, fmt.Errorf(
				"parsing UTxO entry %d: %w",
				totalCount,
				err,
			)
		}

		batch = append(batch, *parsed)
		totalCount++

		if len(batch) >= utxoBatchSize {
			if err := callback(batch); err != nil {
				return totalCount, fmt.Errorf(
					"UTxO callback error at entry %d: %w",
					totalCount,
					err,
				)
			}
			batch = make([]ParsedUTxO, 0, utxoBatchSize)
		}
	}

	// Flush remaining batch
	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return totalCount, fmt.Errorf(
				"UTxO callback error at final batch: %w",
				err,
			)
		}
	}

	return totalCount, nil
}

// ParseUTxOsStreaming decodes the UTxO map from raw CBOR data and
// calls the callback for each batch of parsed UTxOs. This avoids
// loading the entire UTxO set into memory. Both definite-length and
// indefinite-length (0xbf) CBOR maps are supported.
func ParseUTxOsStreaming(
	data cbor.RawMessage,
	callback UTxOCallback,
) (int, error) {
	// Handle indefinite-length map (0xbf header) by delegating to
	// the dedicated parser. DecodeMapHeader does not support
	// indefinite-length maps and would return a confusing error.
	if len(data) > 0 && data[0] == 0xbf {
		return parseIndefiniteUTxOMap(data, callback)
	}

	// The UTxO data is a CBOR map: map[TxIn]TxOut
	// Use StreamDecoder to iterate without needing comparable map
	// keys (cbor.RawMessage is []byte, not comparable).
	decoder, err := cbor.NewStreamDecoder(data)
	if err != nil {
		return 0, fmt.Errorf(
			"creating stream decoder for UTxO map: %w",
			err,
		)
	}

	count, _, _, err := decoder.DecodeMapHeader()
	if err != nil {
		return 0, fmt.Errorf(
			"decoding UTxO map header: %w",
			err,
		)
	}

	i := 0
	next := func() (cbor.RawMessage, cbor.RawMessage, bool, error) {
		if i >= count {
			return nil, nil, true, nil
		}
		idx := i
		i++

		var keyDummy any
		_, keyRaw, err := decoder.DecodeRaw(&keyDummy)
		if err != nil {
			return nil, nil, false, fmt.Errorf(
				"decoding UTxO key %d: %w", idx, err,
			)
		}

		var valDummy any
		_, valRaw, err := decoder.DecodeRaw(&valDummy)
		if err != nil {
			return nil, nil, false, fmt.Errorf(
				"decoding UTxO value %d: %w", idx, err,
			)
		}

		return keyRaw, valRaw, false, nil
	}

	return processBatchedUTxOs(next, callback)
}

// parseUTxOEntry decodes a single TxIn -> TxOut entry from the
// UTxO map. Supports multiple encodings:
//   - Legacy CBOR: TxOut is a CBOR array or map
//   - UTxO-HD MemPack: TxOut is MemPack binary (tags 0-5)
//   - TxIn: CBOR array, tag-24-wrapped, or plain byte string
//     with 32-byte hash + 2-byte LE index
func parseUTxOEntry(
	keyRaw cbor.RawMessage,
	valRaw cbor.RawMessage,
) (*ParsedUTxO, error) {
	txHash, outputIndex, err := decodeTxIn(keyRaw)
	if err != nil {
		return nil, fmt.Errorf("decoding TxIn: %w", err)
	}

	// Unwrap the CBOR byte string wrapper
	txOutData, err := unwrapCborBytes(valRaw)
	if err != nil {
		txOutData = valRaw
	}

	// Check if this is MemPack format (UTxO-HD tvar files)
	if isMempackFormat(txOutData) {
		return parseMempackTxOut(txHash, outputIndex, txOutData)
	}

	// Standard CBOR TxOut (on-chain format)
	return parseCborTxOut(txHash, outputIndex, valRaw, txOutData)
}

// parseMempackTxOut decodes a MemPack-encoded TxOut and returns a
// ParsedUTxO with address keys extracted.
func parseMempackTxOut(
	txHash []byte,
	outputIndex uint32,
	data []byte,
) (*ParsedUTxO, error) {
	decoded, err := decodeMempackTxOut(data)
	if err != nil {
		return nil, fmt.Errorf("decoding MemPack TxOut: %w", err)
	}

	result := &ParsedUTxO{
		TxHash:      txHash,
		OutputIndex: outputIndex,
		Address:     decoded.Address,
		Amount:      decoded.Lovelace,
		Assets:      decoded.Assets,
		DatumHash:   decoded.DatumHash,
		Datum:       decoded.Datum,
		ScriptRef:   decoded.ScriptRef,
	}

	// Extract payment and staking keys from the raw address
	extractAddressKeys(decoded.Address, result)

	return result, nil
}

// extractAddressKeys extracts payment and staking key hashes from
// raw Shelley-format address bytes.
//
// Only base (types 0-3), pointer (types 4-5), and enterprise (types
// 6-7) addresses are handled. The following types are intentionally
// skipped:
//   - Type 8 (Byron/bootstrap): Addresses use a nested CBOR structure
//     rather than raw 28-byte key hashes, so there is nothing to
//     extract.
//   - Types 14-15 (reward/stake): These are used for reward
//     withdrawals and do not appear in transaction outputs (UTxOs).
func extractAddressKeys(addr []byte, result *ParsedUTxO) {
	if len(addr) < 1 {
		return
	}

	headerByte := addr[0]
	addrType := (headerByte >> 4) & 0x0f

	// Even address types (0,2,4,6) have payment KEY credentials;
	// odd types (1,3,5,7) have payment SCRIPT credentials.
	// For staking: types 0,1 have staking KEY; types 2,3 have
	// staking SCRIPT. Only store key hashes, not script hashes,
	// to match the CBOR parsing path (parseCborTxOut).
	paymentIsKey := addrType%2 == 0

	switch {
	case addrType <= 3 && len(addr) >= 57:
		// Base address: 1 header + 28 payment + 28 staking
		if paymentIsKey {
			result.PaymentKey = bytes.Clone(addr[1:29])
		}
		if addrType <= 1 { // staking is key for types 0,1
			result.StakingKey = bytes.Clone(addr[29:57])
		}
	case (addrType == 4 || addrType == 5) && len(addr) >= 29:
		// Pointer address: 1 header + 28 payment + pointer
		if paymentIsKey {
			result.PaymentKey = bytes.Clone(addr[1:29])
		}
	case (addrType == 6 || addrType == 7) && len(addr) >= 29:
		// Enterprise address: 1 header + 28 payment
		if paymentIsKey {
			result.PaymentKey = bytes.Clone(addr[1:29])
		}
	}
}

// parseCborTxOut decodes a standard CBOR-encoded TxOut.
func parseCborTxOut(
	txHash []byte,
	outputIndex uint32,
	valRaw cbor.RawMessage,
	txOutData cbor.RawMessage,
) (*ParsedUTxO, error) {
	txOut, err := ledger.NewTransactionOutputFromCbor(txOutData)
	if err != nil {
		rawPrefix := []byte(valRaw)
		if len(rawPrefix) > 60 {
			rawPrefix = rawPrefix[:60]
		}
		dataPrefix := []byte(txOutData)
		if len(dataPrefix) > 60 {
			dataPrefix = dataPrefix[:60]
		}
		return nil, fmt.Errorf(
			"decoding TxOut: %w\n"+
				"  valRaw[:%d]=%x\n"+
				"  txOutData[:%d]=%x",
			err,
			len(rawPrefix), rawPrefix,
			len(dataPrefix), dataPrefix,
		)
	}

	addrBytes, err := txOut.Address().Bytes()
	if err != nil {
		return nil, fmt.Errorf("encoding address: %w", err)
	}

	result := &ParsedUTxO{
		TxHash:      txHash,
		OutputIndex: outputIndex,
		Address:     addrBytes,
		Amount:      txOut.Amount().Uint64(),
	}

	addr := txOut.Address()
	pkh := addr.PaymentKeyHash()
	if pkh != zeroBlake2b224 {
		result.PaymentKey = pkh.Bytes()
	}
	skh := addr.StakeKeyHash()
	if skh != zeroBlake2b224 {
		result.StakingKey = skh.Bytes()
	}

	if dh := txOut.DatumHash(); dh != nil {
		result.DatumHash = dh.Bytes()
	}
	if d := txOut.Datum(); d != nil {
		result.Datum = d.Cbor()
	}
	if sr := txOut.ScriptRef(); sr != nil {
		result.ScriptRef = sr.RawScriptBytes()
	}

	if multiAsset := txOut.Assets(); multiAsset != nil {
		result.Assets = convertMultiAsset(multiAsset)
	}

	return result, nil
}

// convertMultiAsset converts a gouroboros MultiAsset into
// ParsedAsset slice.
func convertMultiAsset(
	multiAsset *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
) []ParsedAsset {
	var assets []ParsedAsset //nolint:prealloc
	for _, policyId := range multiAsset.Policies() {
		policyIdBytes := policyId.Bytes()
		for _, assetName := range multiAsset.Assets(policyId) {
			amount := multiAsset.Asset(policyId, assetName)
			assets = append(assets, ParsedAsset{
				PolicyId: policyIdBytes,
				Name:     assetName,
				Amount:   amount.Uint64(),
			})
		}
	}
	return assets
}

// ParseUTxOsFromFile reads and streams UTxOs from a UTxO-HD tvar
// file. The tvar format is array(1) containing a map of TxIn->TxOut
// entries. The map may use indefinite-length encoding (0xbf...0xff).
//
// Note: The entire file is read into memory because the CBOR stream
// decoder (gouroboros) requires a contiguous byte slice and does not
// support io.Reader streaming. Mainnet tvar files can be multi-GB.
// A future optimization could use mmap to avoid copying the file
// contents into Go heap memory.
func ParseUTxOsFromFile(
	path string,
	callback UTxOCallback,
) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("reading tvar file: %w", err)
	}

	// Parse outer array header using StreamDecoder
	decoder, err := cbor.NewStreamDecoder(data)
	if err != nil {
		return 0, fmt.Errorf(
			"creating stream decoder for tvar: %w",
			err,
		)
	}

	arrCount, _, _, err := decoder.DecodeArrayHeader()
	if err != nil {
		return 0, fmt.Errorf(
			"decoding tvar array header: %w",
			err,
		)
	}
	if arrCount != 1 {
		return 0, fmt.Errorf(
			"tvar has %d elements, expected 1",
			arrCount,
		)
	}

	// Inner data starts after the array header
	mapStart := decoder.Position()
	mapData := data[mapStart:]

	// Check for indefinite-length map (0xbf)
	if len(mapData) > 0 && mapData[0] == 0xbf {
		return parseIndefiniteUTxOMap(mapData, callback)
	}

	// Definite-length map - use existing streaming parser
	return ParseUTxOsStreaming(
		cbor.RawMessage(mapData), callback,
	)
}

// parseIndefiniteUTxOMap streams UTxO entries from an
// indefinite-length CBOR map (0xbf ... 0xff). Each entry is a
// TxIn key and TxOut value decoded using the existing parsers.
func parseIndefiniteUTxOMap(
	data []byte,
	callback UTxOCallback,
) (int, error) {
	if len(data) < 2 || data[0] != 0xbf {
		return 0, errors.New("expected indefinite map (0xbf)")
	}

	// Skip the 0xbf header byte
	decoder, err := cbor.NewStreamDecoder(data[1:])
	if err != nil {
		return 0, fmt.Errorf(
			"creating stream decoder for UTxO map: %w",
			err,
		)
	}

	entryIndex := 0
	next := func() (cbor.RawMessage, cbor.RawMessage, bool, error) {
		// Check for break byte (0xff) at current position.
		// The decoder operates on data[1:] (after the 0xbf
		// header), so the absolute index into data is 1+pos.
		absPos := 1 + decoder.Position()
		if absPos >= len(data) {
			return nil, nil, false, fmt.Errorf(
				"UTxO map entry %d: buffer exhausted "+
					"at position %d (len %d)",
				entryIndex, absPos, len(data),
			)
		}
		if data[absPos] == 0xff {
			return nil, nil, true, nil
		}

		idx := entryIndex
		entryIndex++

		var keyDummy any
		_, keyRaw, err := decoder.DecodeRaw(&keyDummy)
		if err != nil {
			return nil, nil, false, fmt.Errorf(
				"decoding UTxO key %d: %w", idx, err,
			)
		}

		var valDummy any
		_, valRaw, err := decoder.DecodeRaw(&valDummy)
		if err != nil {
			return nil, nil, false, fmt.Errorf(
				"decoding UTxO value %d: %w", idx, err,
			)
		}

		return keyRaw, valRaw, false, nil
	}

	return processBatchedUTxOs(next, callback)
}

// UTxOToModel converts a ParsedUTxO to a Dingo database Utxo model.
func UTxOToModel(u *ParsedUTxO, slot uint64) models.Utxo {
	utxo := models.Utxo{
		TxId:       u.TxHash,
		OutputIdx:  u.OutputIndex,
		PaymentKey: u.PaymentKey,
		StakingKey: u.StakingKey,
		Amount:     types.Uint64(u.Amount),
		AddedSlot:  slot,
		DatumHash:  u.DatumHash,
		Datum:      u.Datum,
		ScriptRef:  u.ScriptRef,
	}

	// Convert assets
	for _, a := range u.Assets {
		fingerprint := lcommon.NewAssetFingerprint(a.PolicyId, a.Name)
		utxo.Assets = append(utxo.Assets, models.Asset{
			PolicyId:    a.PolicyId,
			Name:        a.Name,
			NameHex:     []byte(hex.EncodeToString(a.Name)),
			Amount:      types.Uint64(a.Amount),
			Fingerprint: []byte(fingerprint.String()),
		})
	}

	return utxo
}

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

package kupo

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
)

func TestParsePatternFamilies(t *testing.T) {
	key := bytesOf(32, 0x11)
	fiveBit, err := bech32.ConvertBits(key, 8, 5, true)
	if err != nil {
		t.Fatal(err)
	}
	vk, err := bech32.Encode("vk", fiveBit)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytesOf(28, 0x22),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	addrBytes, err := addr.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	valid := []string{
		"*",
		"*/*",
		vk + "/*",
		hex.EncodeToString(addrBytes),
		strings.Repeat("ab", 28) + ".*",
		"3@" + strings.Repeat("cd", 32),
		"{42}",
	}
	for _, value := range valid {
		t.Run(value, func(t *testing.T) {
			if _, err := parsePattern(value); err != nil {
				t.Fatalf("parsePattern(%q): %v", value, err)
			}
		})
	}
}

func TestAssetPatternRequiresPolicyID(t *testing.T) {
	for _, value := range []string{"*.01", "*.*"} {
		if _, err := parsePattern(value); err == nil {
			t.Fatalf("parsePattern(%q) unexpectedly succeeded", value)
		}
	}
}

func TestFullScriptStakeAndPointerAddressesRemainExact(t *testing.T) {
	payment := bytesOf(lcommon.AddressHashSize, 0x41)
	staking := bytesOf(lcommon.AddressHashSize, 0x52)
	base, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyScript,
		lcommon.AddressNetworkTestnet,
		payment,
		staking,
	)
	if err != nil {
		t.Fatal(err)
	}
	pointerRaw := append(
		[]byte{byte(lcommon.AddressTypeKeyPointer << 4)},
		payment...,
	)
	pointerRaw = append(pointerRaw, 0x01, 0x00, 0x00)
	pointer, err := lcommon.NewAddressFromBytes(pointerRaw)
	if err != nil {
		t.Fatal(err)
	}
	for _, addr := range []lcommon.Address{base, pointer} {
		pattern, err := parsePattern(
			hex.EncodeToString(mustAddressBytes(t, addr)),
		)
		if err != nil {
			t.Fatal(err)
		}
		if pattern.kind != patternAddress {
			t.Fatalf(
				"address type %d parsed as kind %d",
				addr.Type(),
				pattern.kind,
			)
		}
		if !pattern.matchesAddress(addr) {
			t.Fatalf("exact pattern did not match address type %d", addr.Type())
		}
	}
}

func mustAddressBytes(t *testing.T, addr lcommon.Address) []byte {
	t.Helper()
	ret, err := addr.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return ret
}

func TestMetadataPatternFiltersQuery(t *testing.T) {
	pattern, err := parsePattern("{42}")
	if err != nil {
		t.Fatal(err)
	}
	matchAll, _, err := pattern.addressQuery()
	if err != nil || !matchAll {
		t.Fatalf("metadata address query = %v, %v", matchAll, err)
	}
	var query models.UtxoHistoryQuery
	if applyPatternFilters(&query, pattern) || query.MetadataLabel == nil ||
		*query.MetadataLabel != 42 {
		t.Fatalf("metadata filter query = %#v", query)
	}
}

func bytesOf(length int, value byte) []byte {
	ret := make([]byte, length)
	for i := range ret {
		ret[i] = value
	}
	return ret
}

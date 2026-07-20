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

package types_test

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"reflect"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
)

func TestTypesScanValue(t *testing.T) {
	testDefs := []struct {
		origValue     any
		expectedValue any
	}{
		{
			origValue: func(v types.Uint64) *types.Uint64 { return &v }(
				types.Uint64(123),
			),
			expectedValue: "123",
		},
		{
			origValue: func(v types.Rat) *types.Rat { return &v }(
				types.Rat{
					Rat: big.NewRat(3, 5),
				},
			),
			expectedValue: "3/5",
		},
	}
	var ok bool
	var tmpScanner sql.Scanner
	var tmpValuer driver.Valuer
	for _, testDef := range testDefs {
		tmpValuer, ok = testDef.origValue.(driver.Valuer)
		if !ok {
			t.Fatalf("test original value does not implement driver.Valuer")
		}
		valueOut, err := tmpValuer.Value()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !reflect.DeepEqual(valueOut, testDef.expectedValue) {
			t.Fatalf(
				"did not get expected value from Value(): got %#v, expected %#v",
				valueOut,
				testDef.expectedValue,
			)
		}
		tmpScanner, ok = testDef.origValue.(sql.Scanner)
		if !ok {
			t.Fatalf(
				"test original value does not implement sql.Scanner (it must be a pointer)",
			)
		}
		if err := tmpScanner.Scan(valueOut); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !reflect.DeepEqual(tmpScanner, testDef.origValue) {
			t.Fatalf(
				"did not get expected value after Scan(): got %#v, expected %#v",
				tmpScanner,
				testDef.origValue,
			)
		}
	}
}

// TestBlockTombstoneMarker confirms the marker is what the plugins write
// and detect: bytes start with the magic prefix and IsBlockTombstone
// matches them. The marker carries no embedded (slot, hash) — that
// information is reconstructed by each plugin from its own bp key.
func TestBlockTombstoneMarker(t *testing.T) {
	enc := types.BlockTombstone()
	if !types.IsBlockTombstone(enc) {
		t.Fatal("IsBlockTombstone returned false for BlockTombstone()")
	}
	if !bytes.Equal(enc[:4], types.BlockTombstoneMagic[:]) {
		t.Fatalf("marker prefix = %x, want %x", enc[:4], types.BlockTombstoneMagic[:])
	}
}

func TestParseBlockBlobKeyRoundTrip(t *testing.T) {
	const slot uint64 = 0x0102030405060708
	hash := bytes.Repeat([]byte{0xAB}, 32)

	key := types.BlockBlobKey(slot, hash)
	if len(key) != types.BlockBlobKeySize {
		t.Fatalf("encoded key length %d, want %d", len(key), types.BlockBlobKeySize)
	}
	gotSlot, gotHash, err := types.ParseBlockBlobKey(key)
	if err != nil {
		t.Fatalf("ParseBlockBlobKey: %v", err)
	}
	if gotSlot != slot {
		t.Fatalf("slot = %d, want %d", gotSlot, slot)
	}
	if !bytes.Equal(gotHash, hash) {
		t.Fatalf("hash = %x, want %x", gotHash, hash)
	}
}

func TestParseBlockBlobKeyRejectsMalformed(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
	}{
		{"too short", []byte("bp")},
		{"wrong prefix", append(
			[]byte("xx"), bytes.Repeat([]byte{0}, types.BlockBlobKeySize-2)...,
		)},
		{"too long", append(
			types.BlockBlobKey(1, bytes.Repeat([]byte{0}, 32)),
			byte('m'), byte('e'), byte('t'), byte('a'),
		)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := types.ParseBlockBlobKey(tc.key); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// TestHistoryExpiredErrorWraps ensures the typed error keeps satisfying
// errors.Is(err, ErrHistoryExpired) for callers that just want to detect
// the condition, while errors.As lets archive-proxy wrappers extract the
// (slot, hash).
func TestHistoryExpiredErrorWraps(t *testing.T) {
	hash := bytes.Repeat([]byte{0xCD}, 32)
	original := &types.HistoryExpiredError{Slot: 42, Hash: hash}

	if !errors.Is(original, types.ErrHistoryExpired) {
		t.Fatal("typed error must satisfy errors.Is(..., ErrHistoryExpired)")
	}

	var extracted *types.HistoryExpiredError
	if !errors.As(original, &extracted) {
		t.Fatal("errors.As did not extract the typed error")
	}
	if extracted.Slot != 42 || !bytes.Equal(extracted.Hash, hash) {
		t.Fatalf("extracted = %+v, want slot=42 hash=%x", extracted, hash)
	}
}

// TestNullableHashValueEmptyIsNull verifies that an empty (nil or zero-length)
// NullableHash serializes to SQL NULL, not an empty blob. This is the property
// that keeps nullable hash FK columns (e.g. utxo.spent_at_tx_id) from failing
// their FK to transaction(hash) for unspent/unreferenced UTxOs.
func TestNullableHashValueEmptyIsNull(t *testing.T) {
	cases := []struct {
		name string
		in   types.NullableHash
	}{
		{"nil", nil},
		{"empty", types.NullableHash{}},
		{"zero-len", types.NullableHash([]byte(""))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := tc.in.Value()
			if err != nil {
				t.Fatalf("Value() error: %v", err)
			}
			if v != nil {
				t.Fatalf("Value() = %#v, want nil (SQL NULL)", v)
			}
		})
	}
}

// TestNullableHashValueNonEmpty verifies a non-empty NullableHash serializes to
// its bytes.
func TestNullableHashValueNonEmpty(t *testing.T) {
	in := types.NullableHash(bytes.Repeat([]byte{0xAB}, 32))
	v, err := in.Value()
	if err != nil {
		t.Fatalf("Value() error: %v", err)
	}
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("Value() type = %T, want []byte", v)
	}
	if !bytes.Equal(b, in) {
		t.Fatalf("Value() = %x, want %x", b, in)
	}
}

// TestNullableHashScan round-trips NULL, []byte, and string sources.
func TestNullableHashScan(t *testing.T) {
	h := new(types.NullableHash)

	if err := h.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) error: %v", err)
	}
	if *h != nil {
		t.Fatalf("Scan(nil) = %#v, want nil", *h)
	}

	if err := h.Scan([]byte{}); err != nil {
		t.Fatalf("Scan(empty []byte) error: %v", err)
	}
	if *h != nil {
		t.Fatalf("Scan(empty []byte) = %#v, want nil", *h)
	}

	want := bytes.Repeat([]byte{0x01}, 32)
	if err := h.Scan(want); err != nil {
		t.Fatalf("Scan([]byte) error: %v", err)
	}
	if !bytes.Equal(*h, want) {
		t.Fatalf("Scan([]byte) = %x, want %x", *h, want)
	}

	if err := h.Scan("xyz"); err != nil {
		t.Fatalf("Scan(string) error: %v", err)
	}
	if string(*h) != "xyz" {
		t.Fatalf("Scan(string) = %q, want %q", string(*h), "xyz")
	}

	if err := h.Scan(123); err == nil {
		t.Fatal("Scan(int) should error")
	}
}

// Ensure NullableHash implements the driver interfaces.
var (
	_ driver.Valuer = types.NullableHash(nil)
	_ sql.Scanner   = (*types.NullableHash)(nil)
)

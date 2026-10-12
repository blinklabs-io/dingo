// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"encoding/hex"
	"testing"
)

func TestNormalizeCBORDatasetVectors(t *testing.T) {
	t.Parallel()
	vectors := []struct {
		name string
		in   string
		out  string
	}{
		{name: "indefinite-array", in: "9f0102ff", out: "820102"},
		{name: "indefinite-map", in: "bf0102ff", out: "a10102"},
		{name: "chunked-bytes", in: "5f420102420304ff", out: "4401020304"},
		{name: "chunked-text", in: "7f626162626364ff", out: "6461626364"},
		{name: "nested-indefinite", in: "9f9f01ffbf0102ffff", out: "828101a10102"},
		{name: "empty-indefinite-array", in: "9fff", out: "80"},
		{name: "empty-indefinite-map", in: "bfff", out: "a0"},
		{name: "non-minimal-integer", in: "1805", out: "05"},
		{name: "unsorted-map-keys", in: "a202010102", out: "a202010102"},
		{name: "duplicate-map-keys", in: "a201020103", out: "a201020103"},
		{name: "uint64-maximum", in: "1bffffffffffffffff", out: "1bffffffffffffffff"},
		{name: "rational-tag", in: "d81e820102", out: "d81e820102"},
		{name: "tagged-map", in: "d90103a10102", out: "d90103a10102"},
		{name: "unknown-tag", in: "d9050f01", out: "d9050f01"},
		{name: "tag-over-indefinite-array", in: "d81e9f0102ff", out: "d81e820102"},
		{name: "float16-width", in: "f93c00", out: "f93c00"},
		{name: "positive-bignum-fits", in: "c24296be", out: "1996be"},
		{name: "negative-bignum-fits", in: "c3435e0f31", out: "3a005e0f31"},
		{name: "bignum-leading-zeros", in: "c2420000", out: "00"},
		{name: "bignum-too-large", in: "c249010000000000000000", out: "c249010000000000000000"},
	}
	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			t.Parallel()
			input, err := hex.DecodeString(vector.in)
			if err != nil {
				t.Fatal(err)
			}
			want, err := hex.DecodeString(vector.out)
			if err != nil {
				t.Fatal(err)
			}
			got, err := NormalizeCBOR(input)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("NormalizeCBOR(%s) = %x, want %x", vector.in, got, want)
			}
		})
	}
}

func TestNormalizeCBORRejectsTrailingBytes(t *testing.T) {
	t.Parallel()
	if _, err := NormalizeCBOR([]byte{0x00, 0x01}); err == nil {
		t.Fatal("NormalizeCBOR accepted trailing bytes")
	}
}

func TestNormalizeCBORRejectsInvalidUTF8Text(t *testing.T) {
	t.Parallel()
	for _, input := range []string{
		"61ff",         // Invalid UTF-8 in a definite text string.
		"7f61c261a2ff", // Each chunk must be valid UTF-8 on its own.
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NormalizeCBOR(data); err == nil {
				t.Fatalf("NormalizeCBOR(%s) accepted invalid UTF-8 text", input)
			}
		})
	}
}

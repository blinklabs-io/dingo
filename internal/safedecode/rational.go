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

package safedecode

import (
	"errors"
)

// maxRationalMagnitudeBytes is the width of a Word64. A tag-30 component whose
// bignum magnitude, leading zero bytes aside, is wider than this cannot be
// represented as a Word64, which is the range gouroboros requires of a pool
// margin's numerator and denominator.
const maxRationalMagnitudeBytes = 8

// ErrOversizedRational reports a tag-30 rational with a numerator or
// denominator that does not fit in a Word64. It carries no part of the value:
// the decoder's own rejection text embeds the full decimal expansion of each
// component.
var ErrOversizedRational = errors.New(
	"rational component does not fit in 64 bits",
)

const (
	// maxScanDepth bounds the nesting the scan follows. Deeper input is left
	// to the decoder, which carries its own limits.
	maxScanDepth = 128

	cborTagRational = 30
	// cborTagEncodedCbor marks a byte string holding another CBOR item, which
	// is how transactions and bodies are wrapped on the wire.
	cborTagEncodedCbor = 24
)

var errScanMalformed = errors.New("malformed cbor")

// RejectOversizedRationals scans raw CBOR for a tag-30 rational with a
// component that does not fit in a Word64 and returns ErrOversizedRational if
// it finds one.
//
// gouroboros decodes each component as an arbitrary-size bignum and builds a
// big.Rat with SetFrac, whose GCD is quadratic in operand size, before any
// range check on the field. A 64 KB component costs the decoding goroutine
// about a second, and peer-supplied transactions and blocks are decoded before
// their size or body hash is checked. This scan runs first, in linear time and
// without allocating or copying payload bytes.
//
// It rejects only what it positively identifies. Input it cannot walk
// (truncated, reserved head bytes, nesting past maxScanDepth) returns nil so
// the real decoder reports its own error. Rationals inside a tag-24 byte
// string are scanned as the nested item they carry.
//
// The bound applies to every tag-30 rational, not only pool margins, and
// measures the value as written: a component with more than eight significant
// bytes is refused even where it would reduce to a Word64 once divided by its
// common factor, which the decoder would accept after the quadratic work.
func RejectOversizedRationals(data []byte) error {
	// Items are scanned until the input ends: callers pass one item, but a
	// block or transaction decoder reads only the first, so there is no
	// reason to look past it.
	_, err := scanItem(data, 0, 0)
	if errors.Is(err, ErrOversizedRational) {
		return err
	}
	return nil
}

// scanItem walks one data item starting at pos and returns the offset just
// past it.
func scanItem(data []byte, pos int, depth int) (int, error) {
	if depth > maxScanDepth {
		return 0, errScanMalformed
	}
	if pos >= len(data) {
		return 0, errScanMalformed
	}
	initial := data[pos]
	major := initial >> 5
	info := initial & 0x1f
	value, next, indefinite, err := readHead(data, pos, info)
	if err != nil {
		return 0, err
	}
	switch major {
	case 0, 1, 7:
		return next, nil
	case 2, 3:
		if indefinite {
			return scanChunks(data, next)
		}
		return skipBytes(data, next, value)
	case 4:
		return scanSequence(data, next, value, indefinite, depth)
	case 5:
		if !indefinite {
			if value > uint64(len(data)) {
				return 0, errScanMalformed
			}
			value *= 2
		}
		return scanSequence(data, next, value, indefinite, depth)
	default:
		switch value {
		case cborTagRational:
			return scanRational(data, next, depth)
		case cborTagEncodedCbor:
			return scanEncodedCbor(data, next, depth)
		}
		return scanItem(data, next, depth+1)
	}
}

// scanRational walks the content of a tag-30 item and checks the width of each
// component of a two-element array.
func scanRational(data []byte, pos int, depth int) (int, error) {
	if pos >= len(data) || data[pos] != 0x82 {
		return scanItem(data, pos, depth+1)
	}
	pos++
	for range 2 {
		end, err := scanComponent(data, pos, depth+1)
		if err != nil {
			return 0, err
		}
		pos = end
	}
	return pos, nil
}

// scanComponent walks one rational component. An integer always fits a
// Word64's width; a bignum (tag 2 or 3) is measured by its significant bytes.
func scanComponent(data []byte, pos int, depth int) (int, error) {
	if pos >= len(data) || data[pos]>>5 != 6 {
		return scanItem(data, pos, depth)
	}
	tag, next, _, err := readHead(data, pos, data[pos]&0x1f)
	if err != nil {
		return 0, err
	}
	if (tag != 2 && tag != 3) || next >= len(data) || data[next]>>5 != 2 {
		return scanItem(data, pos, depth)
	}
	significant, end, err := significantBytes(data, next)
	if err != nil {
		return 0, err
	}
	if significant > maxRationalMagnitudeBytes {
		return 0, ErrOversizedRational
	}
	return end, nil
}

// significantBytes returns the number of bytes in the byte string at pos once
// leading zero bytes are dropped, and the offset just past the string. The
// string may be definite or indefinite length.
func significantBytes(data []byte, pos int) (int, int, error) {
	value, next, indefinite, err := readHead(data, pos, data[pos]&0x1f)
	if err != nil {
		return 0, 0, err
	}
	significant, leading := 0, true
	count := func(chunk []byte) {
		for _, b := range chunk {
			if leading && b == 0 {
				continue
			}
			leading = false
			significant++
		}
	}
	if !indefinite {
		end, err := skipBytes(data, next, value)
		if err != nil {
			return 0, 0, err
		}
		count(data[next:end])
		return significant, end, nil
	}
	for {
		if next >= len(data) {
			return 0, 0, errScanMalformed
		}
		if data[next] == 0xff {
			return significant, next + 1, nil
		}
		if data[next]>>5 != 2 {
			return 0, 0, errScanMalformed
		}
		length, start, chunkIndefinite, err := readHead(
			data,
			next,
			data[next]&0x1f,
		)
		if err != nil || chunkIndefinite {
			return 0, 0, errScanMalformed
		}
		end, err := skipBytes(data, start, length)
		if err != nil {
			return 0, 0, err
		}
		count(data[start:end])
		next = end
	}
}

// scanEncodedCbor walks a tag-24 byte string, scanning the item it wraps. A
// wrapper that is not a definite-length byte string, or whose content is not
// one well-formed item, is skipped as ordinary data: only an oversized rational
// is reported.
func scanEncodedCbor(data []byte, pos int, depth int) (int, error) {
	end, err := scanItem(data, pos, depth+1)
	if err != nil {
		return 0, err
	}
	if data[pos]>>5 != 2 || data[pos]&0x1f == 31 {
		return end, nil
	}
	_, contentStart, _, err := readHead(data, pos, data[pos]&0x1f)
	if err != nil {
		return 0, err
	}
	_, scanErr := scanItem(data[contentStart:end], 0, depth+1)
	if errors.Is(scanErr, ErrOversizedRational) {
		return 0, scanErr
	}
	return end, nil
}

func scanSequence(
	data []byte,
	pos int,
	count uint64,
	indefinite bool,
	depth int,
) (int, error) {
	if indefinite {
		for {
			if pos >= len(data) {
				return 0, errScanMalformed
			}
			if data[pos] == 0xff {
				return pos + 1, nil
			}
			end, err := scanItem(data, pos, depth+1)
			if err != nil {
				return 0, err
			}
			pos = end
		}
	}
	// Each item takes at least one byte, which bounds a hostile count
	// without trusting it.
	if count > uint64(len(data)-pos) { //nolint:gosec // pos <= len(data)
		return 0, errScanMalformed
	}
	for range count {
		end, err := scanItem(data, pos, depth+1)
		if err != nil {
			return 0, err
		}
		pos = end
	}
	return pos, nil
}

// scanChunks skips the definite-length chunks of an indefinite byte or text
// string up to its break.
func scanChunks(data []byte, pos int) (int, error) {
	for {
		if pos >= len(data) {
			return 0, errScanMalformed
		}
		if data[pos] == 0xff {
			return pos + 1, nil
		}
		initial := data[pos]
		if initial>>5 != 2 && initial>>5 != 3 {
			return 0, errScanMalformed
		}
		length, next, indefinite, err := readHead(data, pos, initial&0x1f)
		if err != nil || indefinite {
			return 0, errScanMalformed
		}
		end, err := skipBytes(data, next, length)
		if err != nil {
			return 0, err
		}
		pos = end
	}
}

func skipBytes(data []byte, pos int, length uint64) (int, error) {
	if length > uint64(len(data)-pos) { //nolint:gosec // pos <= len(data)
		return 0, errScanMalformed
	}
	return pos + int(length), nil //nolint:gosec // length <= len(data)-pos
}

// readHead decodes the argument of the head at pos. indefinite is set for the
// indefinite-length form (additional information 31).
func readHead(
	data []byte,
	pos int,
	info byte,
) (value uint64, next int, indefinite bool, err error) {
	next = pos + 1
	switch {
	case info < 24:
		return uint64(info), next, false, nil
	case info == 31:
		return 0, next, true, nil
	case info > 27:
		return 0, 0, false, errScanMalformed
	}
	width := 1 << (info - 24)
	if width > len(data)-next {
		return 0, 0, false, errScanMalformed
	}
	for _, b := range data[next : next+width] {
		value = value<<8 | uint64(b)
	}
	return value, next + width, false, nil
}

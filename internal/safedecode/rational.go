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

	"github.com/blinklabs-io/gouroboros/cbor"
)

// Word64Bytes is the width of a Word64. From protocol version 12 the ledger
// decodes a bounded rational's numerator and denominator as Word64 as written,
// so a component with more significant bytes than this is invalid.
const Word64Bytes = 8

// ErrOversizedRational reports a tag-30 rational with a numerator or
// denominator wider than the limit the caller passed. It carries no part of the
// value: the decoder's own rejection text embeds the full decimal expansion of
// each component.
var ErrOversizedRational = errors.New(
	"rational component is wider than the allowed size",
)

const (
	cborTagRational = 30
	// cborTagEncodedCbor marks a byte string holding another CBOR item, which
	// is how transactions and bodies are wrapped on the wire.
	cborTagEncodedCbor = 24
)

// scanner carries the limits one scan applies. maxDepth mirrors the decoder's
// nesting limit, so input the decoder would refuse is not walked.
type scanner struct {
	limit    int
	maxDepth int
}

var errScanMalformed = errors.New("malformed cbor")

// RejectOversizedRationals scans raw CBOR for a tag-30 rational with a
// component whose significant magnitude is wider than maxMagnitudeBytes and
// returns ErrOversizedRational if it finds one. A limit of zero or less
// disables the check.
//
// gouroboros decodes each component as an arbitrary-size bignum and builds a
// big.Rat with SetFrac, whose GCD is quadratic in operand size, before any
// range check on the field. A 64 KB component costs the decoding goroutine
// about a second. This scan runs first, in linear time and without copying
// payload bytes, except to join the chunks of an indefinite-length tag-24
// wrapper.
//
// The caller chooses the limit, because what the ledger accepts depends on the
// protocol version. Below version 12 the ledger bounds the reduced rational,
// so a raw component of any width is legal and no raw limit is safe. From
// version 12 it requires each component to fit a Word64 as written, which is
// the limit Word64Bytes names.
//
// It rejects only what it positively identifies. Input it cannot walk
// (truncated, reserved head bytes, nesting past the decoder's own limit)
// returns nil so the real decoder reports its own error. A rational inside a
// tag-24 byte string is scanned as the nested item it carries.
func RejectOversizedRationals(data []byte, maxMagnitudeBytes int) error {
	if maxMagnitudeBytes <= 0 {
		return nil
	}
	// Only the first item is scanned: a block or transaction decoder reads
	// one item, so there is no reason to look past it.
	s := scanner{limit: maxMagnitudeBytes, maxDepth: cbor.MaxNestedLevels}
	_, err := s.item(data, 0, 0)
	if errors.Is(err, ErrOversizedRational) {
		return err
	}
	return nil
}

// item walks one data item starting at pos and returns the offset just
// past it.
func (s scanner) item(data []byte, pos int, depth int) (int, error) {
	if depth > s.maxDepth {
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
		return s.sequence(data, next, value, indefinite, depth)
	case 5:
		if !indefinite {
			if value > uint64(len(data)) {
				return 0, errScanMalformed
			}
			value *= 2
		}
		return s.sequence(data, next, value, indefinite, depth)
	default:
		switch value {
		case cborTagRational:
			return s.rational(data, next, depth)
		case cborTagEncodedCbor:
			return s.encodedCbor(data, next, depth)
		}
		return s.item(data, next, depth+1)
	}
}

// rational walks the content of a tag-30 item and checks the width of each
// component of a two-element array. gouroboros reads the array through any tag
// stack, a head of any width and the indefinite-length form, so the scan does
// too. An array that does not hold exactly two items is rejected by the
// decoder before it builds anything, so it is walked as ordinary data.
func (s scanner) rational(data []byte, pos int, depth int) (int, error) {
	start := pos
	for pos < len(data) && data[pos]>>5 == 6 {
		_, next, _, err := readHead(data, pos, data[pos]&0x1f)
		if err != nil {
			return 0, err
		}
		pos = next
	}
	if pos >= len(data) || data[pos]>>5 != 4 {
		return s.item(data, start, depth+1)
	}
	count, next, indefinite, err := readHead(data, pos, data[pos]&0x1f)
	if err != nil {
		return 0, err
	}
	if !indefinite && count != 2 {
		return s.item(data, start, depth+1)
	}
	pos = next
	for range 2 {
		if indefinite && (pos >= len(data) || data[pos] == 0xff) {
			return s.item(data, start, depth+1)
		}
		end, err := s.component(data, pos, depth+1)
		if err != nil {
			return 0, err
		}
		pos = end
	}
	if indefinite {
		if pos >= len(data) || data[pos] != 0xff {
			return s.item(data, start, depth+1)
		}
		pos++
	}
	return pos, nil
}

// component walks one rational component. An integer always fits a
// Word64's width; a bignum (tag 2 or 3) is measured by its significant bytes.
func (s scanner) component(data []byte, pos int, depth int) (int, error) {
	if pos >= len(data) || data[pos]>>5 != 6 {
		return s.item(data, pos, depth)
	}
	tag, next, _, err := readHead(data, pos, data[pos]&0x1f)
	if err != nil {
		return 0, err
	}
	if (tag != 2 && tag != 3) || next >= len(data) || data[next]>>5 != 2 {
		return s.item(data, pos, depth)
	}
	significant, end, err := significantBytes(data, next)
	if err != nil {
		return 0, err
	}
	if significant > s.limit {
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

// encodedCbor walks a tag-24 byte string, scanning the item it wraps. A
// wrapper that is not a byte string, or whose content is not one well-formed
// item, is skipped as ordinary data: only an oversized rational is reported.
// An indefinite-length wrapper is scanned over its concatenated chunks, since
// the wrapped item may straddle a chunk boundary; that is the only place the
// scan copies bytes.
func (s scanner) encodedCbor(data []byte, pos int, depth int) (int, error) {
	end, err := s.item(data, pos, depth+1)
	if err != nil {
		return 0, err
	}
	if data[pos]>>5 != 2 {
		return end, nil
	}
	_, contentStart, indefinite, err := readHead(
		data,
		pos,
		data[pos]&0x1f,
	)
	if err != nil {
		return 0, err
	}
	content := data[contentStart:end]
	if indefinite {
		content = joinChunks(data[contentStart : end-1])
	}
	_, scanErr := s.item(content, 0, depth+1)
	if errors.Is(scanErr, ErrOversizedRational) {
		return 0, scanErr
	}
	return end, nil
}

// joinChunks concatenates the definite-length chunks of an indefinite byte
// string, given the bytes between its head and its break. item has already
// validated the chunk structure.
func joinChunks(chunks []byte) []byte {
	var out []byte
	for pos := 0; pos < len(chunks); {
		length, next, _, err := readHead(chunks, pos, chunks[pos]&0x1f)
		if err != nil {
			return nil
		}
		end := next + int(length) //nolint:gosec // validated by item
		out = append(out, chunks[next:end]...)
		pos = end
	}
	return out
}

func (s scanner) sequence(
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
			end, err := s.item(data, pos, depth+1)
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
		end, err := s.item(data, pos, depth+1)
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

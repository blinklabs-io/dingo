// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

const maxCBORDepth = 1024

type cborHead struct {
	major      byte
	additional byte
	argument   uint64
	indefinite bool
}

type cborNormalizer struct {
	data []byte
	off  int
}

// NormalizeCBOR applies the cardano-cbor-dataset shape normalization rules.
// It preserves map order, duplicate entries, tags, and float widths.
func NormalizeCBOR(data []byte) ([]byte, error) {
	normalizer := cborNormalizer{data: data}
	normalized, err := normalizer.item(0)
	if err != nil {
		return nil, err
	}
	if normalizer.off != len(data) {
		return nil, fmt.Errorf("%d trailing bytes after the top-level CBOR item", len(data)-normalizer.off)
	}
	check := cborNormalizer{data: normalized}
	secondPass, err := check.item(0)
	if err != nil {
		return nil, fmt.Errorf("decode normalized CBOR: %w", err)
	}
	if check.off != len(normalized) || !bytes.Equal(secondPass, normalized) {
		return nil, errors.New("CBOR normalization is not idempotent")
	}
	return normalized, nil
}

func (n *cborNormalizer) item(depth int) ([]byte, error) {
	if depth > maxCBORDepth {
		return nil, errors.New("CBOR nesting exceeds the maximum depth")
	}
	start := n.off
	head, err := n.readHead()
	if err != nil {
		return nil, err
	}
	if head.major == 7 {
		if head.indefinite {
			return nil, errors.New("unexpected CBOR break")
		}
		return append([]byte(nil), n.data[start:n.off]...), nil
	}
	if head.indefinite && head.major != 2 && head.major != 3 && head.major != 4 && head.major != 5 {
		return nil, fmt.Errorf("indefinite CBOR item has unsupported major type %d", head.major)
	}

	switch head.major {
	case 0, 1:
		if head.indefinite {
			return nil, errors.New("indefinite CBOR integer")
		}
		return encodeHead(head.major, head.argument), nil
	case 2, 3:
		return n.string(head, depth)
	case 4:
		return n.array(head, depth)
	case 5:
		return n.mapValue(head, depth)
	case 6:
		if head.indefinite {
			return nil, errors.New("indefinite CBOR tag")
		}
		content, err := n.item(depth + 1)
		if err != nil {
			return nil, err
		}
		if head.argument == 2 || head.argument == 3 {
			magnitude, err := decodeDefiniteBytes(content)
			if err != nil {
				return nil, fmt.Errorf("decode bignum magnitude: %w", err)
			}
			magnitude = bytes.TrimLeft(magnitude, "\x00")
			if len(magnitude) <= 8 {
				var value uint64
				for _, b := range magnitude {
					value = value<<8 | uint64(b)
				}
				major := byte(0)
				if head.argument == 3 {
					major = 1
				}
				return encodeHead(major, value), nil
			}
			encoded := encodeHead(6, head.argument)
			encoded = append(encoded, encodeHead(2, uint64(len(magnitude)))...)
			return append(encoded, magnitude...), nil
		}
		encoded := encodeHead(6, head.argument)
		return append(encoded, content...), nil
	case 7:
		return nil, errors.New("unreachable CBOR simple value")
	default:
		return nil, fmt.Errorf("unknown CBOR major type %d", head.major)
	}
}

func (n *cborNormalizer) string(head cborHead, depth int) ([]byte, error) {
	var payload []byte
	if head.indefinite {
		for !n.nextIsBreak() {
			chunk, err := n.readHead()
			if err != nil {
				return nil, err
			}
			if chunk.major != head.major || chunk.indefinite {
				return nil, errors.New("indefinite CBOR string has an invalid chunk")
			}
			part, err := n.readBytes(chunk.argument)
			if err != nil {
				return nil, err
			}
			if head.major == 3 && !utf8.Valid(part) {
				return nil, errors.New("CBOR text chunk contains invalid UTF-8")
			}
			payload = append(payload, part...)
		}
		n.off++
	} else {
		part, err := n.readBytes(head.argument)
		if err != nil {
			return nil, err
		}
		if head.major == 3 && !utf8.Valid(part) {
			return nil, errors.New("CBOR text string contains invalid UTF-8")
		}
		payload = part
	}
	if depth > maxCBORDepth {
		return nil, errors.New("CBOR nesting exceeds the maximum depth")
	}
	encoded := encodeHead(head.major, uint64(len(payload)))
	return append(encoded, payload...), nil
}

func (n *cborNormalizer) array(head cborHead, depth int) ([]byte, error) {
	var items [][]byte
	if head.indefinite {
		for !n.nextIsBreak() {
			item, err := n.item(depth + 1)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		n.off++
	} else {
		for i := uint64(0); i < head.argument; i++ {
			item, err := n.item(depth + 1)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
	}
	encoded := encodeHead(4, uint64(len(items)))
	for _, item := range items {
		encoded = append(encoded, item...)
	}
	return encoded, nil
}

func (n *cborNormalizer) mapValue(head cborHead, depth int) ([]byte, error) {
	type pair struct {
		key   []byte
		value []byte
	}
	var entries []pair
	if head.indefinite {
		for !n.nextIsBreak() {
			key, err := n.item(depth + 1)
			if err != nil {
				return nil, err
			}
			if n.nextIsBreak() {
				return nil, errors.New("indefinite CBOR map ends after a key")
			}
			value, err := n.item(depth + 1)
			if err != nil {
				return nil, err
			}
			entries = append(entries, pair{key: key, value: value})
		}
		n.off++
	} else {
		for i := uint64(0); i < head.argument; i++ {
			key, err := n.item(depth + 1)
			if err != nil {
				return nil, err
			}
			value, err := n.item(depth + 1)
			if err != nil {
				return nil, err
			}
			entries = append(entries, pair{key: key, value: value})
		}
	}
	encoded := encodeHead(5, uint64(len(entries)))
	for _, entry := range entries {
		encoded = append(encoded, entry.key...)
		encoded = append(encoded, entry.value...)
	}
	return encoded, nil
}

func (n *cborNormalizer) readHead() (cborHead, error) {
	if n.off >= len(n.data) {
		return cborHead{}, errors.New("unexpected end of CBOR data")
	}
	initial := n.data[n.off]
	n.off++
	head := cborHead{major: initial >> 5, additional: initial & 0x1f}
	switch {
	case head.additional < 24:
		head.argument = uint64(head.additional)
	case head.additional == 24:
		if n.off+1 > len(n.data) {
			return cborHead{}, errors.New("truncated CBOR argument")
		}
		head.argument = uint64(n.data[n.off])
		n.off++
	case head.additional == 25:
		if n.off+2 > len(n.data) {
			return cborHead{}, errors.New("truncated CBOR argument")
		}
		head.argument = uint64(binary.BigEndian.Uint16(n.data[n.off : n.off+2]))
		n.off += 2
	case head.additional == 26:
		if n.off+4 > len(n.data) {
			return cborHead{}, errors.New("truncated CBOR argument")
		}
		head.argument = uint64(binary.BigEndian.Uint32(n.data[n.off : n.off+4]))
		n.off += 4
	case head.additional == 27:
		if n.off+8 > len(n.data) {
			return cborHead{}, errors.New("truncated CBOR argument")
		}
		head.argument = binary.BigEndian.Uint64(n.data[n.off : n.off+8])
		n.off += 8
	case head.additional == 31:
		head.indefinite = true
	default:
		return cborHead{}, fmt.Errorf("reserved CBOR additional information %d", head.additional)
	}
	return head, nil
}

func (n *cborNormalizer) readBytes(length uint64) ([]byte, error) {
	if length > uint64(math.MaxInt) {
		return nil, errors.New("CBOR string length exceeds addressable memory")
	}
	lengthInt := int(length)
	if lengthInt > len(n.data)-n.off {
		return nil, errors.New("truncated CBOR string")
	}
	start := n.off
	n.off += lengthInt
	return n.data[start:n.off], nil
}

func (n *cborNormalizer) nextIsBreak() bool {
	return n.off < len(n.data) && n.data[n.off] == 0xff
}

func decodeDefiniteBytes(data []byte) ([]byte, error) {
	n := cborNormalizer{data: data}
	head, err := n.readHead()
	if err != nil {
		return nil, err
	}
	if head.major != 2 || head.indefinite {
		return nil, errors.New("bignum magnitude must be a definite byte string")
	}
	value, err := n.readBytes(head.argument)
	if err != nil {
		return nil, err
	}
	if n.off != len(data) {
		return nil, errors.New("trailing data after bignum magnitude")
	}
	return value, nil
}

func encodeHead(major byte, argument uint64) []byte {
	prefix := major << 5
	switch {
	case argument < 24:
		return []byte{prefix | byte(argument)}
	case argument <= 0xff:
		return []byte{prefix | 24, byte(argument)}
	case argument <= 0xffff:
		return []byte{prefix | 25, byte((argument >> 8) & 0xff), byte(argument & 0xff)}
	case argument <= 0xffffffff:
		return []byte{
			prefix | 26,
			byte((argument >> 24) & 0xff),
			byte((argument >> 16) & 0xff),
			byte((argument >> 8) & 0xff),
			byte(argument & 0xff),
		}
	default:
		return []byte{
			prefix | 27,
			byte((argument >> 56) & 0xff),
			byte((argument >> 48) & 0xff),
			byte((argument >> 40) & 0xff),
			byte((argument >> 32) & 0xff),
			byte((argument >> 24) & 0xff),
			byte((argument >> 16) & 0xff),
			byte((argument >> 8) & 0xff),
			byte(argument & 0xff),
		}
	}
}

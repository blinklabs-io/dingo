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

package cardano

import (
	"fmt"
	"sort"
)

// byronCanonicalValue is a JSON value parsed under Byron's reference
// canonical-JSON grammar (the "canonical-json" Haskell package used by
// cardano-sl to hash genesis). Unlike encoding/json, object members are kept
// as an ordered list rather than collapsed into a map, so duplicate keys
// survive parsing instead of being merged with last-value-wins semantics.
type byronCanonicalValue struct {
	kind    byronCanonicalKind
	members []byronCanonicalMember // kind == byronCanonicalKindObject
	elems   []byronCanonicalValue  // kind == byronCanonicalKindArray
	str     string                 // kind == byronCanonicalKindString (unescaped content)
	raw     []byte                 // kind == byronCanonicalKindLiteral (number/true/false/null, verbatim)
}

type byronCanonicalMember struct {
	key   string
	value byronCanonicalValue
}

type byronCanonicalKind int

const (
	byronCanonicalKindObject byronCanonicalKind = iota
	byronCanonicalKindArray
	byronCanonicalKindString
	byronCanonicalKindLiteral
)

// parseByronCanonicalJSON parses genesisBytes under the Byron reference's
// canonical-JSON string grammar: only the \" and \\ escapes are accepted (no
// \/, \uXXXX, \n, \r, \t, \b, \f), object members are preserved in the order
// parsed including duplicates, and whitespace/member order in the input are
// otherwise unconstrained.
func parseByronCanonicalJSON(genesisBytes []byte) (byronCanonicalValue, error) {
	p := &byronCanonicalParser{buf: genesisBytes}
	p.skipWhitespace()
	v, err := p.parseValue()
	if err != nil {
		return byronCanonicalValue{}, err
	}
	p.skipWhitespace()
	if p.pos != len(p.buf) {
		return byronCanonicalValue{}, fmt.Errorf(
			"byron genesis JSON: unexpected trailing data at offset %d",
			p.pos,
		)
	}
	return v, nil
}

type byronCanonicalParser struct {
	buf []byte
	pos int
}

func (p *byronCanonicalParser) skipWhitespace() {
	for p.pos < len(p.buf) {
		switch p.buf[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *byronCanonicalParser) errf(format string, args ...any) error {
	return fmt.Errorf(
		"byron genesis JSON: "+format+" at offset %d",
		append(args, p.pos)...,
	)
}

func (p *byronCanonicalParser) parseValue() (byronCanonicalValue, error) {
	if p.pos >= len(p.buf) {
		return byronCanonicalValue{}, p.errf("unexpected end of input")
	}
	switch c := p.buf[p.pos]; {
	case c == '{':
		return p.parseObject()
	case c == '[':
		return p.parseArray()
	case c == '"':
		return p.parseString()
	case c == 't' || c == 'f' || c == 'n' || c == '-' || (c >= '0' && c <= '9'):
		return p.parseLiteral()
	default:
		return byronCanonicalValue{}, p.errf("unexpected character %q", c)
	}
}

func (p *byronCanonicalParser) parseObject() (byronCanonicalValue, error) {
	p.pos++ // consume '{'
	v := byronCanonicalValue{kind: byronCanonicalKindObject}
	p.skipWhitespace()
	if p.pos < len(p.buf) && p.buf[p.pos] == '}' {
		p.pos++
		return v, nil
	}
	for {
		p.skipWhitespace()
		if p.pos >= len(p.buf) || p.buf[p.pos] != '"' {
			return byronCanonicalValue{}, p.errf("expected object key")
		}
		keyVal, err := p.parseString()
		if err != nil {
			return byronCanonicalValue{}, err
		}
		p.skipWhitespace()
		if p.pos >= len(p.buf) || p.buf[p.pos] != ':' {
			return byronCanonicalValue{}, p.errf("expected ':' after object key")
		}
		p.pos++
		p.skipWhitespace()
		val, err := p.parseValue()
		if err != nil {
			return byronCanonicalValue{}, err
		}
		v.members = append(v.members, byronCanonicalMember{
			key:   keyVal.str,
			value: val,
		})
		p.skipWhitespace()
		if p.pos >= len(p.buf) {
			return byronCanonicalValue{}, p.errf("unterminated object")
		}
		switch p.buf[p.pos] {
		case ',':
			p.pos++
			continue
		case '}':
			p.pos++
			return v, nil
		default:
			return byronCanonicalValue{}, p.errf("expected ',' or '}' in object")
		}
	}
}

func (p *byronCanonicalParser) parseArray() (byronCanonicalValue, error) {
	p.pos++ // consume '['
	v := byronCanonicalValue{kind: byronCanonicalKindArray}
	p.skipWhitespace()
	if p.pos < len(p.buf) && p.buf[p.pos] == ']' {
		p.pos++
		return v, nil
	}
	for {
		p.skipWhitespace()
		val, err := p.parseValue()
		if err != nil {
			return byronCanonicalValue{}, err
		}
		v.elems = append(v.elems, val)
		p.skipWhitespace()
		if p.pos >= len(p.buf) {
			return byronCanonicalValue{}, p.errf("unterminated array")
		}
		switch p.buf[p.pos] {
		case ',':
			p.pos++
			continue
		case ']':
			p.pos++
			return v, nil
		default:
			return byronCanonicalValue{}, p.errf("expected ',' or ']' in array")
		}
	}
}

// parseString parses a JSON string literal under the Byron reference's
// restricted escape grammar: only \" and \\ are accepted. Every other
// backslash escape (\/, \uXXXX, \n, \r, \t, \b, \f) is rejected, even though
// encoding/json and general JSON both accept them.
func (p *byronCanonicalParser) parseString() (byronCanonicalValue, error) {
	start := p.pos
	p.pos++ // consume opening '"'
	var out []byte
	for {
		if p.pos >= len(p.buf) {
			return byronCanonicalValue{}, fmt.Errorf(
				"byron genesis JSON: unterminated string starting at offset %d",
				start,
			)
		}
		c := p.buf[p.pos]
		switch {
		case c == '"':
			p.pos++
			return byronCanonicalValue{
				kind: byronCanonicalKindString,
				str:  string(out),
			}, nil
		case c == '\\':
			if p.pos+1 >= len(p.buf) {
				return byronCanonicalValue{}, p.errf("unterminated escape sequence")
			}
			switch p.buf[p.pos+1] {
			case '"':
				out = append(out, '"')
			case '\\':
				out = append(out, '\\')
			default:
				return byronCanonicalValue{}, fmt.Errorf(
					"byron genesis JSON: escape \\%c is not permitted by the "+
						"Byron canonical-JSON grammar (only \\\" and \\\\ are) at offset %d",
					p.buf[p.pos+1],
					p.pos,
				)
			}
			p.pos += 2
		case c < 0x20:
			return byronCanonicalValue{}, p.errf(
				"unescaped control character 0x%02x in string", c,
			)
		default:
			out = append(out, c)
			p.pos++
		}
	}
}

// parseLiteral parses a number, true, false, or null token, preserving its
// exact source bytes verbatim so re-encoding never risks a numeric precision
// change (unlike a float64 round trip through encoding/json).
func (p *byronCanonicalParser) parseLiteral() (byronCanonicalValue, error) {
	start := p.pos
	if p.buf[p.pos] == 't' || p.buf[p.pos] == 'f' || p.buf[p.pos] == 'n' {
		for _, word := range []string{"true", "false", "null"} {
			if p.pos+len(word) <= len(p.buf) &&
				string(p.buf[p.pos:p.pos+len(word)]) == word {
				p.pos += len(word)
				return byronCanonicalValue{
					kind: byronCanonicalKindLiteral,
					raw:  []byte(word),
				}, nil
			}
		}
		return byronCanonicalValue{}, p.errf("invalid literal")
	}
	// Number: -? digits ('.' digits)? ([eE] [+-]? digits)?
	if p.buf[p.pos] == '-' {
		p.pos++
	}
	if p.pos >= len(p.buf) || p.buf[p.pos] < '0' || p.buf[p.pos] > '9' {
		return byronCanonicalValue{}, p.errf("invalid number")
	}
	for p.pos < len(p.buf) && p.buf[p.pos] >= '0' && p.buf[p.pos] <= '9' {
		p.pos++
	}
	if p.pos < len(p.buf) && p.buf[p.pos] == '.' {
		p.pos++
		if p.pos >= len(p.buf) || p.buf[p.pos] < '0' || p.buf[p.pos] > '9' {
			return byronCanonicalValue{}, p.errf("invalid number")
		}
		for p.pos < len(p.buf) && p.buf[p.pos] >= '0' && p.buf[p.pos] <= '9' {
			p.pos++
		}
	}
	if p.pos < len(p.buf) && (p.buf[p.pos] == 'e' || p.buf[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.buf) && (p.buf[p.pos] == '+' || p.buf[p.pos] == '-') {
			p.pos++
		}
		if p.pos >= len(p.buf) || p.buf[p.pos] < '0' || p.buf[p.pos] > '9' {
			return byronCanonicalValue{}, p.errf("invalid number")
		}
		for p.pos < len(p.buf) && p.buf[p.pos] >= '0' && p.buf[p.pos] <= '9' {
			p.pos++
		}
	}
	return byronCanonicalValue{
		kind: byronCanonicalKindLiteral,
		raw:  p.buf[start:p.pos],
	}, nil
}

// renderByronCanonicalHash serializes v the way the Byron reference does for
// genesis hashing: object members are stable-sorted by key so that every
// duplicate entry survives (matching the reference's "sort, do not merge"
// behavior), arrays keep their original order, and strings are re-escaped
// using only the two reference-supported escapes.
func renderByronCanonicalHash(v byronCanonicalValue) []byte {
	var buf []byte
	buf = appendByronCanonicalHash(buf, v)
	return buf
}

func appendByronCanonicalHash(buf []byte, v byronCanonicalValue) []byte {
	switch v.kind {
	case byronCanonicalKindObject:
		members := make([]byronCanonicalMember, len(v.members))
		copy(members, v.members)
		sort.SliceStable(members, func(i, j int) bool {
			return members[i].key < members[j].key
		})
		buf = append(buf, '{')
		for i, m := range members {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendByronCanonicalString(buf, m.key)
			buf = append(buf, ':')
			buf = appendByronCanonicalHash(buf, m.value)
		}
		buf = append(buf, '}')
	case byronCanonicalKindArray:
		buf = append(buf, '[')
		for i, e := range v.elems {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendByronCanonicalHash(buf, e)
		}
		buf = append(buf, ']')
	case byronCanonicalKindString:
		buf = appendByronCanonicalString(buf, v.str)
	case byronCanonicalKindLiteral:
		buf = append(buf, v.raw...)
	}
	return buf
}

func appendByronCanonicalString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		default:
			buf = append(buf, c)
		}
	}
	return append(buf, '"')
}

// renderByronFirstOccurrenceJSON re-serializes v as ordinary JSON, dropping
// every object member after the first with a given key at each nesting
// level. The Byron reference resolves duplicate keys by first occurrence
// when extracting schema fields (as opposed to genesis hashing, which
// preserves every duplicate -- see renderByronCanonicalHash); this produces a
// duplicate-free document so that a standard JSON decoder such as
// gouroboros's byron.NewByronGenesisFromReader (which, like encoding/json in
// general, resolves duplicates last-occurrence-wins) reads the same value the
// reference would.
func renderByronFirstOccurrenceJSON(v byronCanonicalValue) []byte {
	var buf []byte
	buf = appendByronFirstOccurrenceJSON(buf, v)
	return buf
}

func appendByronFirstOccurrenceJSON(buf []byte, v byronCanonicalValue) []byte {
	switch v.kind {
	case byronCanonicalKindObject:
		seen := make(map[string]struct{}, len(v.members))
		buf = append(buf, '{')
		first := true
		for _, m := range v.members {
			if _, dup := seen[m.key]; dup {
				continue
			}
			seen[m.key] = struct{}{}
			if !first {
				buf = append(buf, ',')
			}
			first = false
			buf = appendByronCanonicalString(buf, m.key)
			buf = append(buf, ':')
			buf = appendByronFirstOccurrenceJSON(buf, m.value)
		}
		buf = append(buf, '}')
	case byronCanonicalKindArray:
		buf = append(buf, '[')
		for i, e := range v.elems {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendByronFirstOccurrenceJSON(buf, e)
		}
		buf = append(buf, ']')
	case byronCanonicalKindString:
		buf = appendByronCanonicalString(buf, v.str)
	case byronCanonicalKindLiteral:
		buf = append(buf, v.raw...)
	}
	return buf
}

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
//
// Ported from chrono 0.4.45 (src/format/parse.rs parse_rfc3339_relaxed,
// src/format/scan.rs, src/format/parsed.rs, src/datetime/mod.rs
// timestamp_nanos_opt), the version Mithril release 2642.0 builds with,
// by way of mithril2pq (internal/mithril/chain/timestamp.go).
// chrono: Copyright 2014 Kang Seonghoon and contributors, Apache-2.0 or MIT.
// mithril2pq: Copyright 2026 Chris Guiney, Apache-2.0.
// Modifications: translated from Rust to Go.

package mithril

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Mithril reads certificate timestamps with chrono's DateTime<Utc>
// deserializer, which is not RFC 3339's strict grammar: it skips
// whitespace around fields, takes a space or a lower-case t as the
// separator, one-digit months, any number of fraction digits, a leap
// second (:60), "UTC" or an offset without a colon, and years beyond four
// digits with a sign. Go's time.Parse differs in both directions (it
// accepts a comma before the fraction and offsets of 24:00 or :60
// minutes), so the timestamp is parsed here as chrono parses it.

var errChronoTimestamp = errors.New("not a timestamp chrono accepts")

// chrono's NaiveDate range: years (i32::MIN >> 13) + 1 to (i32::MAX >> 13) - 1.
const (
	chronoMinYear = -262143
	chronoMaxYear = 262142
)

type chronoScanner struct{ s string }

func (p *chronoScanner) trim() {
	p.s = strings.TrimLeftFunc(p.s, unicode.IsSpace)
}

// number is scan::number with a minimum of one digit, the only minimum
// chrono's RFC 3339 reader uses: between 1 and max ASCII digits.
func (p *chronoScanner) number(max int) (int64, error) {
	if len(p.s) < 1 {
		return 0, errChronoTimestamp
	}
	var n int64
	i := 0
	for ; i < len(p.s) && i < max; i++ {
		c := p.s[i]
		if c < '0' || c > '9' {
			if i < 1 {
				return 0, errChronoTimestamp
			}
			break
		}
		if n > (math.MaxInt64-int64(c-'0'))/10 {
			return 0, errChronoTimestamp
		}
		n = n*10 + int64(c-'0')
	}
	p.s = p.s[i:]
	return n, nil
}

// field is a two-digit Numeric item: leading whitespace skipped, then 1 or 2
// digits, in [lo, hi].
func (p *chronoScanner) field(lo, hi int64) (int64, error) {
	p.trim()
	v, err := p.number(2)
	if err != nil || v < lo || v > hi {
		return 0, errChronoTimestamp
	}
	return v, nil
}

func (p *chronoScanner) literal(l string) error {
	if !strings.HasPrefix(p.s, l) {
		return errChronoTimestamp
	}
	p.s = p.s[len(l):]
	return nil
}

// spaceLiteral is Item::Space("") followed by a literal.
func (p *chronoScanner) spaceLiteral(l string) error {
	p.trim()
	return p.literal(l)
}

func (p *chronoScanner) year() (int64, error) {
	p.trim()
	var v int64
	var err error
	switch {
	case strings.HasPrefix(p.s, "-"):
		p.s = p.s[1:]
		v, err = p.number(math.MaxInt)
		v = -v
	case strings.HasPrefix(p.s, "+"):
		p.s = p.s[1:]
		v, err = p.number(math.MaxInt)
	default:
		v, err = p.number(4)
	}
	if err != nil || v < math.MinInt32 || v > math.MaxInt32 {
		return 0, errChronoTimestamp
	}
	return v, nil
}

// offset is scan::timezone_offset with a colon-or-space separator, Z
// allowed, minutes required and U+2212 accepted as a minus sign.
func (p *chronoScanner) offset() (int64, error) {
	if len(p.s) >= 3 && strings.EqualFold(chronoASCIIOnly(p.s[:3]), "UTC") {
		p.s = p.s[3:]
		return 0, nil
	}
	if strings.HasPrefix(p.s, "Z") || strings.HasPrefix(p.s, "z") {
		p.s = p.s[1:]
		return 0, nil
	}
	r, n := utf8.DecodeRuneInString(p.s)
	var neg bool
	switch r {
	case '+':
	case '-', '−':
		neg = true
	default:
		return 0, errChronoTimestamp
	}
	p.s = p.s[n:]
	if len(p.s) < 2 || !chronoIsDigit(p.s[0]) || !chronoIsDigit(p.s[1]) {
		return 0, errChronoTimestamp
	}
	hours := int64(p.s[0]-'0')*10 + int64(p.s[1]-'0')
	p.s = strings.TrimLeftFunc(
		p.s[2:],
		func(c rune) bool { return c == ':' || unicode.IsSpace(c) },
	)
	if len(p.s) < 2 || p.s[0] < '0' || p.s[0] > '5' || !chronoIsDigit(p.s[1]) {
		return 0, errChronoTimestamp
	}
	minutes := int64(p.s[0]-'0')*10 + int64(p.s[1]-'0')
	p.s = p.s[2:]
	off := hours*3600 + minutes*60
	if neg {
		off = -off
	}
	return off, nil
}

// chronoASCIIOnly returns s if it is ASCII and "" otherwise, so that EqualFold
// compares ASCII case only, as eq_ignore_ascii_case does.
func chronoASCIIOnly(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return ""
		}
	}
	return s
}

func chronoIsDigit(c byte) bool { return c >= '0' && c <= '9' }

// chronoDaysFromCivil counts days from 1970-01-01 to y-m-d in the proleptic
// Gregorian calendar.
func chronoDaysFromCivil(y, m, d int64) int64 {
	if m <= 2 {
		y--
	}
	era := y / 400
	if y < 0 && y%400 != 0 {
		era--
	}
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

func chronoDaysIn(y, m int64) int64 {
	switch m {
	case 2:
		if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	}
	return 31
}

// parseChronoTimestamp parses s as chrono's DateTime<Utc> deserializer
// does and returns the UTC Unix seconds and the nanoseconds past them,
// which are 1e9 or more for a leap second.
func parseChronoTimestamp(s string) (int64, int64, error) {
	p := &chronoScanner{s: s}
	year, err := p.year()
	if err != nil {
		return 0, 0, err
	}
	var month, day, hour, minute, second, nano int64
	steps := []func() error{
		func() error { return p.spaceLiteral("-") },
		func() (err error) { month, err = p.field(1, 12); return },
		func() error { return p.spaceLiteral("-") },
		func() (err error) { day, err = p.field(1, 31); return },
		func() error {
			if p.s == "" || (p.s[0] != 'T' && p.s[0] != 't' && p.s[0] != ' ') {
				return errChronoTimestamp
			}
			p.s = p.s[1:]
			return nil
		},
		func() (err error) { hour, err = p.field(0, 23); return },
		func() error { return p.spaceLiteral(":") },
		func() (err error) { minute, err = p.field(0, 59); return },
		func() error { return p.spaceLiteral(":") },
		func() (err error) { second, err = p.field(0, 60); return },
		func() error {
			if !strings.HasPrefix(p.s, ".") {
				return nil
			}
			p.s = p.s[1:]
			before := len(p.s)
			v, err := p.number(9)
			if err != nil {
				return err
			}
			nano = v * int64(math.Pow10(9-(before-len(p.s))))
			p.s = strings.TrimLeftFunc(
				p.s,
				func(c rune) bool { return c >= '0' && c <= '9' },
			)
			return nil
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return 0, 0, err
		}
	}
	p.trim()
	off, err := p.offset()
	if err != nil {
		return 0, 0, err
	}
	p.trim()
	if p.s != "" {
		return 0, 0, errChronoTimestamp
	}
	if year < chronoMinYear || year > chronoMaxYear ||
		day > chronoDaysIn(year, month) ||
		off <= -86400 ||
		off >= 86400 {
		return 0, 0, errChronoTimestamp
	}
	if second == 60 { // chrono's leap second: :59 plus a second of nanoseconds
		second, nano = 59, nano+1e9
	}
	secs := chronoDaysFromCivil(
		year,
		month,
		day,
	)*86400 + hour*3600 + minute*60 + second - off
	if secs < chronoDaysFromCivil(chronoMinYear, 1, 1)*86400 ||
		secs >= chronoDaysFromCivil(chronoMaxYear+1, 1, 1)*86400 {
		return 0, 0, errChronoTimestamp
	}
	return secs, nano, nil
}

// chronoUnixNanos is chrono's timestamp_nanos_opt().unwrap_or_default() of the
// parsed timestamp: zero when the nanoseconds do not fit an i64, as chrono
// computes them ((secs+1)*1e9 + (nanos-1e9) for negative secs).
func chronoUnixNanos(s string) (int64, error) {
	secs, nanos, err := parseChronoTimestamp(s)
	if err != nil {
		return 0, fmt.Errorf("certificate timestamp %q: %w", s, err)
	}
	if secs < 0 {
		secs, nanos = secs+1, nanos-1e9
	}
	v := new(big.Int).Mul(big.NewInt(secs), big.NewInt(1e9))
	if !v.IsInt64() {
		return 0, nil
	}
	v.Add(v, big.NewInt(nanos))
	if !v.IsInt64() {
		return 0, nil
	}
	return v.Int64(), nil
}

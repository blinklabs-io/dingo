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

package koiosparity

import (
	"encoding/json"
	"fmt"
)

// koiosMaxDecodeDepth is the container depth beyond which elideDeepJSON
// replaces a value with null. It must stay above the deepest container any
// decoded struct reads (TestTxInfoReadPathsAreShallowerThanElisionDepth
// enforces this for KoiosTxInfoItem), and well below encoding/json's fixed
// nesting limit of 10000.
const koiosMaxDecodeDepth = 64

// elideDeepJSON validates data as a single JSON value and returns it with
// every array or object that opens deeper than maxDepth replaced by null.
//
// encoding/json rejects a whole document once any value nests past 10000
// levels, even inside a field the target struct does not have. Koios
// renders ledger values (native scripts, Plutus data, reference scripts) as
// nested JSON, and the ledger bounds their nesting only by transaction size,
// so a real transaction can exceed that limit: preprod tx
// f90dce5765108da976abdbb9fc618f9a6ffd9fa4d93b2f288eed1808545424c9 carries
// native_scripts 10770 levels deep. Eliding below the depth any struct reads
// changes nothing that is decoded, so no bound is imposed on the data itself.
//
// The scan is iterative, so input depth costs heap rather than call stack,
// and it validates the elided regions too, so malformed input still fails
// rather than being hidden behind a null. Scalars are checked with
// json.Valid, which never nests. data is returned unchanged when nothing
// was elided.
func elideDeepJSON(data []byte, maxDepth int) ([]byte, error) {
	const (
		stValue        = iota // a value is required
		stValueOrClose        // just after '[': a value or ']'
		stKey                 // just after ',' in an object: a key
		stKeyOrClose          // just after '{': a key or '}'
		stColon               // just after a key
		stCommaOrClose        // just after a value inside a container
		stEnd                 // the top-level value is complete
	)
	var (
		stack     []byte // open containers; len(stack) is the current depth
		state     = stValue
		skipDepth = -1 // depth outside the container being elided, or -1
		skipStart int
		out       []byte
		copied    int
	)
	finishValue := func() {
		if len(stack) == 0 {
			state = stEnd
		} else {
			state = stCommaOrClose
		}
	}
	n := len(data)
	for i := 0; i < n; {
		c := data[i]
		switch c {
		case ' ', '\t', '\n', '\r':
			i++
			continue
		case '{', '[':
			if state != stValue && state != stValueOrClose {
				return nil, fmt.Errorf("unexpected %q at offset %d", c, i)
			}
			if skipDepth < 0 && len(stack) >= maxDepth {
				skipDepth = len(stack)
				skipStart = i
			}
			stack = append(stack, c)
			if c == '{' {
				state = stKeyOrClose
			} else {
				state = stValueOrClose
			}
			i++
		case '}', ']':
			open := byte('{')
			allowed := state == stKeyOrClose || state == stCommaOrClose
			if c == ']' {
				open = '['
				allowed = state == stValueOrClose || state == stCommaOrClose
			}
			if !allowed || len(stack) == 0 || stack[len(stack)-1] != open {
				return nil, fmt.Errorf("unexpected %q at offset %d", c, i)
			}
			stack = stack[:len(stack)-1]
			i++
			if len(stack) == skipDepth {
				out = append(out, data[copied:skipStart]...)
				out = append(out, "null"...)
				copied = i
				skipDepth = -1
			}
			finishValue()
		case ',':
			if state != stCommaOrClose || len(stack) == 0 {
				return nil, fmt.Errorf("unexpected ',' at offset %d", i)
			}
			if stack[len(stack)-1] == '{' {
				state = stKey
			} else {
				state = stValue
			}
			i++
		case ':':
			if state != stColon {
				return nil, fmt.Errorf("unexpected ':' at offset %d", i)
			}
			state = stValue
			i++
		case '"':
			isKey := state == stKey || state == stKeyOrClose
			if !isKey && state != stValue && state != stValueOrClose {
				return nil, fmt.Errorf("unexpected string at offset %d", i)
			}
			end := i + 1
			for end < n && data[end] != '"' {
				if data[end] == '\\' {
					end++
				}
				end++
			}
			if end >= n {
				return nil, fmt.Errorf("unterminated string at offset %d", i)
			}
			end++
			if !json.Valid(data[i:end]) {
				return nil, fmt.Errorf("invalid string at offset %d", i)
			}
			i = end
			if isKey {
				state = stColon
			} else {
				finishValue()
			}
		default:
			if state != stValue && state != stValueOrClose {
				return nil, fmt.Errorf("unexpected %q at offset %d", c, i)
			}
			end := i
			for end < n && !isJSONDelimiter(data[end]) {
				end++
			}
			if !json.Valid(data[i:end]) {
				return nil, fmt.Errorf(
					"invalid literal %q at offset %d", data[i:end], i,
				)
			}
			i = end
			finishValue()
		}
	}
	if state != stEnd {
		return nil, fmt.Errorf("unexpected end of JSON input at offset %d", n)
	}
	if out == nil {
		return data, nil
	}
	return append(out, data[copied:]...), nil
}

func isJSONDelimiter(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', ',', ':', '[', ']', '{', '}', '"':
		return true
	}
	return false
}

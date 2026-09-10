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

package storagetest

import (
	"net/url"
	"strings"
)

// EscapeLibpqValue quotes and backslash-escapes a value for a libpq
// keyword/value connection string, so a password containing a space,
// single quote, or backslash -- all legal in a Postgres password --
// produces a well-formed DSN instead of breaking the conninfo parse.
//
// Lives here, not in internal/test/conformance or a metadata plugin
// package, so both a plugin's own conformance_test.go (which imports this
// package already) and internal/test/conformance (which imports the plugin
// packages directly to open a real backend) can share it without an import
// cycle.
func EscapeLibpqValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}

// PostgresDSNWithSearchPath returns dsn with the connection's search_path
// pinned to schema. dsn may be a libpq keyword/value string or a URL
// (postgres://user:pass@host/db) -- an operator-supplied POSTGRES_DSN
// override may legitimately be either, and appending " options='...'" text
// to a URL produces a malformed DSN, so the two forms need different
// injection. Either form's DSN may already carry its own "options" value
// (e.g. options=-cstatement_timeout=5000); libpq's "options" keyword takes
// a single space-separated list of "-c name=value" flags, and both a
// duplicate URL query parameter and a duplicate keyword/value pair are
// resolved by last-value-wins, so blindly setting a new "options" here
// would silently discard whatever the operator already configured. Merge
// the search_path flag onto the existing value instead of replacing it.
func PostgresDSNWithSearchPath(dsn, schema string) string {
	searchPathOption := "-csearch_path=" + schema
	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err == nil {
			query := parsed.Query()
			query.Set(
				"options",
				mergeLibpqOptions(query.Get("options"), searchPathOption),
			)
			parsed.RawQuery = query.Encode()
			return parsed.String()
		}
	}

	tokens := splitLibpqTokens(dsn)
	var existingOptions string
	remaining := make([]string, 0, len(tokens)+1)
	for _, tok := range tokens {
		if key, value, ok := strings.Cut(tok, "="); ok && key == "options" {
			existingOptions = unquoteLibpqValue(value)
			continue
		}
		remaining = append(remaining, tok)
	}
	merged := mergeLibpqOptions(existingOptions, searchPathOption)
	remaining = append(remaining, "options="+EscapeLibpqValue(merged))
	return strings.Join(remaining, " ")
}

// mergeLibpqOptions appends addition to a libpq "options" value's existing
// space-separated flag list, or returns addition unchanged if there is no
// existing value.
func mergeLibpqOptions(existing, addition string) string {
	existing = strings.TrimSpace(existing)
	if existing == "" {
		return addition
	}
	return existing + " " + addition
}

// splitLibpqTokens splits a libpq keyword/value string into "key=value"
// tokens on unquoted whitespace, so a single-quoted value containing a
// space (as EscapeLibpqValue produces) is kept as one token instead of
// being split apart.
func splitLibpqTokens(dsn string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false
	escaped := false
	for _, r := range dsn {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case inQuotes && r == '\\':
			current.WriteRune(r)
			escaped = true
		case r == '\'':
			current.WriteRune(r)
			inQuotes = !inQuotes
		case r == ' ' && !inQuotes:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// unquoteLibpqValue reverses EscapeLibpqValue's quoting on a single
// keyword/value token's value, or returns it unchanged if it was never
// quoted (legal for a value with no whitespace or special characters).
func unquoteLibpqValue(value string) string {
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		inner := value[1 : len(value)-1]
		inner = strings.ReplaceAll(inner, `\'`, `'`)
		inner = strings.ReplaceAll(inner, `\\`, `\`)
		return inner
	}
	return value
}

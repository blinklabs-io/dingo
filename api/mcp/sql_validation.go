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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const maxSQLiteQueryLength = 64 << 10

// ValidateReadOnlyQuery accepts one read-only SQLite statement. SQLite still
// checks syntax; the connection's mode=ro and query_only enforce the final boundary.
func ValidateReadOnlyQuery(query string) error {
	if len(query) > maxSQLiteQueryLength {
		return errors.New("SQL query exceeds 64 KiB")
	}
	tokens, err := sqliteQueryTokens(query)
	if err != nil {
		return err
	}
	if len(tokens) > 0 && tokens[len(tokens)-1] == ";" {
		tokens = tokens[:len(tokens)-1]
	}
	if slices.Contains(tokens, ";") {
		return errors.New("multiple SQL statements are not permitted")
	}
	if len(tokens) == 0 {
		return errors.New("empty query")
	}
	explained := tokens[0] == "EXPLAIN"
	if explained {
		tokens = tokens[1:]
		if len(tokens) >= 2 && tokens[0] == "QUERY" && tokens[1] == "PLAN" {
			tokens = tokens[2:]
		}
	}
	if len(tokens) == 0 {
		return errors.New("missing SQL statement")
	}
	if tokens[0] == "PRAGMA" {
		// Some PRAGMAs act during preparation even under EXPLAIN.
		if len(tokens) == 5 && tokens[2] == "(" && tokens[4] == ")" &&
			slices.Contains(
				[]string{"TABLE_INFO", "INDEX_LIST", "INDEX_INFO", "FOREIGN_KEY_LIST"},
				tokens[1],
			) {
			return nil
		}
		return errors.New(
			"only read-only metadata PRAGMAs (table_info, index_list, index_info, foreign_key_list) are allowed",
		)
	}
	// Unlike PRAGMAs, ordinary statements under EXPLAIN return bytecode or
	// a query plan without executing the statement, including DDL and DML.
	if explained {
		return nil
	}
	if tokens[0] == "WITH" {
		tokens, err = sqliteAfterCTEs(tokens[1:])
		if err != nil {
			return err
		}
	}
	if len(tokens) == 0 {
		return errors.New("missing SQL statement after WITH")
	}
	switch tokens[0] {
	case "SELECT", "VALUES":
		return nil
	}
	return fmt.Errorf("query must be read-only; statement %s is forbidden", tokens[0])
}

// sqliteAfterCTEs skips only WITH definitions, never the following statement.
// CTE bodies may contain nested SELECTs, so the first SELECT token is not enough.
func sqliteAfterCTEs(tokens []string) ([]string, error) {
	if len(tokens) > 0 && tokens[0] == "RECURSIVE" {
		tokens = tokens[1:]
	}
	for len(tokens) > 0 {
		tokens = tokens[1:] // CTE name; SQLite validates identifier syntax.
		if len(tokens) > 0 && tokens[0] == "(" {
			tokens = sqliteAfterGroup(tokens)
		}
		if len(tokens) == 0 || tokens[0] != "AS" {
			break
		}
		tokens = tokens[1:]
		if len(tokens) > 0 && tokens[0] == "NOT" {
			tokens = tokens[1:]
		}
		if len(tokens) > 0 && tokens[0] == "MATERIALIZED" {
			tokens = tokens[1:]
		}
		if len(tokens) == 0 || tokens[0] != "(" {
			break
		}
		tokens = sqliteAfterGroup(tokens)
		if len(tokens) == 0 {
			break
		}
		if tokens[0] != "," {
			return tokens, nil
		}
		tokens = tokens[1:]
	}
	return nil, errors.New("invalid or incomplete WITH clause")
}

func sqliteAfterGroup(tokens []string) []string {
	depth := 0
	for i, token := range tokens {
		switch token {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return tokens[i+1:]
			}
		}
	}
	return nil
}

// sqliteQueryTokens keeps SQL structure separate from comments and quoted data.
// A quoted token uses a sentinel so its contents cannot masquerade as SQL keywords.
func sqliteQueryTokens(query string) ([]string, error) {
	if strings.ContainsRune(query, 0) {
		return nil, errors.New("NUL is not permitted in SQL")
	}
	var tokens []string
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case strings.HasPrefix(query[i:], "\ufeff"):
			// SQLite recognizes a UTF-8 BOM as whitespace at token boundaries.
			i += len("\ufeff")
		case strings.ContainsRune(" \t\r\n\v\f", rune(c)):
			i++
		case strings.HasPrefix(query[i:], "--"):
			if end := strings.IndexByte(query[i:], '\n'); end >= 0 {
				i += end + 1
			} else {
				i = len(query)
			}
		case strings.HasPrefix(query[i:], "/*"):
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				i = len(query)
				continue
			}
			i += end + 4
		case c == '\'' || c == '"' || c == '`' || c == '[':
			endQuote := c
			if c == '[' {
				endQuote = ']'
			}
			i++
			closed := false
			for i < len(query) {
				if query[i] != endQuote {
					i++
					continue
				}
				i++
				if c != '[' && i < len(query) && query[i] == endQuote {
					i++
					continue
				}
				closed = true
				break
			}
			if !closed {
				return nil, errors.New("unterminated SQL quote")
			}
			tokens = append(tokens, "<quoted>")
		case sqliteWordByte(c):
			start := i
			for i < len(query) && sqliteWordByte(query[i]) {
				i++
			}
			tokens = append(tokens, strings.ToUpper(query[start:i]))
		default:
			tokens = append(tokens, string(c))
			i++
		}
	}
	return tokens, nil
}

func sqliteWordByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' ||
		c == '$' ||
		c >= 0x80
}

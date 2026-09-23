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
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
)

const (
	maxCellLength   = 128
	maxStringLength = 1024
	maxBlobPreview  = 32
)

// SanitizeExternalString cleans an external or user-provided string by stripping
// invisible Unicode formatting characters, bidirectional overrides, and control characters
// that are commonly exploited in prompt injection or terminal manipulation attacks.
func SanitizeExternalString(input string) string {
	var sb strings.Builder
	sb.Grow(len(input))

	for _, r := range input {
		// Strip zero-width spaces, invisible characters, and bidirectional overrides
		switch r {
		case '\u200B', '\u200C', '\u200D', '\u200E', '\u200F',
			'\u202A', '\u202B', '\u202C', '\u202D', '\u202E',
			'\u2066', '\u2067', '\u2068', '\u2069', '\uFEFF':
			continue
		}

		// Allow newline, tab, and standard printable characters
		if r == '\n' || r == '\t' || r == '\r' ||
			(unicode.IsPrint(r) && !unicode.IsControl(r)) {
			sb.WriteRune(r)
		}
	}

	res := sb.String()
	if len(res) > maxStringLength {
		return res[:maxStringLength] + fmt.Sprintf(
			"... [truncated %d chars]",
			len(res)-maxStringLength,
		)
	}
	return res
}

// WrapUntrustedChainData encapsulates user-supplied on-chain strings (e.g. metadata text,
// datum representations, anchor descriptions) in explicit delimiters to alert LLMs
// that the contents are untrusted raw data rather than system directives.
func WrapUntrustedChainData(data string) string {
	sanitized := SanitizeExternalString(data)
	return fmt.Sprintf(
		"<untrusted_chain_data>\n%s\n</untrusted_chain_data>",
		sanitized,
	)
}

// FormatCell formats a single database column value for LLM consumption, summarizing large
// binary blobs or hex strings so they do not exhaust the client's context window.
func FormatCell(val any) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case []byte:
		if len(v) == 0 {
			return "0x"
		}
		if len(v) > maxBlobPreview {
			prefix := hex.EncodeToString(v[:maxBlobPreview/2])
			suffix := hex.EncodeToString(v[len(v)-(maxBlobPreview/2):])
			return fmt.Sprintf("0x%s...%s (bytes: %d)", prefix, suffix, len(v))
		}
		return "0x" + hex.EncodeToString(v)

	case string:
		clean := SanitizeExternalString(v)
		// Clean markdown pipes so table layout is not corrupted
		clean = strings.ReplaceAll(clean, "|", "\\|")
		clean = strings.ReplaceAll(clean, "\n", " ")
		if len(clean) > maxCellLength {
			return clean[:maxCellLength] + "..."
		}
		return clean

	default:
		s := fmt.Sprintf("%v", v)
		s = strings.ReplaceAll(s, "|", "\\|")
		s = strings.ReplaceAll(s, "\n", " ")
		if len(s) > maxCellLength {
			return s[:maxCellLength] + "..."
		}
		return s
	}
}

// FormatMarkdownTable converts columns and row records into a clean GitHub Markdown table.
func FormatMarkdownTable(columns []string, rows [][]string) string {
	if len(columns) == 0 {
		return "*(empty result)*\n"
	}

	var sb strings.Builder

	// Header row
	sb.WriteString("| ")
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(" |\n")

	// Separator row
	sb.WriteString("| ")
	for i := range columns {
		if i > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString("---")
	}
	sb.WriteString(" |\n")

	// Data rows
	for _, row := range rows {
		sb.WriteString("| ")
		for i, cell := range row {
			if i > 0 {
				sb.WriteString(" | ")
			}
			sb.WriteString(cell)
		}
		sb.WriteString(" |\n")
	}

	return sb.String()
}

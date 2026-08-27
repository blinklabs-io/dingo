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
// injection.
func PostgresDSNWithSearchPath(dsn, schema string) string {
	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err == nil {
			query := parsed.Query()
			query.Set("options", "-csearch_path="+schema)
			parsed.RawQuery = query.Encode()
			return parsed.String()
		}
	}
	return dsn + " options='-csearch_path=" + schema + "'"
}

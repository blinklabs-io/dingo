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
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestPostgresDSNWithSearchPathPreservesExistingOptions proves
// PostgresDSNWithSearchPath merges search_path onto an existing "options"
// value instead of replacing it: an operator-supplied
// options=-cstatement_timeout=5000 must survive alongside the injected
// search_path, for both DSN forms this function accepts. pgx.ParseConfig
// exposes a DSN's "options" value verbatim as RuntimeParams["options"] (it
// does not unpack individual "-c name=value" flags), so both flags are
// asserted as substrings of that one value.
func TestPostgresDSNWithSearchPathPreservesExistingOptions(t *testing.T) {
	t.Run("keyword/value DSN with existing options", func(t *testing.T) {
		dsn := "host=localhost port=5432 user=postgres password=postgres " +
			"dbname=dingo_test options='-cstatement_timeout=5000'"
		got := PostgresDSNWithSearchPath(dsn, "conformance_test")

		cfg, err := pgx.ParseConfig(got)
		require.NoError(t, err)
		options := cfg.RuntimeParams["options"]
		require.Contains(t, options, "-cstatement_timeout=5000")
		require.Contains(t, options, "-csearch_path=conformance_test")
	})

	t.Run("URL DSN with existing options", func(t *testing.T) {
		dsn := "postgres://postgres:postgres@localhost:5432/dingo_test" +
			"?options=-cstatement_timeout%3D5000"
		got := PostgresDSNWithSearchPath(dsn, "conformance_test")

		cfg, err := pgx.ParseConfig(got)
		require.NoError(t, err)
		options := cfg.RuntimeParams["options"]
		require.Contains(t, options, "-cstatement_timeout=5000")
		require.Contains(t, options, "-csearch_path=conformance_test")
	})

	t.Run("keyword/value DSN without existing options", func(t *testing.T) {
		dsn := "host=localhost port=5432 user=postgres password=postgres " +
			"dbname=dingo_test"
		got := PostgresDSNWithSearchPath(dsn, "conformance_test")

		cfg, err := pgx.ParseConfig(got)
		require.NoError(t, err)
		require.Contains(
			t,
			cfg.RuntimeParams["options"],
			"-csearch_path=conformance_test",
		)
	})

	t.Run("URL DSN without existing options", func(t *testing.T) {
		dsn := "postgres://postgres:postgres@localhost:5432/dingo_test"
		got := PostgresDSNWithSearchPath(dsn, "conformance_test")

		cfg, err := pgx.ParseConfig(got)
		require.NoError(t, err)
		require.Contains(
			t,
			cfg.RuntimeParams["options"],
			"-csearch_path=conformance_test",
		)
	})
}

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

package sqlstore

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenDBWrapsDriverOnlyWhenTracingEnabled pins OpenDB's contract: the
// otelsql wrapper is what costs an allocation on every query, so "tracing is
// disabled" has to mean the driver is not wrapped at all, not that a wrapped
// driver reports to a no-op TracerProvider. Comparing against the pool
// database/sql itself opens for the same driver name keeps the assertion on
// the contract rather than on an otelsql-internal type name.
func TestOpenDBWrapsDriverOnlyWhenTracingEnabled(t *testing.T) {
	t.Parallel()
	dsn := func() string {
		return fmt.Sprintf(
			"file:open_wrap_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		)
	}

	plain, err := sql.Open("sqlite", dsn())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, plain.Close()) })
	unwrapped := reflect.TypeOf(plain.Driver())

	untraced, err := OpenDB("sqlite", dsn(), "sqlite", false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, untraced.Close()) })
	require.Equal(
		t,
		unwrapped,
		reflect.TypeOf(untraced.Driver()),
		"tracing disabled must open the driver without an otelsql wrapper",
	)

	traced, err := OpenDB("sqlite", dsn(), "sqlite", true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, traced.Close()) })
	require.NotEqual(
		t,
		unwrapped,
		reflect.TypeOf(traced.Driver()),
		"tracing enabled must keep the otelsql instrumentation",
	)

	// Both pools must still answer the same query identically; skipping the
	// wrapper changes instrumentation, never results.
	for name, db := range map[string]*sql.DB{
		"untraced": untraced,
		"traced":   traced,
	} {
		var got int
		require.NoError(
			t,
			db.QueryRowContext(t.Context(), "SELECT 1").Scan(&got),
			name,
		)
		require.Equal(t, 1, got, name)
	}
}

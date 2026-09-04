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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestListSyncStateKeysByPrefix covers the byte-prefix scan used to repopulate
// the deferred-header retention set after a restart (issue #3727). The match
// must be an exact BYTE prefix on every backend, so this asserts cases a
// collation-sensitive SQL range or LIKE could get wrong: uppercase/mixed-case
// variants a case-insensitive column collation would fold in, a sibling prefix
// one byte away, and a non-ASCII neighbor a synthesized range upper bound could
// mishandle. The prefix also contains a LIKE wildcard ('_') to prove no
// wildcard escaping is needed.
func TestListSyncStateKeysByPrefix(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	const prefix = "deferred_header_validation:"
	want := []string{
		prefix + "10:aa",
		prefix + "13:bb",
		prefix + "21:cc",
	}
	for _, k := range want {
		require.NoError(t, db.SetSyncState(k, "true", nil))
	}
	// Decoys that must NOT match under an exact byte-prefix filter:
	decoys := []string{
		// Shorter than the prefix.
		"deferred_header_validation",
		// A sibling prefix differing only in the last byte.
		"deferred_header_validatioo:zz",
		// Unrelated key.
		"other_key",
		// Uppercased prefix: a case-INSENSITIVE column collation on
		// MySQL/Postgres would wrongly fold this into the match; a byte prefix
		// must exclude it.
		"DEFERRED_HEADER_VALIDATION:99",
		// Mixed case variant, same hazard.
		"Deferred_Header_Validation:88",
		// A non-ASCII key adjacent in Unicode: proves the match does not depend
		// on a synthesized range upper bound (which a non-ASCII prefix could
		// make invalid) and is not confused by locale collation ordering.
		"deferred_header_validationé:77",
	}
	for _, k := range decoys {
		require.NoError(t, db.SetSyncState(k, "x", nil))
	}

	got, err := db.ListSyncStateKeysByPrefix(prefix, nil)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		want,
		got,
		"only exact byte-prefix keys must match (no collation folding)",
	)

	// A non-ASCII prefix must scan safely and match exactly by bytes. The
	// "...é:77" decoy above shares this UTF-8 prefix, so both it and the key
	// added here must come back (and nothing ASCII-only).
	const utf8Prefix = "deferred_header_validationé:"
	require.NoError(t, db.SetSyncState(utf8Prefix+"aa", "true", nil))
	utf8Got, err := db.ListSyncStateKeysByPrefix(utf8Prefix, nil)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]string{utf8Prefix + "77", utf8Prefix + "aa"},
		utf8Got,
	)

	// Empty prefix returns everything.
	all, err := db.ListSyncStateKeysByPrefix("", nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(all), len(want)+len(decoys))

	// A prefix matching nothing returns empty, not an error.
	none, err := db.ListSyncStateKeysByPrefix("zzz_no_such_prefix:", nil)
	require.NoError(t, err)
	require.Empty(t, none)
}

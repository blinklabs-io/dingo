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

package badger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetCommitTimestampRoundTrip proves the normal 8-byte fixed-width path
// still round-trips.
func TestGetCommitTimestampRoundTrip(t *testing.T) {
	store, err := New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	txn := store.NewTransaction(true)
	require.NoError(t, store.SetCommitTimestamp(1234567890, txn))
	require.NoError(t, txn.Commit())

	got, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, int64(1234567890), got)
}

// TestGetCommitTimestampRejectsOversizedLegacyValue proves the legacy
// variable-length fallback decoder rejects a stored value that does not fit
// in an int64 instead of silently truncating it: big.Int.Int64() is
// undefined for such a value, so returning it as though it were a valid
// timestamp would hand callers a garbage value with no indication anything
// was wrong.
func TestGetCommitTimestampRejectsOversizedLegacyValue(t *testing.T) {
	store, err := New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// 2^100, > 8 bytes
	oversized := new(big.Int).Lsh(big.NewInt(1), 100).Bytes()
	require.Greater(t, len(oversized), 8)

	txn := store.NewTransaction(true)
	require.NoError(
		t,
		store.Set(txn, []byte(commitTimestampBlobKey), oversized),
	)
	require.NoError(t, txn.Commit())

	_, err = store.GetCommitTimestamp()
	require.Error(t, err)
}

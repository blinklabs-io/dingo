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
	"encoding/binary"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// writeLegacyLeiosEB writes a manifest (and, if txsRaw is non-nil, a
// transaction list) directly under the pre-issue-#3513 hash-only blob keys,
// bypassing SetLeiosEB entirely -- simulating data persisted by a node
// running before the key format changed to (slot, hash).
func writeLegacyLeiosEB(
	t *testing.T,
	d *Database,
	slot uint64,
	hash []byte,
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) {
	t.Helper()
	blob := d.Blob()
	require.NotNil(t, blob)
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blobTxn := txn.Blob()
	require.NotNil(t, blobTxn)

	manifestVal := make([]byte, 8+len(manifestRaw))
	binary.BigEndian.PutUint64(manifestVal[:8], slot)
	copy(manifestVal[8:], manifestRaw)
	require.NoError(
		t,
		blob.Set(blobTxn, types.LegacyLeiosEBManifestKey(hash), manifestVal),
	)
	if txsRaw != nil {
		txsVal, err := cbor.Encode(txsRaw)
		require.NoError(t, err)
		require.NoError(
			t,
			blob.Set(blobTxn, types.LegacyLeiosEBTxsKey(hash), txsVal),
		)
	}
	require.NoError(t, txn.Commit())
}

// TestGetLeiosEBManifestFallsBackToLegacyKey is the cubic regression: a node
// upgrading from before the blob key format changed to (slot, hash) must
// still be able to read manifests it persisted under the old hash-only key,
// rather than that data becoming silently unreachable.
func TestGetLeiosEBManifestFallsBackToLegacyKey(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("0123456789abcdef0123456789abcdef")[:32]
	slot := uint64(12345)
	manifestRaw := []byte("legacy manifest bytes")

	writeLegacyLeiosEB(t, d, slot, hash, manifestRaw, nil)

	got, err := d.GetLeiosEBManifest(hash, slot)
	require.NoError(t, err)
	require.Equal(t, manifestRaw, got)

	// A request for a different slot must not match the legacy record: its
	// embedded slot is validated, not just its presence under the hash.
	_, err = d.GetLeiosEBManifest(hash, slot+1)
	require.Error(t, err)
}

// TestGetLeiosEBTxsFallsBackToLegacyKey is the transaction-body half of the
// same regression: legacy "et" data (paired 1:1 with a legacy "em" record,
// since the old format could only ever track one occurrence per hash) must
// still be readable after upgrading, gated on the legacy manifest's own
// embedded slot matching the request.
func TestGetLeiosEBTxsFallsBackToLegacyKey(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("fedcba9876543210fedcba9876543210")[:32]
	slot := uint64(54321)
	manifestRaw := []byte("legacy manifest bytes 2")
	txsRaw := []cbor.RawMessage{mustCborForLeiosTest(t, "tx0")}

	writeLegacyLeiosEB(t, d, slot, hash, manifestRaw, txsRaw)

	got, err := d.GetLeiosEBTxs(hash, slot)
	require.NoError(t, err)
	require.Equal(t, txsRaw, got)

	// A different slot must not pick up the legacy txs either.
	_, err = d.GetLeiosEBTxs(hash, slot+1)
	require.Error(t, err)
}

// TestGetLeiosEBManifestPropagatesRealLegacyReadError is the cubic
// regression: a genuine failure reading the legacy fallback key (storage,
// network, auth) must surface as-is, not collapse into the exact-key's
// ordinary ErrBlobKeyNotFound and look like the manifest was simply never
// persisted.
func TestGetLeiosEBManifestPropagatesRealLegacyReadError(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("0123456789abcdef0123456789abcdef")[:32]
	slot := uint64(111)
	readErr := errors.New("legacy read: storage unavailable")

	d.SetBlobStore(&mockBlobStore{
		getErrs: map[string]error{
			string(types.LegacyLeiosEBManifestKey(hash)): readErr,
		},
	})

	_, err := d.GetLeiosEBManifest(hash, slot)
	require.ErrorIs(t, err, readErr)
	require.NotErrorIs(t, err, types.ErrBlobKeyNotFound)
}

// TestGetLeiosEBTxsPropagatesRealLegacyReadError is the transaction-body
// half of the same regression.
func TestGetLeiosEBTxsPropagatesRealLegacyReadError(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	hash := []byte("fedcba9876543210fedcba9876543210")[:32]
	slot := uint64(222)
	readErr := errors.New("legacy read: storage unavailable")

	d.SetBlobStore(&mockBlobStore{
		getErrs: map[string]error{
			string(types.LegacyLeiosEBManifestKey(hash)): readErr,
		},
	})

	_, err := d.GetLeiosEBTxs(hash, slot)
	require.ErrorIs(t, err, readErr)
	require.NotErrorIs(t, err, types.ErrBlobKeyNotFound)
}

func mustCborForLeiosTest(t *testing.T, value any) cbor.RawMessage {
	t.Helper()
	data, err := cbor.Encode(value)
	require.NoError(t, err)
	return cbor.RawMessage(data)
}

// TestMaxLeiosEBSlotReadsCurrentAndLegacyRecords covers both branches of the
// prefix scan, in particular the legacy one: SetLeiosEB only ever writes
// current-format "em"+hash+slot keys, so the branch that reads the slot out
// of the first eight value bytes -- the path that exists solely for nodes
// upgrading across the issue #3513 key change -- was otherwise unexercised
// (chrisguiney review).
func TestMaxLeiosEBSlotReadsCurrentAndLegacyRecords(t *testing.T) {
	t.Parallel()

	d := newTestDB(t)
	manifestRaw := []byte("manifest bytes")

	// No manifests persisted at all: no evidence, not an error.
	got, err := d.MaxLeiosEBSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)

	// Current format: the slot is the last eight bytes of the key.
	require.NoError(
		t,
		d.SetLeiosEBManifest(700, randomHash(t), manifestRaw),
	)
	got, err = d.MaxLeiosEBSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(700), got)

	// Legacy format: the key carries no slot, so the maximum can only be
	// found by reading it out of the value. Dropping the legacy branch of
	// the switch leaves this at 700.
	writeLegacyLeiosEB(t, d, 900, randomHash(t), manifestRaw, nil)
	got, err = d.MaxLeiosEBSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(900), got)

	// The scan takes a maximum, not a last-seen: neither format may lower
	// an already-higher slot, whichever order the keys sort in.
	writeLegacyLeiosEB(t, d, 100, randomHash(t), manifestRaw, nil)
	require.NoError(
		t,
		d.SetLeiosEBManifest(200, randomHash(t), manifestRaw),
	)
	got, err = d.MaxLeiosEBSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(900), got)
}

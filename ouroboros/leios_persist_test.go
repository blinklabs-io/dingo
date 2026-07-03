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

package ouroboros

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// TestLeiosPersistAsyncCoalescesManifestThenComplete mirrors the backfiller's
// two-call pattern for one endorser block — a manifest-only store followed by a
// complete (manifest + txs) store — and verifies that after the async writer
// drains, the blob store holds the COMPLETE endorser block (manifest + all
// txs), i.e. the later complete write is not lost and the redundant manifest
// write is harmless. This exercises the asynchronous persistence path and the
// merged single-commit SetLeiosEB writer.
func TestLeiosPersistAsyncCoalescesManifestThenComplete(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)
	txsRaw := []cbor.RawMessage{
		mustCbor(t, "tx0"),
		mustCbor(t, "tx1"),
	}

	o := newTestOuroborosWithLeiosDB(t)

	// First the manifest only (no txs yet), as the backfiller's manifest fetch
	// does; then the complete block once its txs are fetched.
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, txsRaw))

	// Drain the async writer so all queued persistence has committed.
	o.StopLeiosPersistWriter()

	db := o.leiosDatabase()
	require.NotNil(t, db)

	slot, manifest, err := db.GetLeiosEBManifest(point.Hash)
	require.NoError(t, err)
	require.Equal(t, point.Slot, slot)
	require.Equal(t, []byte(blockRaw), manifest)

	gotTxs, err := db.GetLeiosEBTxs(point.Hash)
	require.NoError(t, err)
	require.Equal(t, txsRaw, gotTxs)
}

// TestLeiosPersistWriterStopIsSafeWithoutStart verifies StopLeiosPersistWriter
// is a no-op when no endorser block was ever fetched (the writer never started)
// and is safe to call more than once.
func TestLeiosPersistWriterStopIsSafeWithoutStart(t *testing.T) {
	o := newTestOuroborosWithLeiosDB(t)
	require.NotPanics(t, func() {
		o.StopLeiosPersistWriter()
		o.StopLeiosPersistWriter()
	})
}

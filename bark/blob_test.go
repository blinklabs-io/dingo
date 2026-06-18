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

package bark

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeArchive serves FetchBlock responses pointing at downloadURL, where
// downloadURL replies with the configured CBOR bytes per (slot, hash).
type fakeArchive struct {
	t           *testing.T
	downloadURL string
	blocks      map[string][]byte // hex(hash) -> CBOR bytes
	prevHash    []byte
	height      uint64
	blockType   archive.BlockType

	fetchCalls int
}

func (a *fakeArchive) FetchBlock(
	_ context.Context,
	req *connect.Request[archive.FetchBlockRequest],
) (*connect.Response[archive.FetchBlockResponse], error) {
	a.fetchCalls++
	resp := &archive.FetchBlockResponse{}
	for _, b := range req.Msg.GetBlocks() {
		hashHex := b.GetHash()
		if _, ok := a.blocks[hashHex]; !ok {
			a.t.Fatalf("fakeArchive: unexpected block requested: %s", hashHex)
		}
		resp.Blocks = append(resp.Blocks, &archive.SignedUrl{
			Block: &archive.BlockRef{
				Hash:   b.Hash,
				Slot:   b.Slot,
				Height: new(a.height),
			},
			Url: a.downloadURL + "?hash=" + hashHex,
			Meta: &archive.BlockMeta{
				Type:     a.blockType.Enum(),
				PrevHash: new(hex.EncodeToString(a.prevHash)),
			},
		})
	}
	return connect.NewResponse(resp), nil
}

// startFakeArchive boots an httptest server that serves both the bark
// archive connect handler and a /download endpoint returning raw CBOR
// keyed by ?hash=<hex>. Returns the base URL (for the connect client)
// and a pointer to the handler so tests can inspect call counts.
func startFakeArchive(
	t *testing.T,
	blocks map[string][]byte,
) (string, *fakeArchive) {
	t.Helper()
	mux := http.NewServeMux()
	a := &fakeArchive{
		t:         t,
		blocks:    blocks,
		prevHash:  bytes.Repeat([]byte{0x11}, 32),
		height:    42,
		blockType: archive.BlockType_BLOCK_TYPE_CONWAY,
	}
	mux.HandleFunc(
		"/download",
		func(w http.ResponseWriter, r *http.Request) {
			hashHex := r.URL.Query().Get("hash")
			cbor, ok := a.blocks[hashHex]
			if !ok {
				http.Error(w, "unknown hash", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(cbor)
		},
	)

	archivePath, archiveHandler := archiveconnect.NewArchiveServiceHandler(a)
	mux.Handle(archivePath, archiveHandler)

	srv := httptest.NewUnstartedServer(mux)
	srv.Config.Protocols = unencryptedHTTP2Protocols()
	srv.Start()
	t.Cleanup(srv.Close)

	a.downloadURL = srv.URL + "/download"
	return srv.URL, a
}

// newTestDB builds an in-memory dingo database for use as the upstream
// blob source. The default plugins (badger blob, sqlite metadata) are
// adequate for these iterator tests, and going through database.New
// avoids reaching into a specific blob plugin.
func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err, "failed to create test database")
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

// newBarkBlobStoreForTest builds a BlobStoreBark wrapping db's blob store
// and pointed at baseURL. Direct struct construction lets the tests inject
// a fake archive client while focusing on the iterator path.
func newBarkBlobStoreForTest(
	db *database.Database, baseURL string,
) *BlobStoreBark {
	httpClient := http.DefaultClient
	return &BlobStoreBark{
		archiveClient: archiveconnect.NewArchiveServiceClient(
			httpClient, baseURL,
		),
		httpClient: httpClient,
		upstream:   db.Blob(),
	}
}

// TestBarkIterator_ResolvesExpiredHistoryViaArchive seeds the database with a
// block, marks it expired locally, then iterates through the bark wrapper.
// ValueCopy must surface the archive's CBOR, not the local expiry marker.
func TestBarkIterator_ResolvesExpiredHistoryViaArchive(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 100
	hash := bytes.Repeat([]byte{0xAB}, 32)
	cbor := []byte("real-block-cbor-from-archive")

	require.NoError(t, db.BlockCreate(models.Block{
		Slot: slot,
		Hash: hash,
		Cbor: []byte("local-cbor"),
		Type: 1,
	}, nil))

	wTxn := db.BlobTxn(true)
	require.NoError(t, wTxn.Do(func(txn *database.Txn) error {
		return db.Blob().TombstoneBlock(txn.Blob(), slot, hash)
	}))

	baseURL, archiveSrv := startFakeArchive(t, map[string][]byte{
		hex.EncodeToString(hash): cbor,
	})
	store := newBarkBlobStoreForTest(db, baseURL)

	rTxn := store.NewTransaction(false)
	t.Cleanup(func() { _ = rTxn.Rollback() })

	it := store.NewIterator(rTxn, types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	})
	require.NotNil(t, it)
	t.Cleanup(it.Close)

	var (
		seenBp   bool
		gotValue []byte
	)
	for it.Seek([]byte(types.BlockBlobKeyPrefix)); it.ValidForPrefix(
		[]byte(types.BlockBlobKeyPrefix),
	); it.Next() {
		item := it.Item()
		require.NotNil(t, item)
		k := item.Key()
		// Skip the metadata key — TombstoneBlock removes it, but a fresh
		// fixture might still see it before commit; either way, expiry
		// markers never live there so it's not interesting for this assertion.
		if bytes.HasSuffix(k, []byte(types.BlockBlobMetadataKeySuffix)) {
			continue
		}
		require.False(t, seenBp,
			"expected exactly one bp key in this fixture")
		seenBp = true
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		gotValue = v
	}
	require.NoError(t, it.Err())
	require.True(t, seenBp, "iterator did not visit the bp key")

	assert.False(t, types.IsBlockTombstone(gotValue),
		"ValueCopy must not surface the raw expiry marker — the wrapper "+
			"is supposed to resolve it via the archive")
	assert.Equal(t, cbor, gotValue,
		"ValueCopy must return the archive-served CBOR for expired history")
	assert.Equal(t, 1, archiveSrv.fetchCalls,
		"exactly one archive FetchBlock call expected for one expired block")
}

// TestUpstreamIterator_SurfacesTypedHistoryExpiredError proves the contract
// bark relies on: the underlying plugin iterator returns a typed
// *types.HistoryExpiredError (carrying slot+hash) from ValueCopy when
// it encounters an expiry marker, so the bark wrapper can resolve via
// errors.As without parsing any blob keys.
func TestUpstreamIterator_SurfacesTypedHistoryExpiredError(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 300
	hash := bytes.Repeat([]byte{0xEF}, 32)

	require.NoError(t, db.BlockCreate(models.Block{
		Slot: slot,
		Hash: hash,
		Cbor: []byte("orig-cbor"),
		Type: 1,
	}, nil))

	wTxn := db.BlobTxn(true)
	require.NoError(t, wTxn.Do(func(txn *database.Txn) error {
		return db.Blob().TombstoneBlock(txn.Blob(), slot, hash)
	}))

	upstream := db.Blob()
	rTxn := upstream.NewTransaction(false)
	t.Cleanup(func() { _ = rTxn.Rollback() })
	it := upstream.NewIterator(rTxn, types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	})
	t.Cleanup(it.Close)

	var typedErrSeen bool
	for it.Seek([]byte(types.BlockBlobKeyPrefix)); it.ValidForPrefix(
		[]byte(types.BlockBlobKeyPrefix),
	); it.Next() {
		item := it.Item()
		require.NotNil(t, item)
		k := item.Key()
		if bytes.HasSuffix(k, []byte(types.BlockBlobMetadataKeySuffix)) {
			continue
		}
		_, err := item.ValueCopy(nil)
		require.Error(t, err,
			"upstream ValueCopy on expired history must surface an error")
		var historyErr *types.HistoryExpiredError
		require.True(t, errors.As(err, &historyErr),
			"upstream error must be (or wrap) *HistoryExpiredError so "+
				"bark can extract slot/hash with errors.As")
		assert.Equal(t, slot, historyErr.Slot)
		assert.Equal(t, hash, historyErr.Hash)
		require.True(t, errors.Is(err, types.ErrHistoryExpired),
			"typed error must still satisfy errors.Is(ErrHistoryExpired)")
		typedErrSeen = true
	}
	require.True(t, typedErrSeen,
		"iterator did not surface the expiry marker we just wrote")
}

// TestBarkIterator_PassesThroughLiveValues checks that values at non-bp
// keys (here: bi index pointers) and at non-expired bp keys go
// straight through without any archive call.
func TestBarkIterator_PassesThroughLiveValues(t *testing.T) {
	db := newTestDB(t)

	const slot uint64 = 200
	hash := bytes.Repeat([]byte{0xCD}, 32)
	cbor := []byte("live-block-cbor")

	require.NoError(t, db.BlockCreate(models.Block{
		Slot: slot,
		Hash: hash,
		Cbor: cbor,
		Type: 1,
	}, nil))

	// Empty block map — any archive call would fatal in the fake handler.
	baseURL, archiveSrv := startFakeArchive(t, map[string][]byte{})
	store := newBarkBlobStoreForTest(db, baseURL)

	rTxn := store.NewTransaction(false)
	t.Cleanup(func() { _ = rTxn.Rollback() })

	// Iterate bp prefix: the live block's CBOR must come through verbatim.
	it := store.NewIterator(rTxn, types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	})
	t.Cleanup(it.Close)
	var found bool
	for it.Seek([]byte(types.BlockBlobKeyPrefix)); it.ValidForPrefix(
		[]byte(types.BlockBlobKeyPrefix),
	); it.Next() {
		item := it.Item()
		require.NotNil(t, item)
		k := item.Key()
		if bytes.HasSuffix(k, []byte(types.BlockBlobMetadataKeySuffix)) {
			continue
		}
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		assert.Equal(t, cbor, v)
		found = true
	}
	require.NoError(t, it.Err())
	require.True(t, found)

	// Iterate bi prefix: the value is the bp key reference (not a CBOR
	// payload); it must pass through unchanged regardless of expiry markers
	// elsewhere.
	itBi := store.NewIterator(rTxn, types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobIndexKeyPrefix),
	})
	t.Cleanup(itBi.Close)
	var biSeen bool
	for itBi.Seek(
		[]byte(types.BlockBlobIndexKeyPrefix),
	); itBi.ValidForPrefix(
		[]byte(types.BlockBlobIndexKeyPrefix),
	); itBi.Next() {
		item := itBi.Item()
		require.NotNil(t, item)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		assert.Equal(t, types.BlockBlobKey(slot, hash), v)
		biSeen = true
	}
	require.NoError(t, itBi.Err())
	require.True(t, biSeen, "iterator did not visit the bi key")

	assert.Zero(t, archiveSrv.fetchCalls,
		"no archive call must occur when no tombstones are present")
}

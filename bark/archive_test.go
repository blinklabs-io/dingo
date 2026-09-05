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
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// signedURLBlobProviderName registers the badger blob store behind a
// deterministic URL signer. Badger itself returns "GetBlockURL not
// supported", so without this wrapper no in-process test can reach the
// archive handler's found path at all — only s3 and gcs sign URLs, and
// both need live cloud credentials.
const signedURLBlobProviderName = "signedbadger"

// signedURLBlobTestExpiry is the fixed expiry the fake signer stamps on
// every URL so a test can assert the value the handler propagated.
var signedURLBlobTestExpiry = time.Date(
	2030, time.January, 2, 3, 4, 5, 0, time.UTC,
)

// signedURLBlobStore wraps a local blob store with a signer that mints a
// deterministic URL for any block the store actually holds, and reports a
// missing block with the same types.ErrBlobKeyNotFound the cloud plugins
// use, so the handler's not-found classification is exercised for real.
type signedURLBlobStore struct {
	blob.BlobStore
	// unsignable holds hex block hashes whose metadata object is treated
	// as lost: GetBlockURL reports types.ErrBlobKeyNotFound while the block
	// itself stays readable through GetBlock. That is the partial-write
	// shape both cloud plugins collapse into the same error they use for a
	// block that was never written (gcs GetBlockURL logs the difference and
	// returns ErrBlobKeyNotFound regardless; s3 GetBlockURL reads metadata
	// before it heads the block object at all), so it cannot be produced by
	// deleting anything -- it has to be injected here.
	unsignable map[string]struct{}
}

func (s *signedURLBlobStore) GetBlockURL(
	_ context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	if _, ok := s.unsignable[hex.EncodeToString(point.Hash)]; ok {
		return types.SignedURL{}, types.BlockMetadata{}, fmt.Errorf(
			"metadata object for block [%d, %s] is missing: %w",
			point.Slot,
			hex.EncodeToString(point.Hash),
			types.ErrBlobKeyNotFound,
		)
	}
	_, metadata, err := s.BlobStore.GetBlock(txn, point.Slot, point.Hash)
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{}, err
	}
	return types.SignedURL{
		URL: url.URL{
			Scheme: "https",
			Host:   "archive.example.com",
			Path: fmt.Sprintf(
				"/%d/%s", point.Slot, hex.EncodeToString(point.Hash),
			),
		},
		Expires: signedURLBlobTestExpiry,
	}, metadata, nil
}

type signedURLBlobConfig struct{}

// registerSignedURLBlobProvider registers the store, treating the given hex
// block hashes as unsignable. dbtest builds a fresh plugin.Host per
// database, so each test gets its own registration and the shared provider
// name does not collide.
func registerSignedURLBlobProvider(
	host *hostplugin.Host,
	unsignable map[string]struct{},
) error {
	return hostplugin.Register(
		host,
		hostplugin.Descriptor{
			Capability:  hostplugin.CapabilityStorageBlob,
			Name:        signedURLBlobProviderName,
			Description: "Badger with deterministic signed block URLs",
		},
		func() signedURLBlobConfig { return signedURLBlobConfig{} },
		func(
			_ context.Context,
			_ signedURLBlobConfig,
			deps blob.ProviderDependencies,
		) (*signedURLBlobStore, hostplugin.Instance, error) {
			store, err := badger.New(
				badger.WithDataDir(deps.DataDir),
				badger.WithLogger(deps.Logger),
				badger.WithDeferOpen(),
			)
			if err != nil {
				return nil, nil, err
			}
			signing := &signedURLBlobStore{
				BlobStore:  store,
				unsignable: unsignable,
			}
			return signing,
				hostplugin.Lifecycle{
					StartFunc: func(context.Context) error {
						return store.Start()
					},
					StopFunc: store.CloseContext,
				}, nil
		},
	)
}

// newSigningTestDB builds a test database whose blob store can sign block
// URLs, which the archive handler needs to produce any SignedUrl at all.
// Hashes in unsignable are the exception: the store holds those blocks but
// refuses to sign them, so GetBlockURL reports them missing.
func newSigningTestDB(
	t *testing.T,
	unsignable map[string]struct{},
) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: t.TempDir()},
		Blob: dbtest.StorageProvider{
			Name: signedURLBlobProviderName,
			Register: func(host *hostplugin.Host) error {
				return registerSignedURLBlobProvider(host, unsignable)
			},
		},
	})
	require.NoError(t, err)
	return db
}

// newArchiveTestHandler builds an archive handler over a signing database
// seeded with count blocks, and returns both the handler and the blocks.
func newArchiveTestHandler(
	t *testing.T,
	count int,
) (*archiveServiceHandler, []models.Block) {
	t.Helper()
	return newArchiveTestHandlerWithLogger(t, count, nil, nil)
}

// newArchiveTestHandlerWithLogger builds an archive handler over a signing
// database seeded with count blocks, refusing to sign the hex block hashes
// in unsignable and logging through logger. A nil logger leaves NewBark's
// discard logger in place.
func newArchiveTestHandlerWithLogger(
	t *testing.T,
	count int,
	unsignable map[string]struct{},
	logger *slog.Logger,
) (*archiveServiceHandler, []models.Block) {
	t.Helper()
	db := newSigningTestDB(t, unsignable)
	blocks := make([]models.Block, 0, count)
	for i := 1; i <= count; i++ {
		// #nosec G115 -- fixed small test fixture values.
		block := testBlock(uint64(i), byte(0x10+i))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	b, err := NewBark(BarkConfig{DB: db, Port: 1, Logger: logger})
	require.NoError(t, err)
	return &archiveServiceHandler{bark: b}, blocks
}

func fetchBlocks(
	t *testing.T,
	handler *archiveServiceHandler,
	refs ...*archive.BlockRef,
) *archive.FetchBlockResponse {
	t.Helper()
	resp, err := handler.FetchBlock(
		t.Context(),
		connect.NewRequest(&archive.FetchBlockRequest{Blocks: refs}),
	)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp.Msg
}

// TestArchiveFetchBlockResolvesSingleReference pins the fully-specified
// single-reference case: one SignedUrl, no not_found, and the requested
// identifier fields echoed back verbatim rather than recomputed — the
// upper-case hash proves the handler returns what was asked for.
func TestArchiveFetchBlockResolvesSingleReference(t *testing.T) {
	handler, blocks := newArchiveTestHandler(t, 3)
	block := blocks[1]
	requestedHash := strings.ToUpper(hex.EncodeToString(block.Hash))

	msg := fetchBlocks(t, handler, &archive.BlockRef{
		Hash:   new(requestedHash),
		Slot:   new(block.Slot),
		Height: new(block.Number),
	})

	require.Empty(t, msg.GetNotFound())
	require.Len(t, msg.GetBlocks(), 1)
	got := msg.GetBlocks()[0]
	require.Equal(t, requestedHash, got.GetBlock().GetHash())
	require.Equal(t, block.Slot, got.GetBlock().GetSlot())
	require.Equal(t, block.Number, got.GetBlock().GetHeight())
	require.Equal(
		t,
		fmt.Sprintf(
			"https://archive.example.com/%d/%s",
			block.Slot,
			hex.EncodeToString(block.Hash),
		),
		got.GetUrl(),
	)
	require.True(
		t,
		signedURLBlobTestExpiry.Equal(got.GetExpiresAt().AsTime()),
		"expected expiry %s, got %s",
		signedURLBlobTestExpiry,
		got.GetExpiresAt().AsTime(),
	)
	require.Equal(
		t,
		archive.BlockType_BLOCK_TYPE_BYRON_MAIN,
		got.GetMeta().GetType(),
	)
}

// TestArchiveFetchBlockResolvesIdentifierOnlyReferences covers each
// identifier the proto allows on its own. The response fills in the
// identifiers the client did not supply so a hash-only or height-only
// caller still learns the block's full identity.
func TestArchiveFetchBlockResolvesIdentifierOnlyReferences(t *testing.T) {
	handler, blocks := newArchiveTestHandler(t, 3)
	block := blocks[2]
	hash := hex.EncodeToString(block.Hash)

	for _, testCase := range []struct {
		name string
		ref  *archive.BlockRef
	}{
		{name: "hash only", ref: &archive.BlockRef{Hash: new(hash)}},
		{name: "slot only", ref: &archive.BlockRef{Slot: new(block.Slot)}},
		{
			name: "height only",
			ref:  &archive.BlockRef{Height: new(block.Number)},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			msg := fetchBlocks(t, handler, testCase.ref)

			require.Empty(t, msg.GetNotFound())
			require.Len(t, msg.GetBlocks(), 1)
			got := msg.GetBlocks()[0].GetBlock()
			require.Equal(t, hash, got.GetHash())
			require.Equal(t, block.Slot, got.GetSlot())
			require.Equal(t, block.Number, got.GetHeight())
		})
	}
}

// TestArchiveFetchBlockReturnsNotFoundWithoutDiscardingBatch proves a
// missing reference no longer aborts the whole request: the blocks that do
// exist still come back, and each missing reference is echoed under
// not_found carrying only the fields the client supplied.
func TestArchiveFetchBlockReturnsNotFoundWithoutDiscardingBatch(
	t *testing.T,
) {
	handler, blocks := newArchiveTestHandler(t, 3)
	firstHash := hex.EncodeToString(blocks[0].Hash)
	lastHash := hex.EncodeToString(blocks[2].Hash)
	missingHash := strings.Repeat("ab", 32)
	missingHeight := uint64(9999)

	msg := fetchBlocks(
		t,
		handler,
		&archive.BlockRef{Hash: new(firstHash), Slot: new(blocks[0].Slot)},
		&archive.BlockRef{Hash: new(missingHash)},
		&archive.BlockRef{Hash: new(lastHash), Slot: new(blocks[2].Slot)},
		&archive.BlockRef{Height: new(missingHeight)},
	)

	require.Len(t, msg.GetBlocks(), 2)
	require.Equal(t, firstHash, msg.GetBlocks()[0].GetBlock().GetHash())
	require.Equal(t, lastHash, msg.GetBlocks()[1].GetBlock().GetHash())

	require.Len(t, msg.GetNotFound(), 2)
	require.Equal(t, missingHash, msg.GetNotFound()[0].GetHash())
	require.Nil(t, msg.GetNotFound()[0].Slot)
	require.Nil(t, msg.GetNotFound()[0].Height)
	require.Equal(t, missingHeight, msg.GetNotFound()[1].GetHeight())
	require.Nil(t, msg.GetNotFound()[1].Hash)
}

// TestArchiveFetchBlockLogsUnservableResolvedBlock covers the one shape a
// not_found answer can hide: the block is in the archive's index, and the
// blob store still reports it missing when asked for a URL. Both cloud
// plugins return types.ErrBlobKeyNotFound for a lost metadata object and
// for a block that was never written, so the error cannot distinguish them
// — a lookup having already read the block is what does.
//
// The client answer stays not_found, since there is no URL to give it and
// failing the call would discard the rest of the batch. The operator gets a
// log line naming the block. The hash+slot and unknown-hash cases below pin
// the other half of that contract: neither resolves through a lookup, so
// neither is evidence of anything and neither may log.
func TestArchiveFetchBlockLogsUnservableResolvedBlock(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(
		&logs,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	// testBlock is deterministic, so the second seeded block's hash is
	// known before the store that has to refuse to sign it is built.
	lostHash := hex.EncodeToString(testBlock(2, 0x12).Hash)
	handler, blocks := newArchiveTestHandlerWithLogger(
		t,
		3,
		map[string]struct{}{lostHash: {}},
		logger,
	)
	lost, served := blocks[1], blocks[0]
	require.Equal(t, lostHash, hex.EncodeToString(lost.Hash))
	servedHash := hex.EncodeToString(served.Hash)

	msg := fetchBlocks(
		t,
		handler,
		&archive.BlockRef{Hash: new(lostHash)},
		&archive.BlockRef{
			Hash: new(servedHash),
			Slot: new(served.Slot),
		},
	)

	require.Len(t, msg.GetBlocks(), 1, "rest of the batch is still served")
	require.Equal(t, servedHash, msg.GetBlocks()[0].GetBlock().GetHash())
	require.Len(t, msg.GetNotFound(), 1)
	require.Equal(t, lostHash, msg.GetNotFound()[0].GetHash())

	logged := logs.String()
	require.Contains(t, logged, "blob store reports it missing")
	require.Contains(t, logged, lostHash)
	require.Contains(t, logged, fmt.Sprintf("slot=%d", lost.Slot))

	// hash+slot resolves with no lookup, so a miss there is an ordinary
	// absent block as far as the handler can tell, and must not be logged
	// as an inconsistency.
	logs.Reset()
	msg = fetchBlocks(t, handler, &archive.BlockRef{
		Hash: new(lostHash),
		Slot: new(lost.Slot),
	})
	require.Len(t, msg.GetNotFound(), 1)
	require.Empty(t, msg.GetBlocks())
	require.Empty(t, logs.String())

	// A hash that names no stored block fails at resolution, before the
	// blob store is asked for anything.
	logs.Reset()
	msg = fetchBlocks(t, handler, &archive.BlockRef{
		Hash: new(strings.Repeat("ab", 32)),
	})
	require.Len(t, msg.GetNotFound(), 1)
	require.Empty(t, logs.String())
}

// TestArchiveFetchBlockTreatsInconsistentReferenceAsNotFound covers a
// well-formed reference whose identifiers name different blocks. No stored
// block satisfies all of them, so it is reported as not_found — a fork or
// a stale client index, not a malformed request — and the rest of the
// batch is unaffected.
func TestArchiveFetchBlockTreatsInconsistentReferenceAsNotFound(
	t *testing.T,
) {
	handler, blocks := newArchiveTestHandler(t, 3)
	first, second := blocks[0], blocks[1]
	firstHash := hex.EncodeToString(first.Hash)
	secondHash := hex.EncodeToString(second.Hash)

	for _, testCase := range []struct {
		name string
		ref  *archive.BlockRef
	}{
		{
			name: "hash and slot disagree",
			ref: &archive.BlockRef{
				Hash: new(firstHash),
				Slot: new(second.Slot),
			},
		},
		{
			name: "hash and height disagree",
			ref: &archive.BlockRef{
				Hash:   new(firstHash),
				Height: new(second.Number),
			},
		},
		{
			name: "slot and height disagree",
			ref: &archive.BlockRef{
				Slot:   new(first.Slot),
				Height: new(second.Number),
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			msg := fetchBlocks(
				t,
				handler,
				testCase.ref,
				&archive.BlockRef{
					Hash: new(secondHash),
					Slot: new(second.Slot),
				},
			)

			require.Len(t, msg.GetNotFound(), 1)
			require.Len(t, msg.GetBlocks(), 1)
			require.Equal(
				t, secondHash, msg.GetBlocks()[0].GetBlock().GetHash(),
			)
		})
	}
}

// TestArchiveFetchBlockRejectsMalformedReference separates a malformed
// reference from a missing one. A reference carrying no identifier, or a
// hash that is not a 32-byte hex string, is not a valid BlockRef at all,
// so the whole request fails with InvalidArgument instead of quietly
// landing in not_found.
func TestArchiveFetchBlockRejectsMalformedReference(t *testing.T) {
	handler, blocks := newArchiveTestHandler(t, 1)
	validHash := hex.EncodeToString(blocks[0].Hash)

	for _, testCase := range []struct {
		name string
		ref  *archive.BlockRef
	}{
		{name: "no identifier", ref: &archive.BlockRef{}},
		{
			name: "hash not hex",
			ref:  &archive.BlockRef{Hash: new(strings.Repeat("zz", 32))},
		},
		{
			name: "hash too short",
			ref:  &archive.BlockRef{Hash: new(strings.Repeat("ab", 16))},
		},
		{name: "empty hash", ref: &archive.BlockRef{Hash: new("")}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := handler.FetchBlock(
				t.Context(),
				connect.NewRequest(&archive.FetchBlockRequest{
					Blocks: []*archive.BlockRef{
						{
							Hash: new(validHash),
							Slot: new(blocks[0].Slot),
						},
						testCase.ref,
					},
				}),
			)
			require.Error(t, err)
			require.Equal(
				t, connect.CodeInvalidArgument, connect.CodeOf(err),
			)
		})
	}
}

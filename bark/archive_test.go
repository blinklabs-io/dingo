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
	"context"
	"encoding/hex"
	"fmt"
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
}

func (s *signedURLBlobStore) GetBlockURL(
	_ context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
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

func registerSignedURLBlobProvider(host *hostplugin.Host) error {
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
			return &signedURLBlobStore{BlobStore: store},
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
func newSigningTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: t.TempDir()},
		Blob: dbtest.StorageProvider{
			Name:     signedURLBlobProviderName,
			Register: registerSignedURLBlobProvider,
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
	db := newSigningTestDB(t)
	blocks := make([]models.Block, 0, count)
	for i := 1; i <= count; i++ {
		// #nosec G115 -- fixed small test fixture values.
		block := testBlock(uint64(i), byte(0x10+i))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	return &archiveServiceHandler{bark: newTestBark(t, db)}, blocks
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

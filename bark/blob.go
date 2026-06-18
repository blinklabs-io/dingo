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
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"connectrpc.com/connect"
	archivev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// archiveFetchTimeout bounds a single archive round-trip (signed-URL request
// plus the follow-up download). Used by both GetBlock and the iterator's
// per-item expired-history resolution.
const archiveFetchTimeout = 20 * time.Second

type BlobStoreBarkConfig struct {
	BaseUrl    string
	HTTPClient *http.Client
}

type BlobStoreBark struct {
	config        BlobStoreBarkConfig
	archiveClient archiveconnect.ArchiveServiceClient
	httpClient    *http.Client
	upstream      blob.BlobStore
}

func NewBarkBlobStore(
	config BlobStoreBarkConfig,
	upstream blob.BlobStore,
) (*BlobStoreBark, error) {
	if upstream == nil {
		return nil, errors.New("bark: upstream blob store is required")
	}

	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	return &BlobStoreBark{
		config: config,
		archiveClient: archiveconnect.NewArchiveServiceClient(
			httpClient,
			config.BaseUrl,
		),
		httpClient: httpClient,
		upstream:   upstream,
	}, nil
}

func (b *BlobStoreBark) Close() error {
	return b.upstream.Close()
}

func (b *BlobStoreBark) DiskSize() (int64, error) {
	return b.upstream.DiskSize()
}

func (b *BlobStoreBark) NewTransaction(b2 bool) types.Txn {
	return b.upstream.NewTransaction(b2)
}

func (b *BlobStoreBark) Get(txn types.Txn, key []byte) ([]byte, error) {
	return b.upstream.Get(txn, key)
}

func (b *BlobStoreBark) Set(txn types.Txn, key, val []byte) error {
	return b.upstream.Set(txn, key, val)
}

func (b *BlobStoreBark) Delete(txn types.Txn, key []byte) error {
	return b.upstream.Delete(txn, key)
}

func (b *BlobStoreBark) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	return &barkIterator{
		upstream: b.upstream.NewIterator(txn, opts),
		store:    b,
	}
}

// barkIterator wraps an upstream blob iterator so that values returned via
// Item().ValueCopy() transparently resolve expired history from the archive.
// Expiry markers only appear at "bp"+slot+hash keys; values at any other key
// (bi/bh, bp_metadata, …) pass through unchanged, so wrapping is zero-cost
// for non-block-CBOR iterations.
type barkIterator struct {
	upstream types.BlobIterator
	store    *BlobStoreBark
}

func (it *barkIterator) Rewind()                      { it.upstream.Rewind() }
func (it *barkIterator) Seek(prefix []byte)           { it.upstream.Seek(prefix) }
func (it *barkIterator) Valid() bool                  { return it.upstream.Valid() }
func (it *barkIterator) ValidForPrefix(p []byte) bool { return it.upstream.ValidForPrefix(p) }
func (it *barkIterator) Next()                        { it.upstream.Next() }
func (it *barkIterator) Close()                       { it.upstream.Close() }
func (it *barkIterator) Err() error                   { return it.upstream.Err() }

func (it *barkIterator) Item() types.BlobItem {
	upstreamItem := it.upstream.Item()
	if upstreamItem == nil {
		return nil
	}
	return &barkItem{upstream: upstreamItem, store: it.store}
}

// barkItem wraps an upstream blob item. Key() passes through. ValueCopy()
// catches the typed *types.HistoryExpiredError surfaced by the upstream
// plugin's iterator and resolves the block via the archive using the
// (slot, hash) carried by the error — keeping the wrapper transparent to
// callers without coupling it to any blob-key format.
type barkItem struct {
	upstream types.BlobItem
	store    *BlobStoreBark
}

func (i *barkItem) Key() []byte { return i.upstream.Key() }

func (i *barkItem) ValueCopy(dst []byte) ([]byte, error) {
	val, err := i.upstream.ValueCopy(dst)
	if err == nil {
		return val, nil
	}
	var historyErr *types.HistoryExpiredError
	if !errors.As(err, &historyErr) {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), archiveFetchTimeout,
	)
	defer cancel()
	cbor, _, fetchErr := i.store.fetchBlockFromArchive(
		ctx, historyErr.Slot, historyErr.Hash,
	)
	if fetchErr != nil {
		return nil, fmt.Errorf(
			"bark iterator: resolving expired history at slot=%d: %w",
			historyErr.Slot, fetchErr,
		)
	}
	return cbor, nil
}

func (b *BlobStoreBark) GetCommitTimestamp() (int64, error) {
	return b.upstream.GetCommitTimestamp()
}

func (b *BlobStoreBark) SetCommitTimestamp(i int64, txn types.Txn) error {
	return b.upstream.SetCommitTimestamp(i, txn)
}

func (b *BlobStoreBark) SetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	cbor []byte,
	id uint64,
	blockType uint,
	height uint64,
	prevHash []byte,
) error {
	return b.upstream.SetBlock(
		txn, slot, hash, cbor, id, blockType, height, prevHash)
}

func (b *BlobStoreBark) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	// Always consult the upstream first so we can pick up the local
	// BlockMetadata (most importantly the block ID, which the chain
	// iterator's BlockByIndex path depends on). Upstream reports
	// ErrHistoryExpired for locally expired blocks while still returning
	// the metadata it kept around for exactly this purpose. Fall through
	// to the archive on ErrBlobKeyNotFound too: blocks expired before the
	// marker-preserving fix (or never indexed locally, e.g. snapshot bootstrap)
	// have no bp entry, but the archive can still serve them.
	upstreamCbor, upstreamMeta, err := b.upstream.GetBlock(txn, slot, hash)
	if err == nil {
		return upstreamCbor, upstreamMeta, nil
	}
	if !errors.Is(err, types.ErrHistoryExpired) &&
		!errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil, types.BlockMetadata{}, err
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), archiveFetchTimeout,
	)
	defer cancel()
	archiveCbor, archiveMeta, archErr := b.fetchBlockFromArchive(ctx, slot, hash)
	if archErr != nil {
		return nil, types.BlockMetadata{}, archErr
	}
	// Prefer the local metadata for ID (the archive does not know our
	// local block IDs), but trust the archive for Type/Height/PrevHash
	// in case upstream returned a zero metadata struct alongside the
	// expired-history error.
	merged := upstreamMeta
	if merged.Type == 0 {
		merged.Type = archiveMeta.Type
	}
	if merged.Height == 0 {
		merged.Height = archiveMeta.Height
	}
	if len(merged.PrevHash) == 0 {
		merged.PrevHash = archiveMeta.PrevHash
	}
	return archiveCbor, merged, nil
}

// fetchBlockFromArchive resolves a (slot, hash) block via the bark archive
// service: requests a signed URL, downloads the CBOR, and returns it along
// with the metadata carried in the archive response.
func (b *BlobStoreBark) fetchBlockFromArchive(
	ctx context.Context,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	resp, err := b.archiveClient.FetchBlock(
		ctx,
		connect.NewRequest(
			&archivev1alpha1.FetchBlockRequest{
				Blocks: []*archivev1alpha1.BlockRef{
					{
						Slot: new(slot),
						Hash: new(hex.EncodeToString(hash)),
					},
				},
			},
		),
	)
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed getting signed url from bark archive service: %w", err)
	}

	blocks := resp.Msg.GetBlocks()
	if len(blocks) != 1 {
		return nil, types.BlockMetadata{},
			fmt.Errorf("expected 1 block, got %d", len(blocks))
	}

	block := blocks[0]

	blockReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		block.GetUrl(),
		nil,
	)
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed creating request for bark supplied url: %w", err)
	}
	blockResp, err := b.httpClient.Do(blockReq) //nolint:gosec
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed downloading block from bark supplied url: %w", err)
	}
	if blockResp == nil {
		return nil, types.BlockMetadata{},
			errors.New("bark supplied url returned nil response")
	}
	defer blockResp.Body.Close()

	if blockResp.StatusCode != http.StatusOK {
		return nil, types.BlockMetadata{},
			fmt.Errorf("bark supplied url returned non-ok: %d",
				blockResp.StatusCode)
	}

	blockBody, err := io.ReadAll(blockResp.Body)
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed reading block body: %w", err)
	}

	prevHash, err := hex.DecodeString(block.GetMeta().GetPrevHash())
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed decoding previous hash: %w", err)
	}

	blockType := block.GetMeta().GetType()
	if blockType < 0 {
		return nil, types.BlockMetadata{},
			fmt.Errorf("invalid block type: %d", blockType)
	}

	return blockBody, types.BlockMetadata{
		Type:     (uint)(blockType),
		Height:   block.GetBlock().GetHeight(),
		PrevHash: prevHash,
	}, nil
}

func (b *BlobStoreBark) DeleteBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	id uint64,
) error {
	return b.upstream.DeleteBlock(txn, slot, hash, id)
}

func (b *BlobStoreBark) TombstoneBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) error {
	return b.upstream.TombstoneBlock(txn, slot, hash)
}

func (b *BlobStoreBark) GetBlockURL(
	ctx context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	return b.upstream.GetBlockURL(ctx, txn, point)
}

func (b *BlobStoreBark) SetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
	cbor []byte,
) error {
	return b.upstream.SetUtxo(txn, txId, outputIdx, cbor)
}

func (b *BlobStoreBark) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	return b.upstream.GetUtxo(txn, txId, outputIdx)
}

func (b *BlobStoreBark) DeleteUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	return b.upstream.DeleteUtxo(txn, txId, outputIdx)
}

func (b *BlobStoreBark) SetTx(
	txn types.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	return b.upstream.SetTx(txn, txHash, offsetData)
}

func (b *BlobStoreBark) GetTx(txn types.Txn, txHash []byte) ([]byte, error) {
	return b.upstream.GetTx(txn, txHash)
}

func (b *BlobStoreBark) DeleteTx(txn types.Txn, txHash []byte) error {
	return b.upstream.DeleteTx(txn, txHash)
}

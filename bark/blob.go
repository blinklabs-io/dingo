package bark

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	archivev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"google.golang.org/protobuf/proto"
)

type BlobStoreBarkConfig struct {
	BaseUrl        string
	SecurityWindow uint64
	HTTPClient     *http.Client
	LedgerState    *ledger.LedgerState
	Logger         *slog.Logger
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
) *BlobStoreBark {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &BlobStoreBark{
		config: config,
		archiveClient: archiveconnect.NewArchiveServiceClient(
			httpClient,
			config.BaseUrl,
		),
		httpClient: httpClient,
		upstream:   upstream,
	}
}

func (b *BlobStoreBark) Close() error {
	return b.upstream.Close()
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
	return b.upstream.NewIterator(txn, opts)
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
	currentSlot, err := b.config.LedgerState.CurrentSlot()
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed to get current slot: %w", err)
	}

	// if the requested slot is still within the security window, defer to the
	// upstream blob storage to retrieve the block
	if b.config.SecurityWindow > currentSlot ||
		slot >= currentSlot-b.config.SecurityWindow {
		return b.upstream.GetBlock(txn, slot, hash)
	}

	resp, err := b.archiveClient.FetchBlock(
		context.Background(),
		connect.NewRequest(
			&archivev1alpha1.FetchBlockRequest{
				Blocks: []*archivev1alpha1.BlockRef{
					{
						Slot: proto.Uint64(slot),
						Hash: proto.String(hex.EncodeToString(hash)),
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

	blockResp, err := b.httpClient.Get(block.GetUrl())
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed downloading block from bark supplied url: %w", err)
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

	return blockBody, types.BlockMetadata{
		Type:     (uint)(block.GetMeta().GetType()),
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

func (b *BlobStoreBark) GetBlockURL(
	ctx context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	return types.SignedURL{}, types.BlockMetadata{},
		errors.New("not implemented")
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

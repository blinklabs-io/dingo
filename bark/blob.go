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
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"connectrpc.com/connect"
	archivev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// archiveFetchTimeout bounds a single archive round-trip (signed-URL request
// plus the follow-up download). Used by both GetBlock and the iterator's
// per-item expired-history resolution.
const archiveFetchTimeout = 20 * time.Second

// defaultMaxArchiveBlockSize caps archive download responses to guard against
// memory exhaustion from a malicious or misconfigured archive service when no
// protocol-derived limit is available.
// 128 KiB covers the current Cardano max block body size (~90 KiB) plus
// header/CBOR overhead while keeping malicious archive responses small.
const defaultMaxArchiveBlockSize = 128 * 1024

// validateArchiveURL rejects download URLs that could enable SSRF, credential
// leakage, or TLS-downgrade attacks.
func validateArchiveURL(rawURL string, allowedHosts map[string]struct{}) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.User != nil {
		return errors.New("URL must not contain embedded credentials")
	}
	if u.Scheme != "https" {
		return fmt.Errorf("URL must use HTTPS, got scheme %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("URL must include a host")
	}
	normalizedHost := strings.ToLower(host)
	if _, ok := allowedHosts[normalizedHost]; !ok {
		return fmt.Errorf("URL host %q is not allowed", host)
	}
	return nil
}

func archiveDownloadHosts(
	baseURL string,
	allowlist []string,
) map[string]struct{} {
	hosts := map[string]struct{}{}
	if u, err := url.Parse(baseURL); err == nil {
		addArchiveDownloadHost(hosts, u.Hostname())
	}
	for _, host := range allowlist {
		addArchiveDownloadHost(hosts, host)
	}
	return hosts
}

func addArchiveDownloadHost(hosts map[string]struct{}, host string) {
	host = strings.TrimSpace(host)
	if host == "" {
		return
	}
	if u, err := url.Parse(host); err == nil && u.Hostname() != "" {
		host = u.Hostname()
	} else if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	host = strings.Trim(host, "[]")
	host = strings.ToLower(host)
	if host != "" {
		hosts[host] = struct{}{}
	}
}

func archiveHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	secured := *client
	secured.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New(
			"bark: redirects are not permitted for archive downloads",
		)
	}
	return &secured
}

type BlobStoreBarkConfig struct {
	BaseUrl                   string
	HTTPClient                *http.Client
	BlockDownloadAllowedHosts []string
	// MaxBlockSize reports the largest block, in bytes, an archive download may
	// carry. It is read on every download, so a limit that follows the live
	// protocol parameters stays current. A nil func, or a zero result, selects
	// the default cap.
	MaxBlockSize func() uint64
}

type BlobStoreBark struct {
	config                    BlobStoreBarkConfig
	archiveClient             archiveconnect.ArchiveServiceClient
	httpClient                *http.Client
	blockDownloadAllowedHosts map[string]struct{}
	upstream                  blob.BlobStore
}

func NewBarkBlobStore(
	config BlobStoreBarkConfig,
	upstream blob.BlobStore,
) (*BlobStoreBark, error) {
	if upstream == nil {
		return nil, errors.New("bark: upstream blob store is required")
	}

	httpClient := archiveHTTPClient(config.HTTPClient)

	return &BlobStoreBark{
		config: config,
		archiveClient: archiveconnect.NewArchiveServiceClient(
			httpClient,
			config.BaseUrl,
		),
		httpClient: httpClient,
		blockDownloadAllowedHosts: archiveDownloadHosts(
			config.BaseUrl,
			config.BlockDownloadAllowedHosts,
		),
		upstream: upstream,
	}, nil
}

// maxBlockSize returns the largest archive download to accept.
func (b *BlobStoreBark) maxBlockSize() int64 {
	if b.config.MaxBlockSize != nil {
		if limit := b.config.MaxBlockSize(); limit > 0 &&
			limit <= math.MaxInt64-1 {
			return int64(limit)
		}
	}
	return defaultMaxArchiveBlockSize
}

func (b *BlobStoreBark) Close() error {
	return b.upstream.Close()
}

func (b *BlobStoreBark) DiskSize() (int64, error) {
	return b.upstream.DiskSize()
}

func (b *BlobStoreBark) Sync() error {
	return b.upstream.Sync()
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

func (it *barkIterator) Rewind() { it.upstream.Rewind() }

func (it *barkIterator) Seek(
	prefix []byte,
) {
	it.upstream.Seek(prefix)
}

func (it *barkIterator) Valid() bool { return it.upstream.Valid() }

func (it *barkIterator) ValidForPrefix(
	p []byte,
) bool {
	return it.upstream.ValidForPrefix(p)
}
func (it *barkIterator) Next()  { it.upstream.Next() }
func (it *barkIterator) Close() { it.upstream.Close() }

func (it *barkIterator) Err() error { return it.upstream.Err() }

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
	archiveCbor, archiveMeta, archErr := b.fetchBlockFromArchive(
		ctx,
		slot,
		hash,
	)
	if archErr != nil {
		return nil, types.BlockMetadata{}, archErr
	}
	// Prefer the local metadata for ID (the archive does not know our
	// local block IDs), and fill Type/Height/PrevHash from the archive
	// result when upstream returned a zero metadata struct alongside the
	// expired-history error. Those fields are derived from the decoded,
	// hash-verified block rather than from the archive's own claims.
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

// RemainingTxnEntries forwards the upstream store's transaction budget.
// Transactions come from upstream unchanged, so its bound is the one a
// caller staging a bulk delete has to respect; a wrapper that swallowed the
// question would leave that caller staging without one.
func (b *BlobStoreBark) RemainingTxnEntries(
	txn types.Txn,
	entryBytes int,
) (int, bool) {
	budget, ok := b.upstream.(blob.TxnBudget)
	if !ok {
		return 0, false
	}
	return budget.RemainingTxnEntries(txn, entryBytes)
}

// GetBlockLocal bypasses Bark's archive fallback. Nested wrappers are
// unwrapped through the same optional interface.
func (b *BlobStoreBark) GetBlockLocal(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	if reader, ok := b.upstream.(blob.LocalBlockReader); ok {
		return reader.GetBlockLocal(txn, slot, hash)
	}
	return b.upstream.GetBlock(txn, slot, hash)
}

// archiveReportedMissing reports whether notFound answers the reference
// this node actually asked for. FetchBlock echoes an unresolved reference
// back carrying the identifiers the caller supplied, and this client always
// supplies both slot and hash, so the answer for this request carries both
// too. Anything else describes a different block and is not evidence that
// this one is absent. The hash is compared case-insensitively because the
// field is hex text echoed verbatim.
func archiveReportedMissing(
	notFound []*archivev1alpha1.BlockRef,
	slot uint64,
	hash []byte,
) bool {
	wantHash := hex.EncodeToString(hash)
	for _, ref := range notFound {
		if ref == nil || ref.Slot == nil || ref.Hash == nil {
			continue
		}
		if ref.GetSlot() != slot {
			continue
		}
		if strings.EqualFold(ref.GetHash(), wantHash) {
			return true
		}
	}
	return false
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
			fmt.Errorf(
				"failed getting signed url from bark archive service: %w",
				err,
			)
	}

	blocks := resp.Msg.GetBlocks()
	if len(blocks) != 1 {
		// The archive reports a block it does not hold under not_found and
		// still answers the rest of the batch, so an empty blocks list
		// carrying the not_found entry for this reference is a missing
		// block rather than a broken archive. Report it the way a local
		// blob store reports one.
		//
		// The entry has to be the one that was asked for. A returned block
		// is re-verified against (slot, hash) by verifyArchiveBlock, but
		// nothing re-verifies an absence, so an archive echoing some other
		// reference would otherwise have this node record the requested
		// block as missing on the strength of an answer about a different
		// block.
		if len(blocks) == 0 &&
			archiveReportedMissing(resp.Msg.GetNotFound(), slot, hash) {
			return nil, types.BlockMetadata{},
				fmt.Errorf(
					"bark: archive has no block at slot %d: %w",
					slot,
					types.ErrBlobKeyNotFound,
				)
		}
		return nil, types.BlockMetadata{},
			fmt.Errorf("expected 1 block, got %d", len(blocks))
	}

	block := blocks[0]

	if err := validateArchiveURL(block.GetUrl(), b.blockDownloadAllowedHosts); err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("bark: archive returned unsafe download URL: %w", err)
	}

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
			fmt.Errorf(
				"failed downloading block from bark supplied url: %w",
				err,
			)
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

	limit := b.maxBlockSize()
	blockBody, err := io.ReadAll(io.LimitReader(blockResp.Body, limit+1))
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed reading block body: %w", err)
	}
	if int64(len(blockBody)) > limit {
		return nil, types.BlockMetadata{},
			fmt.Errorf("bark: archive response exceeds %d-byte limit", limit)
	}

	archivePrevHash, err := hex.DecodeString(block.GetMeta().GetPrevHash())
	if err != nil {
		return nil, types.BlockMetadata{},
			fmt.Errorf("failed decoding previous hash: %w", err)
	}

	blockType := block.GetMeta().GetType()
	if blockType < 0 {
		return nil, types.BlockMetadata{},
			fmt.Errorf("invalid block type: %d", blockType)
	}

	decoded, err := verifyArchiveBlock(
		uint(blockType), blockBody, slot, hash,
	)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}
	// The claimed type is the era the bytes were just decoded and hashed under.
	// It is not re-derived from the header: the protocol major a header
	// announces is its producer's hard-fork readiness, which runs ahead of the
	// era at a boundary, so gledger.DetermineBlockType classifies a genuine
	// block there as another era or as none. A claim that does not match the
	// bytes fails decode, the body-hash check, or the hash check in
	// verifyArchiveBlock; a layout-compatible adjacent era decodes the same
	// bytes to the same hash, so the bytes are still the requested block and
	// only the recorded type follows the claim, the residual
	// internal/blockverify.Hash also accepts.
	era := uint(blockType)
	if err := assertBodyFullyAuthenticated(era); err != nil {
		return nil, types.BlockMetadata{}, err
	}
	meta, err := archiveBlockMetadata(
		decoded, era, block.GetBlock().GetHeight(), archivePrevHash,
	)
	if err != nil {
		return nil, types.BlockMetadata{}, err
	}

	return blockBody, meta, nil
}

// Errors reported when an archive response fails local verification. They are
// distinct from transport failures on purpose: a transport error is worth
// retrying, whereas these mean the archive served something that is not the
// block that was asked for, and its answers cannot be trusted as chain data.
var (
	// ErrArchiveBlockUndecodable reports an archive response body that does
	// not decode as a block of the type the archive claimed.
	ErrArchiveBlockUndecodable = errors.New(
		"bark: archive block could not be decoded",
	)
	// ErrArchiveBlockHashMismatch reports a decoded block whose computed
	// hash is not the hash that was requested.
	ErrArchiveBlockHashMismatch = errors.New(
		"bark: archive block hash does not match the requested hash",
	)
	// ErrArchiveBlockSlotMismatch reports a decoded block that does not sit
	// at the slot that was requested.
	ErrArchiveBlockSlotMismatch = errors.New(
		"bark: archive block slot does not match the requested slot",
	)
	// ErrArchiveMetadataMismatch reports archive-supplied metadata that
	// contradicts the contents of the verified block it accompanied.
	ErrArchiveMetadataMismatch = errors.New(
		"bark: archive metadata contradicts the block",
	)
	// ErrArchiveBlockNotFullyAuthenticated reports a block whose body cannot
	// be bound to its header in full, so the archive could alter the
	// unauthenticated part without changing anything checked here.
	ErrArchiveBlockNotFullyAuthenticated = errors.New(
		"bark: archive block body cannot be fully authenticated",
	)
)

// assertBodyFullyAuthenticated refuses blocks whose body is only partly bound
// to their header.
//
// Byron main blocks are the sole case. gouroboros checks their transaction,
// delegation, and update proofs but not ssc_proof, because the SSC proof
// hashes cardano-ledger's own encoding of the sub-payloads rather than the
// bytes carried in the block. An alteration confined to the SSC payload
// therefore changes nothing this package verifies — hash, slot, height, and
// previous hash all come from the untouched header — so the archive could
// still substitute part of a historical block.
//
// Epoch boundary blocks are unaffected: they carry no transactions and no SSC
// payload, and a single body hash covers the whole body.
//
// This restriction can be lifted once Byron SSC proof validation exists
// upstream.
func assertBodyFullyAuthenticated(blockType uint) error {
	if blockType == gledger.BlockTypeByronMain {
		return fmt.Errorf(
			"%w: byron main block ssc payload is unverified",
			ErrArchiveBlockNotFullyAuthenticated,
		)
	}
	return nil
}

// verifyArchiveBlock establishes locally that the bytes the archive returned
// really are the block that was requested. Bark chooses both the download URL
// and the response body, so it can only be trusted to store blocks, not to
// identify them: consensus-relevant identity is re-derived here before any
// caller sees the data.
//
// The block type carried in the archive response is a decode hint only. A
// wrong type fails to decode or yields a different block hash, and both are
// rejected below; a layout-compatible adjacent era decodes the same bytes to
// the same hash. Either way it cannot be used to smuggle in substitute bytes. Decoding runs with validation enabled, so a block whose
// header is genuine but whose body was swapped fails the body-hash check.
func verifyArchiveBlock(
	blockType uint,
	body []byte,
	slot uint64,
	hash []byte,
) (gledger.Block, error) {
	decoded, err := models.DecodeBlockCbor(blockType, body)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: slot %d, type %d: %w",
			ErrArchiveBlockUndecodable, slot, blockType, err,
		)
	}
	decodedHash := decoded.Hash()
	if !bytes.Equal(decodedHash[:], hash) {
		return nil, fmt.Errorf(
			"%w: got %x, requested %x",
			ErrArchiveBlockHashMismatch, decodedHash[:], hash,
		)
	}
	if decoded.SlotNumber() != slot {
		return nil, fmt.Errorf(
			"%w: block %x is at slot %d, requested slot %d",
			ErrArchiveBlockSlotMismatch,
			decodedHash[:], decoded.SlotNumber(), slot,
		)
	}
	return decoded, nil
}

// archiveBlockMetadata derives block metadata from the verified block rather
// than from what the archive claimed alongside it, and rejects archive-supplied
// values that contradict the block. The bytes are already hash-verified by this
// point, so a disagreement means the archive is misreporting; failing is more
// useful than silently preferring the decoded value and carrying on.
//
// Zero-valued archive fields are treated as absent rather than as a conflict:
// the archive is not required to populate them.
func archiveBlockMetadata(
	decoded gledger.Block,
	era uint,
	archiveHeight uint64,
	archivePrevHash []byte,
) (types.BlockMetadata, error) {
	height := decoded.BlockNumber()
	if archiveHeight != 0 && archiveHeight != height {
		return types.BlockMetadata{}, fmt.Errorf(
			"%w: reported height %d, block height %d",
			ErrArchiveMetadataMismatch, archiveHeight, height,
		)
	}
	prevHash := decoded.PrevHash()
	if len(archivePrevHash) > 0 && !bytes.Equal(archivePrevHash, prevHash[:]) {
		return types.BlockMetadata{}, fmt.Errorf(
			"%w: reported previous hash %x, block previous hash %x",
			ErrArchiveMetadataMismatch, archivePrevHash, prevHash[:],
		)
	}
	return types.BlockMetadata{
		Type:     era,
		Height:   height,
		PrevHash: prevHash[:],
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

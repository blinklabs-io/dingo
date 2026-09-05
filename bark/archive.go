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
	"math"

	"connectrpc.com/connect"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/protocol/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// blockHashLength is the byte length of a Cardano block hash, which the
// hex-encoded BlockRef.hash field has to decode to.
const blockHashLength = 32

var _ archiveconnect.ArchiveServiceHandler = &archiveServiceHandler{}

type archiveServiceHandler struct {
	bark *Bark
}

// blockRefRequest is one validated archive.BlockRef. It keeps which
// identifiers the client actually supplied, separately from their values,
// because that is both what has to agree with the stored block and what
// has to be echoed back in the response.
type blockRefRequest struct {
	hashHex   string
	hash      []byte
	slot      uint64
	height    uint64
	hasHash   bool
	hasSlot   bool
	hasHeight bool
}

// parseBlockRef validates one reference without touching storage. A
// reference naming no identifier at all, or carrying something that is not
// a hex-encoded block hash, is not a valid BlockRef under the protocol --
// it names no block rather than naming a block that happens to be absent
// -- so it is rejected outright instead of being reported as not_found.
func parseBlockRef(ref *archive.BlockRef) (blockRefRequest, error) {
	if ref == nil {
		return blockRefRequest{}, errors.New("block reference is nil")
	}
	if ref.Hash == nil && ref.Slot == nil && ref.Height == nil {
		return blockRefRequest{}, errors.New(
			"at least one of hash, slot, or height is required",
		)
	}
	parsed := blockRefRequest{
		hasHash:   ref.Hash != nil,
		hasSlot:   ref.Slot != nil,
		hasHeight: ref.Height != nil,
		slot:      ref.GetSlot(),
		height:    ref.GetHeight(),
	}
	if parsed.hasHash {
		hash, err := hex.DecodeString(ref.GetHash())
		if err != nil {
			return blockRefRequest{}, fmt.Errorf(
				"failed decoding hash %q: %w", ref.GetHash(), err,
			)
		}
		if len(hash) != blockHashLength {
			return blockRefRequest{}, fmt.Errorf(
				"hash length must be %d bytes, got %d",
				blockHashLength,
				len(hash),
			)
		}
		parsed.hashHex = ref.GetHash()
		parsed.hash = hash
	}
	return parsed, nil
}

// requested rebuilds the reference exactly as the client supplied it, for
// the not_found list, so the client can match the answer to its question.
func (r blockRefRequest) requested() *archive.BlockRef {
	ref := &archive.BlockRef{}
	if r.hasHash {
		ref.Hash = new(r.hashHex)
	}
	if r.hasSlot {
		ref.Slot = new(r.slot)
	}
	if r.hasHeight {
		ref.Height = new(r.height)
	}
	return ref
}

// resolved describes a found block. Every identifier the client supplied is
// preserved verbatim -- a hash sent in upper case comes back in upper case
// -- and the identifiers it left out are filled in from the stored block,
// so a hash-only or height-only caller still learns the full identity.
func (r blockRefRequest) resolved(
	point common.Point,
	height uint64,
) *archive.BlockRef {
	hashHex := r.hashHex
	if !r.hasHash {
		hashHex = hex.EncodeToString(point.Hash)
	}
	return &archive.BlockRef{
		Hash:   new(hashHex),
		Slot:   new(point.Slot),
		Height: new(height),
	}
}

// resolveBlockPoint maps a validated reference to the (slot, hash) point
// the blob store keys blocks by, and reports whether resolving it actually
// read the block out of the store. That second value is what separates "the
// archive does not hold this block" from "the archive's index holds it but
// the blob store will not serve it": only a lookup path proves the block was
// there a moment ago.
//
// The precedence is hash+slot, then hash, then slot, then height:
//
//   - hash and slot together already are the blob store's block key, so
//     they need no index lookup at all. Going through the hash index
//     instead would also refuse a block written before that index existed
//     (#1915), which is exactly the historical range an archive serves.
//   - a hash alone is the only identifier unique across forks, and it
//     resolves through an O(1) hash-index read.
//   - a slot alone needs a bounded prefix scan of that one slot.
//   - a height alone is last: block numbers are not indexed at all, so
//     resolving one is a binary search over the block-ID space.
//
// Because the point is always built from the identifiers the client
// supplied, a returned point agrees with them by construction: a hash
// paired with the wrong slot simply misses the blob key, and a slot-only
// or hash-only lookup returns the block that carries it. Height is the one
// identifier that cannot be checked here, since it is not what any of
// these lookups key on -- FetchBlock validates it against the block
// metadata instead.
func resolveBlockPoint(
	db *database.Database,
	ref blockRefRequest,
) (common.Point, bool, error) {
	switch {
	case ref.hasHash && ref.hasSlot:
		// Taken at face value: nothing was read, so this says nothing
		// about whether the archive holds the block.
		return common.NewPoint(ref.slot, ref.hash), false, nil
	case ref.hasHash:
		block, err := database.BlockByHash(db, ref.hash)
		if err != nil {
			return common.Point{}, false, err
		}
		return common.NewPoint(block.Slot, block.Hash), true, nil
	case ref.hasSlot:
		block, err := database.BlockBySlot(db, ref.slot)
		if err != nil {
			return common.Point{}, false, err
		}
		return common.NewPoint(block.Slot, block.Hash), true, nil
	default:
		block, err := database.BlockByNumber(db, ref.height)
		if err != nil {
			return common.Point{}, false, err
		}
		return common.NewPoint(block.Slot, block.Hash), true, nil
	}
}

// isBlockMissing reports whether err means the reference names no stored
// block, rather than a storage failure. Both spellings occur: the block
// lookups report a miss as models.ErrBlockNotFound, while the cloud blob
// plugins report an absent object as types.ErrBlobKeyNotFound.
func isBlockMissing(err error) bool {
	return errors.Is(err, models.ErrBlockNotFound) ||
		errors.Is(err, types.ErrBlobKeyNotFound)
}

// logBlockUnservable records a block the archive resolved out of its own
// index and then could not sign a URL for.
//
// The cloud blob plugins report a lost metadata object with the same
// types.ErrBlobKeyNotFound they use for an absent block object -- gcs
// GetBlock and GetBlockURL log the partial write and return that error
// anyway, and s3 GetBlockURL reads metadata before it ever heads the block
// object -- so the error alone cannot tell the two apart. What does tell
// them apart is that a lookup path already read this block: the index says
// the archive holds it, and the blob store now says it does not.
//
// The reference is still answered in not_found. There is no URL to hand the
// client, FetchBlockResponse has no per-reference error field, and failing
// the call would discard every other block in the batch -- the exact defect
// #3442 asked to remove. The operator, who is the only party that can act on
// it, gets the log line instead.
func (a *archiveServiceHandler) logBlockUnservable(
	point common.Point,
	err error,
) {
	if a.bark == nil || a.bark.config.Logger == nil {
		return
	}
	a.bark.config.Logger.Warn(
		"block resolved from the index but the blob store reports it missing; treating as not_found",
		"component",
		"bark",
		"slot",
		point.Slot,
		"hash",
		hex.EncodeToString(point.Hash),
		"error",
		err,
	)
}

// FetchBlock resolves each requested block reference to a signed URL. A
// reference the archive does not hold is reported in
// FetchBlockResponse.not_found and the rest of the batch is still served;
// only a malformed request or a storage failure fails the whole call.
func (a *archiveServiceHandler) FetchBlock(
	ctx context.Context,
	req *connect.Request[archive.FetchBlockRequest],
) (*connect.Response[archive.FetchBlockResponse], error) {
	resp := &archive.FetchBlockResponse{}
	blocks := req.Msg.GetBlocks()
	if len(blocks) == 0 {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("at least one block reference is required"),
		)
	}
	if len(blocks) > DefaultMaxFetchBlockRefs {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf(
				"block reference count %d exceeds maximum %d",
				len(blocks),
				DefaultMaxFetchBlockRefs,
			),
		)
	}

	// Validate the whole batch before acquiring the database, so a
	// malformed request costs no storage work.
	refs := make([]blockRefRequest, 0, len(blocks))
	for i, b := range blocks {
		ref, err := parseBlockRef(b)
		if err != nil {
			return nil, connect.NewError(
				connect.CodeInvalidArgument,
				fmt.Errorf("block reference %d: %w", i, err),
			)
		}
		refs = append(refs, ref)
	}

	db, release, err := a.bark.Acquire()
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	defer release()

	for _, ref := range refs {
		point, confirmed, err := resolveBlockPoint(db, ref)
		if err != nil {
			if isBlockMissing(err) {
				resp.NotFound = append(resp.NotFound, ref.requested())
				continue
			}
			return nil, fmt.Errorf(
				"failed resolving block reference: %w",
				err,
			)
		}

		signedURL, metadata, err := database.BlockURL(ctx, db, point)
		if err != nil {
			if isBlockMissing(err) {
				if confirmed {
					a.logBlockUnservable(point, err)
				}
				resp.NotFound = append(resp.NotFound, ref.requested())
				continue
			}
			return nil, fmt.Errorf(
				"failed getting signed url for block [%d, %s]: %w",
				point.Slot,
				hex.EncodeToString(point.Hash),
				err,
			)
		}

		// The point already agrees with any hash and slot the client sent
		// (see resolveBlockPoint), but height is not what any lookup keys
		// on. A reference whose height belongs to a different block
		// describes no stored block, so it is reported as not_found rather
		// than served the block its other identifiers happened to name.
		if ref.hasHeight && ref.height != metadata.Height {
			resp.NotFound = append(resp.NotFound, ref.requested())
			continue
		}

		blockType := metadata.Type
		if blockType > math.MaxInt32 {
			return nil, fmt.Errorf("invalid block type: %d", blockType)
		}

		resp.Blocks = append(resp.Blocks, &archive.SignedUrl{
			Block:     ref.resolved(point, metadata.Height),
			Url:       signedURL.URL.String(),
			ExpiresAt: timestamppb.New(signedURL.Expires),
			Meta: &archive.BlockMeta{
				Type:     archive.BlockType(blockType).Enum(),
				PrevHash: new(hex.EncodeToString(metadata.PrevHash)),
			},
		})
	}

	return connect.NewResponse(resp), nil
}

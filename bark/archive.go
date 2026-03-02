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
	"math"

	"connectrpc.com/connect"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/protocol/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ archiveconnect.ArchiveServiceHandler = &archiveServiceHandler{}

type archiveServiceHandler struct {
	bark *Bark
}

func (a *archiveServiceHandler) FetchBlock(
	ctx context.Context,
	req *connect.Request[archive.FetchBlockRequest],
) (*connect.Response[archive.FetchBlockResponse], error) {
	resp := &archive.FetchBlockResponse{}

	for _, b := range req.Msg.GetBlocks() {
		hash, err := hex.DecodeString(b.GetHash())
		if err != nil {
			return nil,
				fmt.Errorf("failed decoding hash %q: %w", b.GetHash(), err)
		}
		if len(hash) != 32 {
			return nil,
				fmt.Errorf("hash length must be 32 bytes, got %d", len(hash))
		}

		point := common.NewPoint(b.GetSlot(), hash)
		signedURL, metadata, err := database.BlockURL(ctx, a.bark.config.DB, point)
		if err != nil {
			return nil, fmt.Errorf("failed getting signed url for block [%d, %s]: %w", point.Slot, point.Hash, err)
		}

		blockType := metadata.Type
		if blockType > math.MaxInt32 {
			return nil, fmt.Errorf("invalid block type: %d", blockType)
		}

		resp.Blocks = append(resp.Blocks, &archive.SignedUrl{
			Block: &archive.BlockRef{
				Hash:   b.Hash,
				Slot:   b.Slot,
				Height: proto.Uint64(metadata.Height),
			},
			Url:       signedURL.URL.String(),
			ExpiresAt: timestamppb.New(signedURL.Expires),
			Meta: &archive.BlockMeta{
				Type:     (archive.BlockType)(blockType).Enum(),
				PrevHash: proto.String(hex.EncodeToString(metadata.PrevHash)),
			},
		})
	}

	return connect.NewResponse(resp), nil
}

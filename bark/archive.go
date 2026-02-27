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
		point := common.NewPoint(b.GetSlot(), []byte(b.GetHash()))
		signedURL, metadata, err := database.BlockURL(ctx, a.bark.config.DB, point)
		if err != nil {
			return nil, fmt.Errorf("failed getting signed url for block [%d, %s]: %w", point.Slot, point.Hash, err)
		}

		blockType := metadata.Type
		if blockType >= math.MaxInt32 {
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

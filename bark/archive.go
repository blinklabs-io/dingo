package bark

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

var _ archive.ArchiveServiceHandler = &archiveServiceHandler{}

func NewArchiveServiceHandler(b *Bark) archive.ArchiveServiceHandler {
	return &archiveServiceHandler{
		bark: b,
	}
}

type archiveServiceHandler struct {
	bark *Bark
}

func (a *archiveServiceHandler) FetchBlock(ctx context.Context, req *connect.Request[archivev1alpha1.FetchBlockRequest]) (*connect.Response[archivev1alpha1.FetchBlockResponse], error) {
	resp := &connect.Response[archivev1alpha1.FetchBlockResponse]{}

	for _, b := range req.Msg.GetBlocks() {
		point := common.NewPoint(b.GetSlot(), []byte(b.GetHash()))
		u, err := database.BlockURL(a.bark.config.DB, point)
		if err != nil {
			return nil, fmt.Errorf("failed getting signed url for block %v: %w", point, err)
		}
		resp.Msg.Blocks = append(resp.Msg.Blocks, &archivev1alpha1.SignedUrl{
			Url: u.String(),
		})
	}

	return connect.NewResponse[archivev1alpha1.FetchBlockResponse](nil), nil
}

package bark

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	"github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

var _ archivev1alpha1connect.ArchiveServiceHandler = &archiveServiceHandler{}

type archiveServiceHandler struct {
	bark *Bark
}

func (a *archiveServiceHandler) FetchBlock(
	ctx context.Context,
	req *connect.Request[archive.FetchBlockRequest],
) (*connect.Response[archive.FetchBlockResponse], error) {
	resp := &connect.Response[archive.FetchBlockResponse]{}

	for _, b := range req.Msg.GetBlocks() {
		point := common.NewPoint(b.GetSlot(), []byte(b.GetHash()))
		u, err := database.BlockURL(a.bark.config.DB, point)
		if err != nil {
			return nil, fmt.Errorf("failed getting signed url for block %v: %w", point, err)
		}
		resp.Msg.Blocks = append(resp.Msg.Blocks, &archive.SignedUrl{
			Url: u.String(),
		})
	}

	return connect.NewResponse[archive.FetchBlockResponse](nil), nil
}

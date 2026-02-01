package bark

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
)

var (
	_ archive.ArchiveServiceHandler = &archiveServiceHandler{}
)

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

	for _, b := range req.Msg.Blocks {
		txn := a.bark.config.DB.BlobTxn(false)
		u, err := a.bark.config.DB.Blob().GetBlockURL(txn, []byte(b.GetHash()), b.GetSlot())
		if err != nil {
			return nil, fmt.Errorf("failed getting signed url for block %x: %w", b.GetHash(), err)
		}
		resp.Msg.Blocks = append(resp.Msg.Blocks, &archivev1alpha1.SignedUrl{
			Url: u.String(),
		})
	}

	return connect.NewResponse[archivev1alpha1.FetchBlockResponse](nil), nil
}

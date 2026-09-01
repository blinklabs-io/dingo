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
	"compress/gzip"
	"encoding/hex"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"
	archivev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func gzipBarkRequestBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func sendBarkProtoRequest(
	t *testing.T,
	client *http.Client,
	baseURL string,
	procedure string,
	body []byte,
	compressed bool,
) *http.Response {
	t.Helper()
	if compressed {
		body = gzipBarkRequestBody(t, body)
	}
	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		baseURL+procedure,
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Connect-Protocol-Version", "1")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestBarkConnectLimitRejectsOversizedDecompressedMessageBeforeAuth(
	t *testing.T,
) {
	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	_, _, clientCAPath := writeTestCA(t)
	b, err := NewBark(BarkConfig{
		DB:                              newTestDB(t),
		Lifecycle:                       newTestLifecycleService(t),
		SnapshotDir:                     t.TempDir(),
		Host:                            "127.0.0.1",
		Port:                            freeTCPPort(t),
		TlsCertFilePath:                 serverCertPath,
		TlsKeyFilePath:                  serverKeyPath,
		TlsClientCAFilePath:             clientCAPath,
		OperatorCertificateFingerprints: []string{strings.Repeat("00", 32)},
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() { _ = b.Stop(t.Context()) })

	// A highly-compressible unknown protobuf field expands beyond the limit.
	// The destructive procedure would reject this anonymous client at the
	// interceptor, so HTTP 429 also proves the decoder enforced the limit first.
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(
		body,
		bytes.Repeat([]byte{'a'}, DefaultMaxRequestBody),
	)
	compressed := gzipBarkRequestBody(t, body)
	require.Less(t, len(compressed), DefaultMaxRequestBody)

	resp := sendBarkProtoRequest(
		t,
		mtlsHTTPClient(t, "", ""),
		"https://"+b.Addr(),
		databaseconnect.DatabaseServiceCancelOperationProcedure,
		body,
		true,
	)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
}

func TestBarkConnectLimitRejectsOversizedCompressedWireMessage(t *testing.T) {
	b, err := NewBark(BarkConfig{
		DB:   newTestDB(t),
		Host: "127.0.0.1",
		Port: freeTCPPort(t),
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() { _ = b.Stop(t.Context()) })

	// Random payload bytes do not compress, so gzip expands the request. The
	// decoded protobuf message remains within the limit while the compressed
	// wire message exceeds it, isolating the pre-decompression wire bound.
	payload := make([]byte, DefaultMaxRequestBody-5)
	_, err = rand.NewChaCha8([32]byte{1}).Read(payload)
	require.NoError(t, err)
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(body, payload)
	compressed := gzipBarkRequestBody(t, body)
	require.LessOrEqual(t, len(body), DefaultMaxRequestBody)
	require.Greater(t, len(compressed), DefaultMaxRequestBody)

	resp := sendBarkProtoRequest(
		t,
		http.DefaultClient,
		"http://"+b.Addr(),
		archiveconnect.ArchiveServiceFetchBlockProcedure,
		body,
		true,
	)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
}

func TestArchiveFetchBlockRejectsInvalidBatchSizes(t *testing.T) {
	db := newTestDB(t)
	handler := &archiveServiceHandler{bark: newTestBark(t, db)}

	t.Run("empty", func(t *testing.T) {
		_, err := handler.FetchBlock(
			t.Context(),
			connect.NewRequest(&archivev1alpha1.FetchBlockRequest{}),
		)
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("oversized", func(t *testing.T) {
		blocks := make(
			[]*archivev1alpha1.BlockRef,
			DefaultMaxFetchBlockRefs+1,
		)
		for i := range blocks {
			blocks[i] = &archivev1alpha1.BlockRef{}
		}
		_, err := handler.FetchBlock(
			t.Context(),
			connect.NewRequest(&archivev1alpha1.FetchBlockRequest{
				Blocks: blocks,
			}),
		)
		require.Error(t, err)
		require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	})

	t.Run("maximum reaches storage", func(t *testing.T) {
		block := testBlock(1, 0x42)
		require.NoError(t, db.BlockCreate(block, nil))
		blocks := make(
			[]*archivev1alpha1.BlockRef,
			DefaultMaxFetchBlockRefs,
		)
		for i := range blocks {
			blocks[i] = &archivev1alpha1.BlockRef{
				Hash: new(hex.EncodeToString(block.Hash)),
				Slot: new(block.Slot),
			}
		}
		_, err := handler.FetchBlock(
			t.Context(),
			connect.NewRequest(&archivev1alpha1.FetchBlockRequest{
				Blocks: blocks,
			}),
		)
		// The in-memory test blob store deliberately does not support signed
		// URLs. Reaching its exact error proves equality passed the count guard;
		// an accidental >= comparison would return ResourceExhausted first.
		require.ErrorContains(t, err, "GetBlockURL not supported")
		require.NotEqual(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	})
}

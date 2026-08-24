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

package utxorpc

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func gzipRequestBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func sendHealthRequest(
	t *testing.T,
	handler http.Handler,
	body []byte,
	compressed bool,
	authHeader string,
) *httptest.ResponseRecorder {
	t.Helper()
	contentEncoding := ""
	if compressed {
		body = gzipRequestBody(t, body)
		contentEncoding = "gzip"
	}
	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://example.test/grpc.health.v1.Health/Check",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func TestConnectRequestBodyLimitPreservesValidMessages(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{})
	handler := u.newServeMux()

	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncompressed", true: "compressed"}[compressed], func(t *testing.T) {
			resp := sendHealthRequest(t, handler, []byte("{}"), compressed, "")
			require.Equal(t, http.StatusOK, resp.Code)
		})
	}
}

func TestConnectRequestBodyLimitPreservesAuthentication(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{})
	verifier, err := apiauth.NewVerifier(apiconfig.EffectiveAuth{
		Enabled: true,
		Token:   "shared-secret",
	})
	require.NoError(t, err)
	u.verifier = verifier
	handler := u.newServeMux()

	missing := sendHealthRequest(t, handler, []byte("{}"), false, "")
	require.Equal(t, http.StatusUnauthorized, missing.Code)
	valid := sendHealthRequest(
		t,
		handler,
		[]byte("{}"),
		false,
		"Bearer shared-secret",
	)
	require.Equal(t, http.StatusOK, valid.Code)
}

func TestConnectRequestBodyLimitRejectsOversizedCompressedMessage(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{})
	handler := u.newServeMux()

	// Keep the wire body small while making the decoded protobuf message exceed
	// the limit. The Connect handler must stop decompression at the configured
	// limit, before the request reaches an interceptor or service method.
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(body, bytes.Repeat([]byte{'a'}, DefaultMaxRequestBody))
	compressed := gzipRequestBody(t, body)
	require.Less(t, len(compressed), DefaultMaxRequestBody)

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://example.test/grpc.health.v1.Health/Check",
		bytes.NewReader(compressed),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Connect-Protocol-Version", "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	require.Equal(t, http.StatusTooManyRequests, response.Code)
}

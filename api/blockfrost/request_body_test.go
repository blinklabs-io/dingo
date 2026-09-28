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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransactionBodyReadDeadline(t *testing.T) {
	for _, route := range []struct {
		path        string
		contentType string
		body        string
	}{
		{"/api/v0/tx/submit", "application/cbor", "80"},
		{"/api/v0/utils/txs/evaluate", "application/cbor", "80"},
		{"/api/v0/utils/txs/evaluate/utxos", "application/json", `{"cbor":"80"}`},
	} {
		t.Run(route.path, func(t *testing.T) {
			for _, mode := range []string{"complete", "truncated", "stalled"} {
				t.Run(mode, func(t *testing.T) {
					b := New(BlockfrostConfig{}, &mockNode{}, nil)
					b.requestBodyTimeout = 100 * time.Millisecond
					server := httptest.NewServer(b.handler())
					defer server.Close()
					conn, err := net.DialTimeout(
						"tcp",
						strings.TrimPrefix(server.URL, "http://"),
						5*time.Second,
					)
					require.NoError(t, err)
					// Close the client before server teardown, including failed assertions.
					defer conn.Close()
					length := len(route.body)
					if mode != "complete" {
						length += 4096
					}
					_, err = fmt.Fprintf(
						conn,
						"POST %s HTTP/1.1\r\nHost: localhost\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n%s",
						route.path,
						route.contentType,
						length,
						route.body,
					)
					require.NoError(t, err)
					if mode == "truncated" {
						require.NoError(t, conn.(*net.TCPConn).CloseWrite())
					}
					require.NoError(
						t,
						conn.SetReadDeadline(time.Now().Add(3*time.Second)),
					)
					response, err := http.ReadResponse(
						bufio.NewReader(conn),
						nil,
					)
					require.NoError(
						t,
						err,
						"body reader did not return a response to the %s client",
						mode,
					)
					defer response.Body.Close()
					body, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					if mode == "complete" {
						require.Equal(
							t,
							http.StatusOK,
							response.StatusCode,
							string(body),
						)
					} else {
						require.Equal(t, http.StatusBadRequest, response.StatusCode, string(body))
						require.Contains(t, string(body), "failed to read transaction body")
					}
				})
			}
		})
	}
}

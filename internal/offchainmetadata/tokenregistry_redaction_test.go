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

package offchainmetadata

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenRegistryRedactsURLLogs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		source       string
		status       int
		files        map[string]string
		transportErr bool
		message      string
	}{
		{name: "success", status: 200, files: map[string]string{
			"mappings/valid.json": `{"subject":"` + syncSubjectNut + `","name":{"value":"Nut"}}`,
		}, message: "token registry sync complete"},
		{name: "unchanged", status: 304, message: "token registry unchanged"},
		{name: "empty", status: 200, message: "no usable mappings"},
		{name: "partial", status: 200, files: map[string]string{
			"mappings/bad.json": `{`,
		}, message: "snapshot had unusable mappings"},
		{name: "status error", status: 503, message: "token registry sync failed"},
		{name: "transport error", transportErr: true, message: "token registry sync failed"},
		{name: "userinfo", source: "https://private-user:private-password@registry.example/registry?key=query-secret#fragment-secret", message: "token registry sync failed"},
		{name: "malformed", source: "https://registry.example/%zz?key=query-secret#fragment-secret", message: "token registry sync failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.source
			if source == "" {
				source = "https://registry.example/registry?key=query-secret#fragment-secret"
			}
			var logs bytes.Buffer
			calls := 0
			syncer, err := NewTokenRegistrySync(TokenRegistryConfig{
				Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
				Store:  newFakeTokenRegistryStore(), SourceURL: source,
				AllowPrivateAddresses: true,
				HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					calls++
					require.Equal(t, "query-secret", req.URL.Query().Get("key"))
					if tc.transportErr {
						return nil, &url.Error{Op: "Get", URL: "https://redirect.example/?key=redirect-secret", Err: context.DeadlineExceeded}
					}
					return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(tarballOf(t, tc.files)))}, nil
				})},
			})
			require.NoError(t, err)
			syncer.runOnce(context.Background())
			output := logs.String()
			require.Contains(t, output, tc.message)
			for _, secret := range []string{"query-secret", "fragment-secret", "private-user", "private-password", "redirect-secret"} {
				require.NotContains(t, output, secret)
			}
			if tc.source == "" {
				require.Equal(t, 1, calls)
				require.Contains(t, output, "https://registry.example/registry")
			} else {
				require.Zero(t, calls)
			}
		})
	}
}

func TestTokenRegistryRedactsReturnedRequestError(t *testing.T) {
	syncer, err := NewTokenRegistrySync(TokenRegistryConfig{
		Store:                 newFakeTokenRegistryStore(),
		SourceURL:             "https://registry.example/?key=query-secret#fragment-secret",
		AllowPrivateAddresses: true,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.Join(context.DeadlineExceeded, errors.New("redirect-secret"))
		})},
	})
	require.NoError(t, err)
	_, err = syncer.SyncOnce(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotContains(t, err.Error(), "secret")
	require.True(t, strings.Contains(err.Error(), "fetch token registry"))
}

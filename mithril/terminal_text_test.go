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

package mithril

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEscapeText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"printable", "plain text é 日本", "plain text é 日本"},
		{"csi", "a\x1b[2Jb", `a\x1b[2Jb`},
		{"osc", "\x1b]0;title\x07", `\x1b]0;title\x07`},
		{"newline and tab", "a\r\nb\tc", `a\r\nb\tc`},
		{"c1 csi", "\u009b2J", `\x9b2J`},
		{"invalid utf8", "a\xffb", `a\xffb`},
		{"bidi override", "a\u202eb", `a\u202eb`},
		{"supplementary format", "\U000e0041", `\U000e0041`},
		{"nul", "a\x00b", `a\x00b`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, EscapeText(tc.in))
		})
	}
}

func TestStatusErrorEscapesAggregatorBody(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom\x1b[2J\x07"))
		},
	))
	defer server.Close()

	client := NewClient(server.URL, WithAllowInsecureHTTP())
	_, err := client.ListCardanoDatabaseSnapshots(context.Background())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "\x1b")
	assert.NotContains(t, err.Error(), "\x07")
	assert.Contains(t, err.Error(), `boom\x1b[2J\x07`)
}

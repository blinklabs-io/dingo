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

package node

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func boolPtr(v bool) *bool { return &v }

func TestResolvePeerSharing(t *testing.T) {
	tests := []struct {
		name            string
		dingoNative     *bool
		blockProducer   bool
		cardanoNode     *bool
		want            bool
		wantLogContains string
	}{
		{
			name:            "explicit true on BP warns and stays true",
			dingoNative:     boolPtr(true),
			blockProducer:   true,
			cardanoNode:     nil,
			want:            true,
			wantLogContains: "leaks topology",
		},
		{
			name:          "explicit true off BP stays true",
			dingoNative:   boolPtr(true),
			blockProducer: false,
			cardanoNode:   nil,
			want:          true,
		},
		{
			name:          "explicit false on BP stays false",
			dingoNative:   boolPtr(false),
			blockProducer: true,
			cardanoNode:   boolPtr(true),
			want:          false,
		},
		{
			name:          "explicit false off BP stays false",
			dingoNative:   boolPtr(false),
			blockProducer: false,
			cardanoNode:   boolPtr(true),
			want:          false,
		},
		{
			name:            "unset on BP defaults off, ignores cardano-node",
			dingoNative:     nil,
			blockProducer:   true,
			cardanoNode:     boolPtr(true),
			want:            false,
			wantLogContains: "disabled by default on block producer",
		},
		{
			name:            "unset off BP, cardano-node true falls through",
			dingoNative:     nil,
			blockProducer:   false,
			cardanoNode:     boolPtr(true),
			want:            true,
			wantLogContains: "cardano-node config.json PeerSharing",
		},
		{
			name:          "unset off BP, cardano-node false falls through",
			dingoNative:   nil,
			blockProducer: false,
			cardanoNode:   boolPtr(false),
			want:          false,
		},
		{
			name:          "unset off BP, cardano-node nil defaults off",
			dingoNative:   nil,
			blockProducer: false,
			cardanoNode:   nil,
			want:          false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			got := resolvePeerSharing(
				tc.dingoNative,
				tc.blockProducer,
				tc.cardanoNode,
				logger,
			)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			if tc.wantLogContains != "" &&
				!strings.Contains(buf.String(), tc.wantLogContains) {
				t.Fatalf(
					"log %q missing %q",
					buf.String(),
					tc.wantLogContains,
				)
			}
		})
	}
}

func TestResolvePeerSharingNilLogger(t *testing.T) {
	// Must not panic with nil logger on any branch.
	cases := []struct {
		dingoNative   *bool
		blockProducer bool
		cardanoNode   *bool
	}{
		{boolPtr(true), true, nil},
		{nil, true, nil},
		{nil, false, boolPtr(true)},
		{nil, false, nil},
	}
	for _, tc := range cases {
		_ = resolvePeerSharing(
			tc.dingoNative,
			tc.blockProducer,
			tc.cardanoNode,
			nil,
		)
	}
}

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

package ouroboros

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerSharingConfigSetsLocalDisabledFromNodeConfig(t *testing.T) {
	tests := []struct {
		name              string
		peerSharing       bool
		wantLocalDisabled bool
	}{
		{
			name:              "disabled",
			peerSharing:       false,
			wantLocalDisabled: true,
		},
		{
			name:              "enabled",
			peerSharing:       true,
			wantLocalDisabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NewOuroboros(OuroborosConfig{
				PeerSharing: tt.peerSharing,
			})

			cfg := o.peerSharingConfig()

			require.Equal(t, tt.wantLocalDisabled, cfg.LocalDisabled)
			require.False(t, cfg.RemoteDisabled)
			require.NotNil(t, cfg.ShareRequestFunc)
		})
	}
}

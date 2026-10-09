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

package dmq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopicNetworkMagic(t *testing.T) {
	t.Parallel()
	for network, want := range map[string]uint32{
		"mainnet": 2912307721,
		"preprod": 2147483649,
		"preview": 2147483650,
	} {
		got, ok := TopicNetworkMagic("mithril", network)
		require.True(t, ok, network)
		require.Equal(t, want, got, network)
	}
	_, ok := TopicNetworkMagic("mithril", "devnet")
	require.False(t, ok)
	_, ok = TopicNetworkMagic("unknown", "mainnet")
	require.False(t, ok)
}

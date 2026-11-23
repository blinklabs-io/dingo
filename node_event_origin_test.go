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

package dingo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventNodeIDUsesConfiguredOrigin(t *testing.T) {
	t.Parallel()

	n := &Node{config: NewConfig(WithBarkEventNodeID("relay-eu-1"))}
	require.Equal(t, "relay-eu-1", n.eventNodeID())
}

// TestEventNodeIDNeverPublishesHostname pins that an unconfigured origin
// exposes no host name, stays fixed for the node, and differs between nodes,
// since Publish refuses events whose origin equals this node's.
func TestEventNodeIDNeverPublishesHostname(t *testing.T) {
	t.Parallel()

	a := &Node{config: NewConfig()}
	b := &Node{config: NewConfig()}
	id := a.eventNodeID()

	require.NotEmpty(t, id)
	if host, err := os.Hostname(); err == nil && host != "" {
		require.NotContains(t, id, host)
	}
	require.Equal(t, id, a.eventNodeID())
	require.NotEqual(t, id, b.eventNodeID())
}

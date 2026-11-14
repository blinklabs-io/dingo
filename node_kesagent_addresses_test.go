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
	"bytes"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/stretchr/testify/require"
)

func TestPoolKeySourceBuildsAFailoverClientForAnAddressList(t *testing.T) {
	t.Parallel()
	opCertKey := &bursa.LoadedKey{
		VKey:    bytes.Repeat([]byte{1}, 32),
		RawCBOR: []byte{0x82},
	}
	n := &Node{config: Config{
		logger: slog.New(slog.NewTextHandler(new(bytes.Buffer), nil)),
	}}
	newClient := func(pool BlockProducerPool) kesAgentSignClient {
		t.Helper()
		ks := &poolKeySource{
			n:       n,
			keys:    producerKeysFromPool(pool),
			metrics: func() *kesagent.Metrics { return nil },
		}
		client, err := ks.newKESAgentSignClient(opCertKey)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })
		return client
	}

	single := newClient(BlockProducerPool{
		ShelleyKESAgentAddress:   "kes-agent-a.example:3443",
		ShelleyKESAgentPlaintext: true,
	})
	require.IsType(t, &kesagent.RemoteClient{}, single,
		"one address keeps the plain remote client")

	list := newClient(BlockProducerPool{
		ShelleyKESAgentAddresses: []string{
			"kes-agent-a.example:3443",
			"kes-agent-b.example:3443",
		},
		ShelleyKESAgentPlaintext: true,
	})
	failover, ok := list.(*kesagent.FailoverClient)
	require.True(t, ok, "an address list builds a failover client")
	require.Equal(t, "kes-agent-a.example:3443", failover.Active(),
		"the first listed agent is used first")
}

func TestNodeRemoteAgentAddresses(t *testing.T) {
	t.Parallel()
	n := &Node{}
	require.Nil(t, n.remoteAgentAddresses())
	n.config.shelleyKESAgentAddress = "a.example:1"
	require.Equal(t, []string{"a.example:1"}, n.remoteAgentAddresses())
	n.config.shelleyKESAgentAddresses = []string{"b.example:1", "c.example:1"}
	require.Equal(t,
		[]string{"b.example:1", "c.example:1"},
		n.remoteAgentAddresses(),
		"the list wins over the single address",
	)
	ks := n.primaryKeySource()
	require.Equal(t,
		[]string{"b.example:1", "c.example:1"},
		ks.keys.agentAddresses,
	)
	require.True(t, ks.kesAgentConfigured())
}

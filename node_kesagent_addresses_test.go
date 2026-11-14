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
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/bursa/kesagent/wire"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger/forging"
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

// admissionProfiles listens on plain TCP as a remote KES agent would, and
// sends the profile of every admission request it reads to the returned
// channel. It admits nobody: the connection closes after the request.
func admissionProfiles(t *testing.T) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	profiles := make(chan string, 8)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()
				if err := wire.WriteFrameLimit(conn, wire.MaxSignFrameLen, wire.Hello{
					Protocol: wire.AttestedProtocolID, Mode: wire.ModeSign,
				}); err != nil {
					return
				}
				var req wire.AttestationRequest
				if err := wire.ReadFrameLimit(
					conn, wire.MaxSignFrameLen, nil, &req,
				); err != nil {
					return
				}
				profiles <- req.Profile
			}()
		}
	}()
	return ln.Addr().String(), profiles
}

func TestPoolKeySourceSendsTheAgentProfileAtAdmission(t *testing.T) {
	t.Parallel()
	opCertKey := &bursa.LoadedKey{
		VKey:    bytes.Repeat([]byte{1}, 32),
		RawCBOR: []byte{0x82},
	}
	n := &Node{config: Config{
		logger: slog.New(slog.NewTextHandler(new(bytes.Buffer), nil)),
	}}
	prepare := func(pool BlockProducerPool) {
		t.Helper()
		ks := &poolKeySource{
			n:       n,
			keys:    producerKeysFromPool(pool),
			metrics: func() *kesagent.Metrics { return nil },
		}
		client, err := ks.newKESAgentSignClient(opCertKey)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })
		preparer, ok := client.(forging.RemoteKESPreparer)
		require.True(t, ok)
		// The listener admits nobody, so Prepare fails after the request.
		require.Error(t, preparer.Prepare())
	}
	next := func(profiles <-chan string) string {
		t.Helper()
		select {
		case p := <-profiles:
			return p
		case <-time.After(5 * time.Second):
			t.Fatal("no admission request")
			return ""
		}
	}

	addr, profiles := admissionProfiles(t)
	prepare(BlockProducerPool{
		ShelleyKESAgentAddress:   addr,
		ShelleyKESAgentPlaintext: true,
		ShelleyKESAgentProfile:   "pool-a",
	})
	require.Equal(t, "pool-a", next(profiles))

	first, firstProfiles := admissionProfiles(t)
	second, secondProfiles := admissionProfiles(t)
	prepare(BlockProducerPool{
		ShelleyKESAgentAddresses: []string{first, second},
		ShelleyKESAgentPlaintext: true,
		ShelleyKESAgentProfile:   "pool-b",
	})
	require.Equal(t, "pool-b", next(firstProfiles))
	require.Equal(t, "pool-b", next(secondProfiles))
}

func TestPoolKeySourceCarriesTheAgentProfile(t *testing.T) {
	t.Parallel()
	keys := producerKeysFromPool(BlockProducerPool{
		ShelleyKESAgentAddress: "kes-agent.example:3443",
		ShelleyKESAgentProfile: "pool-a",
	})
	require.Equal(t, "pool-a", keys.agentProfile)

	// The single-pool option reaches the first pool's key source.
	n := &Node{config: NewConfig(
		WithShelleyKESAgentRemote("kes-agent.example:3443", "", "", ""),
		WithShelleyKESAgentPlaintext(true),
		WithShelleyKESAgentProfile("pool-b"),
	)}
	require.Equal(t, "pool-b", n.primaryKeySource().keys.agentProfile)
}

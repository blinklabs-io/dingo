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

package config

import (
	"strings"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func remotePool(n string) BlockProducerPool {
	return BlockProducerPool{
		ShelleyVRFKey:                   "/keys/" + n + "/vrf.skey",
		ShelleyOperationalCertificate:   "/keys/" + n + "/node.cert",
		ShelleyKESAgentAddress:          "kes-agent.example:3443",
		ShelleyKESAgentClientCertFile:   "/keys/kes-client.crt",
		ShelleyKESAgentClientKeyFile:    "/keys/kes-client.key",
		ShelleyKESAgentServerSPKISHA256: strings.Repeat("ab", 32),
	}
}

func TestValidateBlockProducerPools(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		modify  func(c *Config)
		wantErr string
	}{
		{
			name: "two pools",
			modify: func(c *Config) {
				c.BlockProducerPools = []BlockProducerPool{
					remotePool("a"),
					{
						ShelleyVRFKey:                 "/keys/b/vrf.skey",
						ShelleyKESKey:                 "/keys/b/kes.skey",
						ShelleyOperationalCertificate: "/keys/b/node.cert",
					},
				}
			},
		},
		{
			name: "pools and top-level keys",
			modify: func(c *Config) {
				c.ShelleyVRFKey = "/keys/vrf.skey"
				c.BlockProducerPools = []BlockProducerPool{remotePool("a")}
			},
			wantErr: "blockProducerPools replaces shelleyVrfKey",
		},
		{
			name: "entry missing its opcert",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyOperationalCertificate = ""
				c.BlockProducerPools = []BlockProducerPool{remotePool("a"), b}
			},
			wantErr: "[blockProducerPools[1].shelleyOperationalCertificate]",
		},
		{
			name: "entry with a bad pin",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentServerSPKISHA256 = "abcd"
				c.BlockProducerPools = []BlockProducerPool{b}
			},
			wantErr: "blockProducerPools[0].shelleyKesAgentServerSpkiSha256 must be 64 hex characters",
		},
		{
			name: "plaintext entry with a pin",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentPlaintext = true
				c.BlockProducerPools = []BlockProducerPool{b}
			},
			wantErr: "so blockProducerPools[0].shelleyKesAgentPlaintext takes neither",
		},
		{
			name: "plaintext entry without an address",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentAddress = ""
				b.ShelleyKESAgentClientCertFile = ""
				b.ShelleyKESAgentClientKeyFile = ""
				b.ShelleyKESAgentServerSPKISHA256 = ""
				b.ShelleyKESKey = "/keys/b/kes.skey"
				b.ShelleyKESAgentPlaintext = true
				c.BlockProducerPools = []BlockProducerPool{b}
			},
			wantErr: "blockProducerPools[0].shelleyKesAgentPlaintext requires blockProducerPools[0].shelleyKesAgentAddress",
		},
		{
			name: "client certificate entry without an address",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentAddress = ""
				b.ShelleyKESAgentClientCertFile = ""
				b.ShelleyKESAgentClientKeyFile = ""
				b.ShelleyKESAgentServerSPKISHA256 = ""
				b.ShelleyKESAgentSocket = "/run/kes-agent.sock"
				b.ShelleyKESAgentClientCertFile = "/keys/kes-client.crt"
				c.BlockProducerPools = []BlockProducerPool{remotePool("a"), b}
			},
			wantErr: "blockProducerPools[1].shelleyKesAgentClientCertFile requires blockProducerPools[1].shelleyKesAgentAddress",
		},
		{
			name: "client key entry without an address",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentAddress = ""
				b.ShelleyKESAgentClientCertFile = ""
				b.ShelleyKESAgentClientKeyFile = ""
				b.ShelleyKESAgentServerSPKISHA256 = ""
				b.ShelleyKESAgentSocket = "/run/kes-agent.sock"
				b.ShelleyKESAgentClientKeyFile = "/keys/kes-client.key"
				c.BlockProducerPools = []BlockProducerPool{remotePool("a"), b}
			},
			wantErr: "blockProducerPools[1].shelleyKesAgentClientKeyFile requires blockProducerPools[1].shelleyKesAgentAddress",
		},
		{
			name: "pinned entry without an address",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentAddress = ""
				b.ShelleyKESAgentClientCertFile = ""
				b.ShelleyKESAgentClientKeyFile = ""
				b.ShelleyKESAgentServerSPKISHA256 = ""
				b.ShelleyKESAgentSocket = "/run/kes-agent.sock"
				b.ShelleyKESAgentServerSPKISHA256 = strings.Repeat("ab", 32)
				c.BlockProducerPools = []BlockProducerPool{remotePool("a"), b}
			},
			wantErr: "blockProducerPools[1].shelleyKesAgentServerSpkiSha256 requires blockProducerPools[1].shelleyKesAgentAddress",
		},
		{
			name: "entry with an out-of-range port",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentAddress = "kes-agent.example:0"
				c.BlockProducerPools = []BlockProducerPool{b}
			},
			wantErr: "blockProducerPools[0].shelleyKesAgentAddress \"kes-agent.example:0\" must be host:port",
		},
		{
			name: "entry with two KES sources",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESKey = "/keys/b/kes.skey"
				c.BlockProducerPools = []BlockProducerPool{b}
			},
			wantErr: "blockProducerPools[0].shelleyKesAgentAddress together with",
		},
		{
			name: "two entries share an opcert",
			modify: func(c *Config) {
				c.BlockProducerPools = []BlockProducerPool{
					remotePool("a"),
					remotePool("a"),
				}
			},
			wantErr: "blockProducerPools[0] and blockProducerPools[1] use the same operational certificate",
		},
		{
			name: "entry with an invalid mode",
			modify: func(c *Config) {
				b := remotePool("b")
				b.ShelleyKESAgentAddress = ""
				b.ShelleyKESAgentSocket = "/run/kes.sock"
				b.ShelleyKESAgentMode = "signing"
				c.BlockProducerPools = []BlockProducerPool{b}
			},
			wantErr: "invalid blockProducerPools[0].shelleyKesAgentMode",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validTestConfig()
			cfg.BlockProducer = true
			tc.modify(cfg)
			err := cfg.Validate(RunModeServe)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateBlockProducerPoolsRefusesLeiosWithSeveralPools(t *testing.T) {
	t.Parallel()
	const want = "more than one pool, which Leios forging and voting do not support"
	twoPools := []BlockProducerPool{remotePool("a"), remotePool("b")}

	cfg := validTestConfig()
	cfg.BlockProducer = true
	cfg.BlockProducerPools = twoPools
	require.ErrorContains(t, cfg.Validate(RunModeLeios), want)

	cfg = validTestConfig()
	cfg.BlockProducer = true
	cfg.BlockProducerPools = twoPools
	cfg.LeiosVoteSigningKeyFile = "/keys/vote.skey"
	require.ErrorContains(t, cfg.Validate(RunModeServe), want)

	cfg = validTestConfig()
	cfg.BlockProducer = true
	cfg.BlockProducerPools = twoPools
	cfg.Network = ouroboros.NetworkCardanoMusashi.Name
	cfg.NetworkMagic = ouroboros.NetworkCardanoMusashi.NetworkMagic
	require.ErrorContains(t, cfg.Validate(RunModeServe), want)

	cfg = validTestConfig()
	cfg.BlockProducer = true
	cfg.BlockProducerPools = twoPools[:1]
	require.NoError(t, cfg.Validate(RunModeLeios))
}

func TestValidateBlockProducerPoolsRefusesASharedKESAgentAddress(t *testing.T) {
	t.Parallel()
	validate := func(pools ...BlockProducerPool) error {
		cfg := validTestConfig()
		cfg.BlockProducer = true
		cfg.BlockProducerPools = pools
		return cfg.Validate(RunModeServe)
	}

	a, b := remotePool("a"), remotePool("b")
	b.ShelleyKESAgentAddress = "KES-AGENT.example:3443"
	err := validate(a, b)
	require.ErrorContains(
		t,
		err,
		"blockProducerPools[0] and blockProducerPools[1] use the same shelleyKesAgentAddress",
	)
	require.ErrorContains(
		t,
		err,
		"an attested KES agent session cannot name a profile, so Bursa allows one profile per attested listener",
	)
	require.NotContains(t, err.Error(), "bursa#")

	// The port is compared as a number: validateHostPort accepts a
	// zero-padded port, which names the same listener.
	b.ShelleyKESAgentAddress = "kes-agent.example:03443"
	require.ErrorContains(
		t,
		validate(a, b),
		"blockProducerPools[0] and blockProducerPools[1] use the same shelleyKesAgentAddress",
	)

	// An IPv6 literal is compared case-insensitively, its port as a number.
	a.ShelleyKESAgentAddress = "[2001:DB8::1]:3443"
	b.ShelleyKESAgentAddress = "[2001:db8::1]:003443"
	require.ErrorContains(
		t,
		validate(a, b),
		"blockProducerPools[0] and blockProducerPools[1] use the same shelleyKesAgentAddress",
	)
	a = remotePool("a")

	b.ShelleyKESAgentAddress = "kes-agent-b.example:3443"
	require.NoError(t, validate(a, b))

	// A local agent socket serves several pools through its profiles.
	local := func(n string) BlockProducerPool {
		return BlockProducerPool{
			ShelleyVRFKey:                 "/keys/" + n + "/vrf.skey",
			ShelleyOperationalCertificate: "/keys/" + n + "/node.cert",
			ShelleyKESAgentSocket:         "/run/kes-agent.sock",
			ShelleyKESAgentMode:           "sign",
		}
	}
	require.NoError(t, validate(local("a"), local("b")))
}

// Not t.Parallel: loadConfigThroughPipeline resets the package-level
// globalConfig.
func TestPipeline_BlockProducerPoolsFromYAML(t *testing.T) {
	cfg, err := loadConfigThroughPipeline(t, `
blockProducer: true
blockProducerPools:
  - shelleyVrfKey: /keys/a/vrf.skey
    shelleyOperationalCertificate: /keys/a/node.cert
    shelleyKesAgentAddress: kes-agent.example:3443
    shelleyKesAgentClientCertFile: /keys/kes-client.crt
    shelleyKesAgentClientKeyFile: /keys/kes-client.key
    shelleyKesAgentServerSpkiSha256: `+strings.Repeat("cd", 32)+`
  - shelleyVrfKey: /keys/b/vrf.skey
    shelleyOperationalCertificate: /keys/b/node.cert
    shelleyKesAgentSocket: /run/kes-b.sock
`, nil)
	require.NoError(t, err)
	require.Len(t, cfg.BlockProducerPools, 2)
	require.Equal(t, "sign", cfg.BlockProducerPools[0].ShelleyKESAgentMode)
	require.Equal(t, "serve-key", cfg.BlockProducerPools[1].ShelleyKESAgentMode)
	require.Equal(t, "/run/kes-b.sock", cfg.BlockProducerPools[1].ShelleyKESAgentSocket)
}

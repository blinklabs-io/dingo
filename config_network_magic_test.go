package dingo

import "testing"

// TestConfigPopulateNetworkMagicSyncsCompatField is a regression test for the
// devnet handshake blocker: a config built by network NAME (magic left 0, the
// production path from internal/node) must resolve the compat networkMagic
// field that the ouroboros handshake reads. configPopulateNetworkMagic resolves
// the canonical cfg.NetworkMagic, but the handshake reads Config.networkMagic
// (set only by syncCompatFields at construction time, before the name was
// resolved). If the compat field stays 0, gouroboros refuses every connection
// with "invalid network magic value provided: 0". This affects any network
// started by name (devnet, preview, ...), not just devnet.
func TestConfigPopulateNetworkMagicSyncsCompatField(t *testing.T) {
	cases := []struct {
		network string
		want    uint32
	}{
		{"devnet", 42},
		{"preview", 2},
	}
	for _, tc := range cases {
		t.Run(tc.network, func(t *testing.T) {
			cfg := NewConfig(WithNetwork(tc.network))
			n := &Node{config: cfg}
			if err := n.configPopulateNetworkMagic(); err != nil {
				t.Fatalf("configPopulateNetworkMagic: %v", err)
			}
			if got := n.config.cfg.NetworkMagic; got != tc.want {
				t.Fatalf("cfg.NetworkMagic = %d, want %d", got, tc.want)
			}
			// The compat field the ouroboros handshake actually reads.
			if got := n.config.networkMagic; got != tc.want {
				t.Fatalf(
					"config.networkMagic = %d, want %d (handshake would get 0)",
					got, tc.want,
				)
			}
		})
	}
}

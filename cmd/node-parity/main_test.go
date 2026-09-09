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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withGlobalFlags sets globalFlags for the duration of a test and restores
// the previous value afterward, since it is package-level mutable state
// shared with the real CLI flag parsing.
func withGlobalFlags(t *testing.T, network, dingoAddr, cardanoAddr string) {
	t.Helper()
	saved := globalFlags
	globalFlags.network = network
	globalFlags.dingoAddr = dingoAddr
	globalFlags.cardanoAddr = cardanoAddr
	t.Cleanup(func() { globalFlags = saved })
}

// TestRequireNetwork covers --network's validation: "preview" and
// "preprod" are the only two accepted values (this tool is scoped to
// testnets), an unset value is an error rather than silently defaulting to
// anything, and any other value (e.g. "mainnet") is rejected rather than
// passed through to network-magic resolution.
func TestRequireNetwork(t *testing.T) {
	t.Run("preview is valid", func(t *testing.T) {
		withGlobalFlags(t, "preview", "", "")
		got, err := requireNetwork()
		require.NoError(t, err)
		assert.Equal(t, "preview", got)
	})
	t.Run("preprod is valid", func(t *testing.T) {
		withGlobalFlags(t, "preprod", "", "")
		got, err := requireNetwork()
		require.NoError(t, err)
		assert.Equal(t, "preprod", got)
	})
	t.Run("empty is required", func(t *testing.T) {
		withGlobalFlags(t, "", "", "")
		_, err := requireNetwork()
		require.Error(t, err)
	})
	t.Run("mainnet is rejected", func(t *testing.T) {
		withGlobalFlags(t, "mainnet", "", "")
		_, err := requireNetwork()
		require.Error(t, err, "this tool is scoped to preview/preprod only")
	})
}

// TestRequireAddrs covers --dingo-addr/--cardano-addr's validation: both
// must be supplied explicitly, in any combination of missing, since this
// tool never guesses a default address for either node (unlike
// cmd/koios-parity, it does not manage node lifecycle, so there is no
// "the node it's running against" to default to).
func TestRequireAddrs(t *testing.T) {
	t.Run("both set", func(t *testing.T) {
		withGlobalFlags(t, "", "/tmp/dingo.socket", "/tmp/cardano.socket")
		require.NoError(t, requireAddrs())
	})
	t.Run("missing dingo-addr", func(t *testing.T) {
		withGlobalFlags(t, "", "", "/tmp/cardano.socket")
		require.Error(t, requireAddrs())
	})
	t.Run("missing cardano-addr", func(t *testing.T) {
		withGlobalFlags(t, "", "/tmp/dingo.socket", "")
		require.Error(t, requireAddrs())
	})
	t.Run("both missing", func(t *testing.T) {
		withGlobalFlags(t, "", "", "")
		require.Error(t, requireAddrs())
	})
}

// TestNetworkMagic covers networkMagic's resolution of a network name to
// its real Ouroboros network magic: both supported networks must resolve
// to a real (non-zero) magic, the two magics must actually differ from
// each other (a copy-paste bug that resolved both to the same value would
// silently make every cross-network comparison meaningless), and an
// unrecognized name must error rather than resolve to the zero value.
func TestNetworkMagic(t *testing.T) {
	t.Run("preview resolves", func(t *testing.T) {
		magic, err := networkMagic("preview")
		require.NoError(t, err)
		assert.NotZero(
			t,
			magic,
			"preview must have a real, non-zero network magic",
		)
	})
	t.Run("preprod resolves", func(t *testing.T) {
		magic, err := networkMagic("preprod")
		require.NoError(t, err)
		assert.NotZero(t, magic)
	})
	t.Run("preview and preprod differ", func(t *testing.T) {
		preview, err := networkMagic("preview")
		require.NoError(t, err)
		preprod, err := networkMagic("preprod")
		require.NoError(t, err)
		assert.NotEqual(t, preview, preprod)
	})
	t.Run("unknown network errors", func(t *testing.T) {
		_, err := networkMagic("mainnet-typo-xyz")
		require.Error(t, err)
	})
}

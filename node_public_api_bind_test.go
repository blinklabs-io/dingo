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
	"context"
	"io/fs"
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeRunPublicAPIsUseSharedBindAddress(t *testing.T) {
	for _, bind := range []string{"0.0.0.0", "127.0.0.2"} {
		t.Run(bind, func(t *testing.T) {
			n := newAPIPluginRuntimeNode(t)
			t.Cleanup(func() { require.NoError(t, n.Stop()) })
			if bind != "0.0.0.0" {
				WithBindAddr(bind)(&n.config)
				n.config.syncCompatFields()
			}
			probes := map[plugin.Capability]*apiLifecycleProbe{
				plugin.CapabilityAPIUtxorpc:    {},
				plugin.CapabilityAPIBlockfrost: {},
				plugin.CapabilityAPIMesh:       {},
			}
			for capability, probe := range probes {
				registerAPIProbe(
					t,
					n.pluginHost,
					capability,
					"bind-probe",
					probe,
				)
				selectAPIProbe(n, capability, "bind-probe", 18080)
			}
			// Fail after API composition, using lifecycle-only providers:
			// observe the actual dependency handoff without binding public sockets.
			n.config.blockProducer = true
			require.ErrorIs(t, n.Run(context.Background()), fs.ErrNotExist)
			for capability, probe := range probes {
				assert.Equal(t, bind, probe.host, "provider %s", capability)
			}
		})
	}
}

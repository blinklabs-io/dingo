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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProgrammaticPublicAPIAllowsAnonymousRemoteBind(t *testing.T) {
	t.Parallel()
	for _, bind := range []string{"0.0.0.0", "::", "192.0.2.10"} {
		t.Run(bind, func(t *testing.T) {
			cfg := NewConfig(
				WithStorageMode(StorageModeAPI),
				WithAPIBindAddr(bind),
				WithNetworkMagic(42),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
			)
			node := &Node{config: cfg}
			require.NoError(t, node.configValidate())
		})
	}
}

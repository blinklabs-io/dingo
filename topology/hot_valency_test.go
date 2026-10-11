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

package topology_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/require"
)

func TestTopologyHotValency(t *testing.T) {
	t.Parallel()
	for _, fields := range []string{
		`"hotValency": 1`,
		`"valency": 1`,
		`"hotValency": 0, "valency": 1`,
		`"hotValency": 1, "warmValency": 2`,
	} {
		cfg, err := topology.NewTopologyConfigFromReader(strings.NewReader(`{
			"unknownSharedField": true,
			"localRoots": [{"accessPoints": [
				{"address":"127.0.0.1","port":3001},
				{"address":"127.0.0.2","port":3001}
			], ` + fields + `}]}`))
		require.NoError(t, err)
		require.Equal(t, uint(1), cfg.LocalRoots[0].Valency)
		require.GreaterOrEqual(t, cfg.LocalRoots[0].WarmValency, uint(1))
	}
}

func TestTopologyRejectsHotValencyAboveAccessPointCount(t *testing.T) {
	t.Parallel()
	for _, hot := range []int{2, 3} {
		_, err := topology.NewTopologyConfigFromReader(strings.NewReader(fmt.Sprintf(`{
			"localRoots": [{"accessPoints": [
				{"address":"127.0.0.1","port":3001},
				{"address":"127.0.0.2","port":3001}
			], "hotValency": %d}]}`, hot)))
		if hot == 2 {
			require.NoError(t, err)
		} else {
			if err == nil {
				t.Fatal("topology accepted hotValency above the access-point count")
			}
		}
	}
}

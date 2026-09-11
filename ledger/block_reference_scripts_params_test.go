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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func TestBlockReferenceScriptsRejectNilParameters(t *testing.T) {
	state := referenceScriptStateFunc(
		func(lcommon.TransactionInput) (lcommon.Utxo, error) {
			t.Fatal("parameter rejection must precede UTxO lookup")
			return lcommon.Utxo{}, nil
		},
	)
	for _, block := range []struct {
		name  string
		value ledger.Block
	}{
		{"conway", &conway.ConwayBlock{}},
		{"dijkstra", &dijkstra.DijkstraBlock{}},
	} {
		for _, params := range []struct {
			name  string
			value lcommon.ProtocolParameters
		}{
			{"nil", nil},
			{"typed_nil_conway", (*conway.ConwayProtocolParameters)(nil)},
			{"typed_nil_dijkstra", (*dijkstra.DijkstraProtocolParameters)(nil)},
		} {
			t.Run(block.name+"/"+params.name, func(t *testing.T) {
				require.NotPanics(t, func() {
					err := validateBlockReferenceScripts(
						block.value, params.value, state,
					)
					require.Error(t, err)
				})
			})
		}
	}
}

func TestBlockReferenceScriptsAcceptParameterShapes(t *testing.T) {
	conwayParams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 10},
	}
	for _, tc := range []struct {
		name  string
		block ledger.Block
		pp    lcommon.ProtocolParameters
	}{
		{"conway", &conway.ConwayBlock{}, conwayParams},
		{"dijkstra_conway", &dijkstra.DijkstraBlock{}, conwayParams},
		{
			"dijkstra", &dijkstra.DijkstraBlock{},
			&dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: *conwayParams,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, validateBlockReferenceScripts(tc.block, tc.pp, nil))
		})
	}
}

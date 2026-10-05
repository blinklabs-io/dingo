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
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// TestDijkstraBatchCertificatesApplyInBatchOrder registers and deregisters
// one stake credential at different levels of a batch. The credential's final
// registration must follow the batch order, children first and the top level
// last, on the delta, replay and live paths alike.
func TestDijkstraBatchCertificatesApplyInBatchOrder(t *testing.T) {
	t.Parallel()
	const deposit = uint64(2_000_000)
	stakeKey := bytes.Repeat([]byte{0xc5}, 28)
	credential := []any{uint64(0), stakeKey}
	register := []any{uint64(7), credential, deposit}
	deregister := []any{uint64(8), credential, deposit}
	paths := map[string]func(
		*stateBatchHarness,
		*testing.T,
		*dijkstra.DijkstraTransaction,
	) error{
		"delta": func(h *stateBatchHarness, t *testing.T, tx *dijkstra.DijkstraTransaction) error {
			_, err := h.apply(t, tx)
			return err
		},
		"replay": func(h *stateBatchHarness, t *testing.T, tx *dijkstra.DijkstraTransaction) error {
			return h.process(t, tx, false)
		},
		"live": func(h *stateBatchHarness, t *testing.T, tx *dijkstra.DijkstraTransaction) error {
			return h.process(t, tx, true)
		},
	}
	tests := []struct {
		name       string
		registered bool
		children   [][]any
		top        []any
		registers  bool
	}{
		{
			name:      "child registers, later child deregisters",
			children:  [][]any{{register}, {deregister}},
			registers: false,
		},
		{
			name:       "child deregisters, later child registers",
			registered: true,
			children:   [][]any{{deregister}, {register}},
			registers:  true,
		},
		{
			name:      "child registers, top level deregisters",
			children:  [][]any{{register}},
			top:       []any{deregister},
			registers: false,
		},
	}
	for pathName, run := range paths {
		for _, tc := range tests {
			t.Run(pathName+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				h := newStateBatchHarness(t)
				// Certificate deposits are read from the published
				// protocol parameters.
				pparams := dijkstraTestProtocolParameters()
				pparams.KeyDeposit = uint(deposit)
				h.ls.currentPParams = pparams
				h.ls.publishSnapshotsLocked()
				if tc.registered {
					h.seedAccount(t, stakeKey, 0)
				}
				topIn := batchRef{seed: 0xc6}
				h.seedUtxo(t, topIn, 10_000_000)
				children := make([]batchLevel, 0, len(tc.children))
				for _, certs := range tc.children {
					children = append(children, batchLevel{certs: certs})
				}
				tx := buildStateBatch(t, children, batchLevel{
					inputs:  []batchRef{topIn},
					outputs: []uint64{1_000_000},
					certs:   tc.top,
				}, false)
				require.NoError(t, run(h, t, tx))
				account, err := h.db.GetAccountByCredential(
					0,
					stakeKey,
					false,
					nil,
				)
				if tc.registers {
					require.NoError(t, err)
					require.NotNil(t, account)
					require.True(t, account.Active)
					return
				}
				require.True(
					t,
					err != nil || account == nil || !account.Active,
					"credential must end deregistered, got %+v", account,
				)
			})
		}
	}
}

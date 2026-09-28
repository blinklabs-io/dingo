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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type syncProgressLedger struct {
	MeshLedgerState
	progress float64
}

func (l syncProgressLedger) SyncProgress() float64 { return l.progress }

func TestNetworkStatusUsesLedgerSyncProgress(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider bool
		progress float64
		synced   bool
	}{
		{name: "no provider"},
		{name: "unknown target", provider: true},
		{name: "behind target", provider: true, progress: 0.99},
		{name: "caught up", provider: true, progress: 1, synced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := newTestDeps()
			h := newTestHandler(t, deps, func(cfg *ServerConfig) {
				if tc.provider {
					cfg.LedgerState = syncProgressLedger{
						MeshLedgerState: deps.ledger,
						progress:        tc.progress,
					}
				}
			})
			rec := postJSON(t, h, "/network/status", NetworkRequest{
				networkIdentifierField: networkIdentifierField{
					NetworkIdentifier: testNetworkID(),
				},
			})
			resp := decodeResponse[NetworkStatusResponse](t, rec)
			require.NotNil(t, resp.SyncStatus)
			require.NotNil(t, resp.SyncStatus.Synced)
			require.Equal(t, tc.synced, *resp.SyncStatus.Synced)
		})
	}
}

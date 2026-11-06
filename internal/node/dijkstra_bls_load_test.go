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

package node

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLoadWithDBSelectsDijkstraBlsProofDomain(t *testing.T) {
	// Not t.Parallel: replaces the package-level newLedgerStateForLoad seam.
	stop := errors.New("ledger config captured")
	prior := newLedgerStateForLoad
	t.Cleanup(func() { newLedgerStateForLoad = prior })
	var captured ledger.LedgerStateConfig
	newLedgerStateForLoad = func(c ledger.LedgerStateConfig) (*ledger.LedgerState, error) { captured = c; return nil, stop }
	// LoadWithDB stops at the captured config before writing anything, so one
	// database serves every network.
	db := newTestDB(t)
	for _, tc := range []struct {
		network string
		want    lcommon.DijkstraBlsProofDomain
	}{
		{"dijkstra", lcommon.DijkstraBlsProofDomainLegacySig}, {"musashi", lcommon.DijkstraBlsProofDomainPop}, {"preview", lcommon.DijkstraBlsProofDomainPop},
	} {
		t.Run(tc.network, func(t *testing.T) {
			err := LoadWithDB(
				context.Background(),
				&config.Config{Network: tc.network},
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				"unused",
				db,
			)
			require.ErrorIs(t, err, stop)
			require.Equal(t, tc.want, captured.DijkstraBlsProofDomain)
		})
	}
}

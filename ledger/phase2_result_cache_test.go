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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestNewLedgerStateSharesPhase2ResultCache checks that a LedgerState built
// by NewLedgerState holds a phase-2 result cache, and that every LedgerView
// validation builds over it exposes that same cache to the era rules.
func TestNewLedgerStateSharesPhase2ResultCache(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	cache := ls.Phase2ResultCache()
	require.NotNil(t, cache)

	for range 2 {
		lv := &LedgerView{ls: ls}
		require.Same(t, cache, lv.Phase2ResultCache())
		var state lcommon.LedgerState = lv
		provider, ok := lcommon.UnwrapLedgerState(state).(eras.Phase2ResultCacheProvider)
		require.True(t, ok)
		require.Same(t, cache, provider.Phase2ResultCache())
	}
}

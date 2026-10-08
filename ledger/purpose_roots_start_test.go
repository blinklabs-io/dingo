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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const purposeRootStartEpoch = 10

// startWithUnseededChild starts a Mithril-bootstrapped ledger whose current
// epoch is purposeRootStartEpoch and whose only proposal chains to a parent
// that exists nowhere, expiring after expiresEpoch.
func startWithUnseededChild(
	t *testing.T,
	expiresEpoch uint64,
) (*LedgerState, error) {
	t.Helper()
	db := newTestDB(t)
	require.NoError(t, db.SetSyncState(mithrilLedgerSlotSyncKey, "42", nil))
	require.NoError(t, db.SetEpoch(
		purposeRootStartEpoch*100, purposeRootStartEpoch,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, 1000, 100, nil,
	))
	parentIdx := uint32(0)
	require.NoError(t, db.SetGovernanceProposal(context.Background(), &models.GovernanceProposal{
		TxHash:          bytes.Repeat([]byte{0x62}, 32),
		ActionType:      uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch:   purposeRootStartEpoch - 1,
		ExpiresEpoch:    expiresEpoch,
		ParentTxHash:    bytes.Repeat([]byte{0x61}, 32),
		ParentActionIdx: &parentIdx,
		ReturnAddress:   make([]byte, 29),
		AnchorHash:      make([]byte, 32),
		AddedSlot:       purposeRootStartEpoch*100 - 1,
	}, nil))
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		PromRegistry:      prometheus.NewRegistry(),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(ls.publishCancel)
	err = ls.Start(t.Context())
	t.Cleanup(func() { _ = ls.Close() })
	return ls, err
}

func TestLedgerStateStartRefusesUnseededPurposeRoot(t *testing.T) {
	t.Parallel()

	ls, err := startWithUnseededChild(t, purposeRootStartEpoch+5)
	require.ErrorIs(t, err, governance.ErrMissingEnactedRoot)
	// Node.Run registers Close only after Start succeeds, so the refusal
	// must happen before Start creates anything Close would release.
	require.Nil(t, ls.dbWorkerPool, "worker pool started before refusal")
	ls.cleanupMu.Lock()
	timer := ls.timerCleanupConsumedUtxos
	ls.cleanupMu.Unlock()
	require.Nil(t, timer, "cleanup timer scheduled before refusal")
}

// The startup check reads the active set at the latest stored epoch, the
// set the next boundary tally reads, so a proposal already past its expiry
// is not checked. The fixture cannot complete Start (it has no genesis
// block), so the assertion is that Start got past the check: the worker
// pool is created immediately after it.
func TestLedgerStateStartChecksPurposeRootsAtLoadedEpoch(t *testing.T) {
	t.Parallel()

	ls, err := startWithUnseededChild(t, purposeRootStartEpoch-5)
	require.NotErrorIs(t, err, governance.ErrMissingEnactedRoot)
	require.NotNil(t, ls.dbWorkerPool, "Start stopped before the check")
}

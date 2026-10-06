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
func startWithUnseededChild(t *testing.T, expiresEpoch uint64) error {
	t.Helper()
	db := newTestDB(t)
	require.NoError(t, db.SetSyncState(mithrilLedgerSlotSyncKey, "42", nil))
	require.NoError(t, db.SetEpoch(
		purposeRootStartEpoch*100, purposeRootStartEpoch,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, 1000, 100, nil,
	))
	parentIdx := uint32(0)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
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
	cm, err := chain.NewManager(db, nil)
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
	if err == nil {
		t.Cleanup(func() { _ = ls.Close() })
	}
	return err
}

func TestLedgerStateStartRefusesUnseededPurposeRoot(t *testing.T) {
	t.Parallel()

	err := startWithUnseededChild(t, purposeRootStartEpoch+5)
	require.ErrorIs(t, err, governance.ErrMissingEnactedRoot)
}

// The startup check reads the active set at the loaded current epoch, the
// set the next boundary tally reads, so a proposal already past its expiry
// is not checked.
func TestLedgerStateStartChecksPurposeRootsAtLoadedEpoch(t *testing.T) {
	t.Parallel()

	err := startWithUnseededChild(t, purposeRootStartEpoch-5)
	require.NotErrorIs(t, err, governance.ErrMissingEnactedRoot)
}

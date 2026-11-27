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
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// ledgerAccountExpirations opens the live UTxO set a ledger load left in
// dataDir and renders each account's expiration, keyed by credential.
func ledgerAccountExpirations(t *testing.T, dataDir string) map[string]uint64 {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: utxoset.DirFor(dataDir)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	out := map[string]uint64{}
	require.NoError(t, set.Accounts(func(a *models.Account) error {
		out[fmt.Sprintf("%d/%x", a.CredentialTag, a.StakingKey)] = a.ExpirationEpoch
		return nil
	}))
	return out
}

func indexerAccountExpirations(t *testing.T, raw *sql.DB) map[string]uint64 {
	t.Helper()
	rows, err := raw.Query(
		"SELECT credential_tag, staking_key, expiration_epoch FROM account",
	)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var tag int64
		var key []byte
		var expiration int64
		require.NoError(t, rows.Scan(&tag, &key, &expiration))
		out[fmt.Sprintf("%d/%x", tag, key)] = uint64(expiration) //nolint:gosec
	}
	require.NoError(t, rows.Err())
	return out
}

// With CIP-0163 delegator inactivity on, the accounts the IndexerDB derives
// from the preview chain expire at the epochs the ledger's accounts do:
// activation stamps every active account at the first boundary, and the
// derived facts carry the window the ledger's rules give them.
func TestDerivedInactivityEqualsTheLedgers(t *testing.T) {
	t.Parallel()
	consensus := inactivityStoreTemplate.openEquivalence(t)
	want := ledgerAccountExpirations(t, consensus.DataDir())
	require.NotEmpty(t, want)

	idx, raw := newEquivalenceIndexer(t, consensus)
	store, bf, d := deriveInactivityParts(t, idx, inactivityWindow)
	w := deriveWriterOver(
		store, indexer.NewDatabaseSource(consensus), bf, bf, d, 700,
	)
	require.NoError(t, w.CatchUp(t.Context()))

	require.Equal(t, want, indexerAccountExpirations(t, raw))
	var stamped int
	for _, expiration := range want {
		if expiration != 0 {
			stamped++
		}
	}
	require.Equal(t, len(want), stamped, "every account carries an expiration")

	// A window that expires no account within the chain leaves every other
	// fact as it is without the gate.
	golden, got := readGolden(t), factDump(t, raw)
	for _, n := range factTableNames() {
		if n == "account" {
			continue
		}
		require.Equal(t, golden[n], got[n], "table %s", n)
	}
}

// A rollback below the activation boundary undoes the one-time stamp, and
// indexing forward stamps again: the accounts then equal those the ledger
// holds. The activation marker is cleared by the rollback; with it left, the
// second pass would not stamp and the accounts would stay at the value the
// rollback recomputed.
func TestDerivedInactivityFollowsARollback(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	consensus := inactivityStoreTemplate.openEquivalence(t)
	want := ledgerAccountExpirations(t, consensus.DataDir())
	idx, raw := newEquivalenceIndexer(t, consensus)
	src := indexer.NewDatabaseSource(consensus)
	store, bf, d := deriveInactivityParts(t, idx, inactivityWindow)
	w := deriveWriterOver(store, src, bf, bf, d, 700)
	require.NoError(t, w.CatchUp(ctx))
	require.Equal(t, want, indexerAccountExpirations(t, raw))

	tip, err := consensus.GetTip(nil)
	require.NoError(t, err)
	// A point in epoch 0, before the first boundary activates the gate.
	point := pointAt(t, src, tip.Point.Slot, 1, 20)
	require.Less(t, point.Slot, uint64(86_400))
	cut := &truncatedSource{ChainSource: src, limit: point}
	require.NoError(t, deriveWriterOver(store, cut, bf, bf, d, 700).CatchUp(ctx))
	rolled := indexerAccountExpirations(t, raw)
	require.NotEmpty(t, rolled)
	for key, expiration := range rolled {
		require.NotEqual(
			t, want[key], expiration,
			"account %s keeps the stamp of the abandoned boundary", key,
		)
	}

	require.NoError(t, w.CatchUp(ctx))
	require.Equal(t, want, indexerAccountExpirations(t, raw))
}

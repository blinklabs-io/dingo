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

// Package livesettest opens the LedgerDB live UTxO set that every
// ledger.LedgerState requires, for tests that build a ledger.
package livesettest

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// Open opens an empty live UTxO set in a temporary directory and closes it
// when tb ends, after anything registered later, such as the ledger reading
// it. Until it is reconciled with db or loaded, the ledger reads its other
// ledger-state families from db.
func Open(tb testing.TB) *utxoset.Set {
	tb.Helper()
	set, err := utxoset.Open(utxoset.Config{
		Dir:          tb.TempDir(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		MemTableSize: 8 << 20,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, set.Close()) })
	return set
}

// NewFrom returns a Mem holding db's live UTxO rows, for a test that seeds
// outputs into the metadata store before it builds the ledger, as the
// startup migration moves them into the live UTxO set.
func NewFrom(tb testing.TB, db *database.Database) *Mem {
	tb.Helper()
	m := New()
	require.NoError(tb, db.IterateLiveUtxos(
		tb.Context(),
		nil,
		func(u *models.Utxo) error {
			return m.ApplyUtxoChanges(nil, []ledger.UtxoEvent{{
				Added: []ledger.UtxoOutput{{
					Ref:  models.UtxoId{Hash: u.TxId, Idx: u.OutputIdx},
					Cbor: u.Cbor,
				}},
			}})
		},
	))
	return m
}

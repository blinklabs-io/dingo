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

package ledger_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/test/livesettest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func init() {
	ledger.OpenLiveSetForTest = func(t testing.TB, db *database.Database) ledger.UtxoSetWriter {
		t.Helper()
		set := livesettest.Open(t)
		require.NoError(t, set.Reconcile(
			context.Background(), dbadapter.New(db), nil,
		))
		return set
	}
	ledger.OpenUtxoOnlyLiveSetForTest = func(
		t testing.TB, dataDir string,
	) (ledger.UtxoSetWriter, func() error) {
		t.Helper()
		set, err := utxoset.Open(utxoset.Config{
			Dir:          utxoset.DirFor(dataDir),
			Logger:       slog.New(slog.DiscardHandler),
			Only:         true,
			MemTableSize: 8 << 20,
		})
		require.NoError(t, err)
		return utxoOnlySet{set}, set.Close
	}
	ledger.NewMemLiveSetForTest = func() ledger.UtxoSetWriter {
		return livesettest.New()
	}
}

// utxoOnlySet exposes a production live set's outputs and nothing else, so a
// ledger on it keeps every other ledger-state family in the metadata store.
type utxoOnlySet struct{ set *utxoset.Set }

func (u utxoOnlySet) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	return u.set.ApplyUtxoChanges(tip, changes)
}

func (u utxoOnlySet) Get(txID []byte, index uint32) ([]byte, bool, error) {
	return u.set.Get(txID, index)
}

func (u utxoOnlySet) Sum(key []byte) (uint64, error) {
	return u.set.Sum(key)
}

func (u utxoOnlySet) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	return u.set.EachUtxo(fn)
}

func (u utxoOnlySet) RollbackUtxoSet(point ocommon.Point) (ledger.UtxoEvent, error) {
	return u.set.RollbackUtxoSet(point)
}

func (u utxoOnlySet) PreviewUtxoSetRollback(
	point ocommon.Point,
) (ledger.UtxoEvent, error) {
	return u.set.PreviewUtxoSetRollback(point)
}

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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/leiosclosurefixture"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLoadCertifiedClosureFromDatabase(t *testing.T) {
	t.Parallel()
	scenario, err := leiosclosurefixture.New()
	require.NoError(t, err)
	db := newLeiosClosureLoaderDB(t)
	scenario.StoreChain(t, db)
	src := NewDatabaseClosureSource(db)

	closure, err := LoadCertifiedClosure(t.Context(), src, scenario.Certifying)
	require.NoError(t, err)
	require.NotNil(t, closure)
	require.Equal(t, scenario.EBHash, closure.EndorserBlockHash)
	require.Equal(
		t,
		leiosclosurefixture.AnnouncingSlot,
		closure.EndorserBlockSlot,
	)
	txs, err := closure.Decode()
	require.NoError(t, err)
	require.Len(t, txs, len(scenario.Txs))
	bodies, err := closure.BodyCbors()
	require.NoError(t, err)
	for i, tx := range txs {
		require.Equal(t, scenario.Txs[i].Hash(), tx.Hash())
		// A transaction's id is the hash of its body.
		require.Equal(t, tx.Hash(), lcommon.Blake2b256Hash(bodies[i]))
	}

	// A block that certifies nothing has no closure.
	none, err := LoadCertifiedClosure(t.Context(), src, scenario.Announcing)
	require.NoError(t, err)
	require.Nil(t, none)
}

func TestLoadCertifiedClosureRejectsInvalidData(t *testing.T) {
	t.Parallel()
	scenario, err := leiosclosurefixture.New()
	require.NoError(t, err)

	t.Run("unstored endorser block", func(t *testing.T) {
		t.Parallel()
		db := newLeiosClosureLoaderDB(t)
		require.NoError(
			t,
			db.BlockCreate(leiosclosurefixture.Model(scenario.Announcing), nil),
		)
		_, err := LoadCertifiedClosure(
			t.Context(), NewDatabaseClosureSource(db), scenario.Certifying,
		)
		require.ErrorContains(t, err, "load certified endorser-block manifest")
	})
	t.Run("transaction count differs from manifest", func(t *testing.T) {
		t.Parallel()
		db := newLeiosClosureLoaderDB(t)
		require.NoError(
			t,
			db.BlockCreate(leiosclosurefixture.Model(scenario.Announcing), nil),
		)
		require.NoError(t, db.SetLeiosEB(
			leiosclosurefixture.AnnouncingSlot, scenario.EBHash.Bytes(),
			scenario.ManifestCbor, scenario.Raw[:1],
		))
		_, err := LoadCertifiedClosure(
			t.Context(), NewDatabaseClosureSource(db), scenario.Certifying,
		)
		require.ErrorContains(t, err, "transaction count mismatch")
	})
	t.Run("manifest does not hash to the announcement", func(t *testing.T) {
		t.Parallel()
		db := newLeiosClosureLoaderDB(t)
		require.NoError(
			t,
			db.BlockCreate(leiosclosurefixture.Model(scenario.Announcing), nil),
		)
		// A manifest of one transaction hashes differently from the
		// announced one.
		short := &lcommon.LeiosEndorserBlock{
			TransactionReferences: []lcommon.LeiosTransactionReference{{
				TransactionHash: lcommon.Blake2b256Hash(scenario.Raw[0]),
				TransactionSize: uint32(len(scenario.Raw[0])), //nolint:gosec
			}},
		}
		shortCbor, err := short.MarshalCBOR()
		require.NoError(t, err)
		require.NoError(t, db.SetLeiosEB(
			leiosclosurefixture.AnnouncingSlot, scenario.EBHash.Bytes(),
			shortCbor, scenario.Raw[:1],
		))
		_, err = LoadCertifiedClosure(
			t.Context(), NewDatabaseClosureSource(db), scenario.Certifying,
		)
		require.ErrorContains(t, err, "manifest hash mismatch")
	})
	t.Run(
		"transaction differs from its manifest reference",
		func(t *testing.T) {
			t.Parallel()
			db := newLeiosClosureLoaderDB(t)
			require.NoError(
				t,
				db.BlockCreate(
					leiosclosurefixture.Model(scenario.Announcing),
					nil,
				),
			)
			// The same transactions in the other order decode, but not to the
			// hashes the manifest lists.
			swapped := []cbor.RawMessage{scenario.Raw[1], scenario.Raw[0]}
			require.NoError(t, db.SetLeiosEB(
				leiosclosurefixture.AnnouncingSlot, scenario.EBHash.Bytes(),
				scenario.ManifestCbor, swapped,
			))
			closure, err := LoadCertifiedClosure(
				t.Context(), NewDatabaseClosureSource(db), scenario.Certifying,
			)
			require.NoError(t, err)
			_, err = closure.Decode()
			require.ErrorContains(t, err, "hash mismatch")
			_, err = safedecode.DijkstraBlockTransaction(swapped[0])
			require.NoError(t, err, "the transactions themselves decode")
		},
	)
}

func newLeiosClosureLoaderDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeAPI,
	})
	require.NoError(t, err)
	return db
}

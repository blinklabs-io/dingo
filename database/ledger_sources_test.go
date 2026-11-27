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

package database_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The ledger's database keeps no pool state of its own: until the ledger
// installs itself as the source, every pool read is refused rather than
// answered empty.
func TestPoolReadsOfTheLedgersDatabaseNeedASource(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ctx := t.Context()
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x7a}, 28))

	_, err = db.GetPool(ctx, pool, true, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPools(ctx, []lcommon.PoolKeyHash{pool}, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolVrfState(ctx, pool, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolByVrfKeyHash(ctx, make([]byte, 32), 1, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetFuturePoolByLeiosKey(ctx, make([]byte, 96), 1, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetActivePoolRelays(ctx, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetActivePoolKeyHashes(ctx, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolRegistrations(pool, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetPoolsRetiringAtEpoch(ctx, 3, 100, nil)
	require.ErrorIs(t, err, database.ErrNoPoolSource)
	_, err = db.GetActivePoolKeyHashesOrdered(ctx, nil)
	require.ErrorIs(t, err, database.ErrNoMetadataStore)
	_, _, err = db.GetPoolCertificateHistory(ctx, pool, nil)
	require.ErrorIs(t, err, database.ErrNoMetadataStore)
	require.ErrorIs(
		t,
		db.ImportPool(ctx, nil, &models.Pool{}, &models.PoolRegistration{}),
		database.ErrNoMetadataStore,
	)
}

// The ledger's database keeps no block nonces: a truncate with no nonce
// source installed fails rather than leave the ledger's nonces untouched.
func TestTruncateAfterSlotOfTheLedgersDatabaseNeedsANonceSource(
	t *testing.T,
) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	target := models.Block{
		ID:     1,
		Slot:   1500,
		Hash:   bytes.Repeat([]byte{0x15}, 32),
		Cbor:   []byte{0x80},
		Number: 1,
		Type:   babbage.BlockTypeBabbage,
	}
	require.NoError(t, db.BlockCreate(target, nil))

	_, _, err = db.TruncateAfterSlot(
		t.Context(), ocommon.Point{Slot: target.Slot, Hash: target.Hash}, 0, nil,
	)

	require.ErrorIs(t, err, database.ErrNoNonceSource)
}

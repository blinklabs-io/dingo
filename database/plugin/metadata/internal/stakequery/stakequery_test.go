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

package stakequery

import (
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestPoolQueryChunkSizeDefault(t *testing.T) {
	chunkSize := poolQueryChunkSize(nil)
	require.Equal(t, 800, chunkSize)

	_, cteArgs := activeDelegationCTE(nil, 0)
	const stakeQueryArgs = 2
	require.LessOrEqual(t, len(cteArgs)+stakeQueryArgs+chunkSize, 999)
}

func TestPreEpochPoolQueryChunkSizeSQLite(t *testing.T) {
	chunkSize := preEpochPoolQueryChunkSize(
		"sqlite",
		defaultPoolQueryChunkSize,
	)
	require.Equal(t, defaultPoolQueryChunkSize/2, chunkSize)
	// The query binds the pool chunk twice plus three scalar arguments.
	require.LessOrEqual(t, 2*chunkSize+3, 999)
}

func TestEffectivePoolRegistrationsForEpoch(t *testing.T) {
	db := newPoolParamsTestDB(t)
	const (
		epochStartSlot = 100
		endedEpoch     = 4
		snapshotSlot   = 199
	)

	type poolCase struct {
		name       string
		pool       byte
		wantRegID  uint
		wantOwner  byte
		setupEvent func(*testing.T, *gorm.DB, []byte)
	}
	cases := []poolCase{
		{
			name:      "existing pool keeps pre-epoch params",
			pool:      'a',
			wantRegID: 1,
			wantOwner: 1,
			setupEvent: func(t *testing.T, db *gorm.DB, pool []byte) {
				insertPoolRegistration(t, db, 1, pool, 90, 1, 1, 1)
				insertPoolRegistration(t, db, 2, pool, 120, 2, 2, 1)
			},
		},
		{
			name:      "fresh pool uses earliest in-epoch chain position",
			pool:      'b',
			wantRegID: 3,
			wantOwner: 3,
			setupEvent: func(t *testing.T, db *gorm.DB, pool []byte) {
				insertPoolRegistration(t, db, 3, pool, 110, 3, 3, 1)
				insertPoolRegistration(t, db, 4, pool, 110, 4, 4, 1)
			},
		},
		{
			name:      "executed retirement makes pool fresh",
			pool:      'c',
			wantRegID: 6,
			wantOwner: 6,
			setupEvent: func(t *testing.T, db *gorm.DB, pool []byte) {
				insertPoolRegistration(t, db, 5, pool, 70, 5, 5, 1)
				insertPoolRetirement(t, db, 1, pool, 90, endedEpoch, 6, 6, 1)
				insertPoolRegistration(t, db, 6, pool, 120, 7, 7, 1)
				insertPoolRegistration(t, db, 7, pool, 130, 8, 8, 1)
			},
		},
		{
			name:      "future retirement does not remove pre-epoch params",
			pool:      'd',
			wantRegID: 8,
			wantOwner: 8,
			setupEvent: func(t *testing.T, db *gorm.DB, pool []byte) {
				insertPoolRegistration(t, db, 8, pool, 70, 9, 9, 1)
				insertPoolRetirement(t, db, 2, pool, 90, endedEpoch+1, 10, 10, 1)
				insertPoolRegistration(t, db, 9, pool, 120, 11, 11, 1)
			},
		},
		{
			name:      "registration after retirement cancels retirement",
			pool:      'e',
			wantRegID: 10,
			wantOwner: 10,
			setupEvent: func(t *testing.T, db *gorm.DB, pool []byte) {
				insertPoolRetirement(t, db, 3, pool, 70, endedEpoch, 12, 12, 1)
				insertPoolRegistration(t, db, 10, pool, 90, 13, 13, 1)
				insertPoolRegistration(t, db, 11, pool, 120, 14, 14, 1)
			},
		},
		{
			name:      "same-slot pre-epoch cert uses block position",
			pool:      'f',
			wantRegID: 13,
			wantOwner: 13,
			setupEvent: func(t *testing.T, db *gorm.DB, pool []byte) {
				insertPoolRegistration(t, db, 12, pool, 90, 15, 15, 1)
				insertPoolRegistration(t, db, 13, pool, 90, 16, 16, 2)
				insertPoolRegistration(t, db, 14, pool, 120, 17, 17, 1)
			},
		},
	}

	pkhs := make([][]byte, 0, len(cases))
	for _, testCase := range cases {
		pool := []byte{testCase.pool}
		testCase.setupEvent(t, db, pool)
		pkhs = append(pkhs, pool)
	}
	registrations, err := EffectivePoolRegistrationsForEpoch(
		db,
		pkhs,
		epochStartSlot,
		endedEpoch,
		snapshotSlot,
	)
	require.NoError(t, err)
	require.Len(t, registrations, len(cases))

	byPool := make(map[string]models.PoolRegistration, len(registrations))
	for _, registration := range registrations {
		byPool[string(registration.PoolKeyHash)] = registration
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			registration, ok := byPool[string([]byte{testCase.pool})]
			require.True(t, ok)
			require.Equal(t, testCase.wantRegID, registration.ID)
			require.Len(t, registration.Owners, 1)
			require.Equal(
				t,
				[]byte{testCase.wantOwner},
				registration.Owners[0].KeyHash,
			)
		})
	}
}

func TestPopulatePoolRegistrationOwnersChunksSQLiteQueries(t *testing.T) {
	db := newPoolParamsTestDB(t)
	registrations := make(
		[]models.PoolRegistration,
		defaultPoolQueryChunkSize+1,
	)
	for i := range registrations {
		registrations[i].ID = uint(i + 1)
	}
	require.NoError(t, db.Exec(
		"INSERT INTO pool_registration_owner "+
			"(id, key_hash, pool_registration_id, pool_id) VALUES (?, ?, ?, ?)",
		1, []byte{1}, 1, 1,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO pool_registration_owner "+
			"(id, key_hash, pool_registration_id, pool_id) VALUES (?, ?, ?, ?)",
		2, []byte{2}, len(registrations), len(registrations),
	).Error)

	require.NoError(t, PopulatePoolRegistrationOwners(db, registrations))
	require.Equal(t, []byte{1}, registrations[0].Owners[0].KeyHash)
	require.Empty(t, registrations[1].Owners)
	require.Equal(t, []byte{2}, registrations[len(registrations)-1].Owners[0].KeyHash)
}

func newPoolParamsTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	statements := []string{
		`CREATE TABLE "transaction" (id integer PRIMARY KEY, block_index integer)`,
		`CREATE TABLE certs (id integer PRIMARY KEY, transaction_id integer, cert_index integer)`,
		`CREATE TABLE pool_registration (id integer PRIMARY KEY, pool_key_hash blob, certificate_id integer, added_slot integer)`,
		`CREATE TABLE pool_retirement (id integer PRIMARY KEY, pool_key_hash blob, certificate_id integer, added_slot integer, epoch integer)`,
		`CREATE TABLE pool_registration_owner (id integer PRIMARY KEY, key_hash blob, pool_registration_id integer, pool_id integer)`,
	}
	for _, statement := range statements {
		require.NoError(t, db.Exec(statement).Error)
	}
	return db
}

func insertPoolRegistration(
	t *testing.T,
	db *gorm.DB,
	id uint,
	pool []byte,
	slot uint64,
	certID uint,
	txID uint,
	blockIndex uint,
) {
	t.Helper()
	insertPoolCertPosition(t, db, certID, txID, blockIndex)
	require.NoError(t, db.Exec(
		"INSERT INTO pool_registration "+
			"(id, pool_key_hash, certificate_id, added_slot) VALUES (?, ?, ?, ?)",
		id, pool, certID, slot,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO pool_registration_owner "+
			"(id, key_hash, pool_registration_id, pool_id) VALUES (?, ?, ?, ?)",
		id, []byte{byte(id)}, id, id,
	).Error)
}

func insertPoolRetirement(
	t *testing.T,
	db *gorm.DB,
	id uint,
	pool []byte,
	slot uint64,
	epoch uint64,
	certID uint,
	txID uint,
	blockIndex uint,
) {
	t.Helper()
	insertPoolCertPosition(t, db, certID, txID, blockIndex)
	require.NoError(t, db.Exec(
		"INSERT INTO pool_retirement "+
			"(id, pool_key_hash, certificate_id, added_slot, epoch) "+
			"VALUES (?, ?, ?, ?, ?)",
		id, pool, certID, slot, epoch,
	).Error)
}

func insertPoolCertPosition(
	t *testing.T,
	db *gorm.DB,
	certID uint,
	txID uint,
	blockIndex uint,
) {
	t.Helper()
	require.NoError(t, db.Exec(
		`INSERT INTO "transaction" (id, block_index) VALUES (?, ?)`,
		txID, blockIndex,
	).Error, fmt.Sprintf("insert transaction %d", txID))
	require.NoError(t, db.Exec(
		"INSERT INTO certs (id, transaction_id, cert_index) VALUES (?, ?, ?)",
		certID, txID, certID,
	).Error, fmt.Sprintf("insert certificate %d", certID))
}

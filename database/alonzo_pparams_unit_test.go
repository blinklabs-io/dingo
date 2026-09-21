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

package database

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestCheckNodeSettingsRejectsUnsafeAlonzoPParamsUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		marker  string
		delete  bool
		wantErr string
	}{
		{
			name:    "legacy byte encoding",
			marker:  nodesettings.AlonzoPParamsUnitLegacyByteV0,
			wantErr: "legacy byte units",
		},
		{
			name:    "unknown encoding",
			marker:  "future-unit-v99",
			wantErr: "unknown value",
		},
		{
			name:    "missing marker",
			delete:  true,
			wantErr: "marker is missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			cfg := &Config{
				DataDir:     dataDir,
				StorageMode: "core",
				Network:     "preprod",
			}
			db, err := newTestDatabase(t, cfg)
			require.NoError(t, err)
			if tt.marker == nodesettings.AlonzoPParamsUnitLegacyByteV0 {
				require.NoError(t, db.SetPParams(
					[]byte{0x80},
					0,
					0,
					alonzo.EraIdAlonzo,
					nil,
				))
			}
			require.NoError(t, closeTestDatabase(db))

			sqlDB, err := sql.Open(
				"sqlite",
				filepath.Join(dataDir, "metadata.sqlite"),
			)
			require.NoError(t, err)
			if tt.delete {
				_, err = sqlDB.Exec(
					"DELETE FROM node_settings_gate WHERE name = ?",
					nodesettings.AlonzoPParamsUnitGateName,
				)
			} else {
				_, err = sqlDB.Exec(`
UPDATE node_settings_gate SET value = ? WHERE name = ?`,
					tt.marker, nodesettings.AlonzoPParamsUnitGateName,
				)
			}
			require.NoError(t, err)
			require.NoError(t, sqlDB.Close())

			_, err = newTestDatabase(t, cfg)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestReconcileAlonzoPParamsUnitAfterRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		marker       string
		keepAlonzo   bool
		nullEpochRow bool
		wantMarker   string
	}{
		{
			name:       "legacy row removed by recovery",
			marker:     nodesettings.AlonzoPParamsUnitLegacyByteV0,
			wantMarker: nodesettings.AlonzoPParamsUnitWordV1,
		},
		{
			name:       "legacy row survives recovery",
			marker:     nodesettings.AlonzoPParamsUnitLegacyByteV0,
			keepAlonzo: true,
			wantMarker: nodesettings.AlonzoPParamsUnitLegacyByteV0,
		},
		{
			name:         "legacy row with null epoch survives recovery",
			marker:       nodesettings.AlonzoPParamsUnitLegacyByteV0,
			nullEpochRow: true,
			wantMarker:   nodesettings.AlonzoPParamsUnitLegacyByteV0,
		},
		{
			name:       "unknown marker is not inferred",
			marker:     "future-unit-v99",
			wantMarker: "future-unit-v99",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			db, err := newTestDatabase(t, &Config{
				DataDir:     dataDir,
				StorageMode: "core",
				Network:     "preprod",
			})
			require.NoError(t, err)
			if !tt.nullEpochRow {
				require.NoError(t, db.SetPParams(
					[]byte{0x80},
					10,
					0,
					alonzo.EraIdAlonzo,
					nil,
				))
			} else {
				sqlDB, err := sql.Open(
					"sqlite",
					filepath.Join(dataDir, "metadata.sqlite"),
				)
				require.NoError(t, err)
				_, err = sqlDB.Exec(`
INSERT INTO pparams (cbor, added_slot, epoch, era_id)
VALUES (X'80', 10, NULL, ?)`, alonzo.EraIdAlonzo)
				require.NoError(t, err)
				require.NoError(t, sqlDB.Close())
			}
			require.NoError(t, db.Metadata().SetNodeSettingsGates(
				nodesettings.Values{
					nodesettings.AlonzoPParamsUnitGateName: tt.marker,
				},
				0,
				0,
			))
			if !tt.keepAlonzo && !tt.nullEpochRow {
				require.NoError(t, db.DeletePParamsAfterSlot(0, nil))
			}

			require.NoError(t, db.ReconcileAlonzoPParamsUnitAfterRecovery())
			gates, err := db.Metadata().GetNodeSettingsGates()
			require.NoError(t, err)
			require.Equal(t, tt.wantMarker,
				gates[nodesettings.AlonzoPParamsUnitGateName])
		})
	}
}

func TestRecoveryPhase1RechecksAlonzoPParamsUnit(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	cfg := &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	}
	db, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		[]byte{0x80},
		0,
		0,
		alonzo.EraIdAlonzo,
		nil,
	))
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		nodesettings.Values{
			nodesettings.AlonzoPParamsUnitGateName: nodesettings.AlonzoPParamsUnitLegacyByteV0,
		},
		0,
		0,
	))
	metaTxn := db.Metadata().Transaction(t.Context())
	require.NoError(t, db.Metadata().SetCommitTimestamp(123456789, metaTxn))
	require.NoError(t, metaTxn.Commit())
	require.NoError(t, closeTestDatabase(db))

	reopened, reopenErr := openForRecoveryTest(t, cfg)
	require.Error(t, reopenErr)
	var commitErr CommitTimestampError
	require.ErrorAs(t, reopenErr, &commitErr)
	require.NotNil(t, reopened)

	require.ErrorContains(
		t,
		reopened.CheckNodeSettings(),
		"legacy byte units",
	)
}

func TestCheckNodeSettingsRepairsStrandedLegacyMarker(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	cfg := &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
	}
	db, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		[]byte{0x80},
		10,
		0,
		alonzo.EraIdAlonzo,
		nil,
	))
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		nodesettings.Values{
			nodesettings.AlonzoPParamsUnitGateName: nodesettings.AlonzoPParamsUnitLegacyByteV0,
		},
		0,
		0,
	))
	// Model a crash after rollback's metadata truncate committed but before
	// the recovery path could rewrite the conservative marker.
	require.NoError(t, db.DeletePParamsAfterSlot(0, nil))
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeTestDatabase(reopened)) })
	gates, err := reopened.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		gates[nodesettings.AlonzoPParamsUnitGateName])
}

// alonzoPParamsUnitMarkerAt reads the unit marker straight out of a closed
// database's metadata store.
func alonzoPParamsUnitMarkerAt(t *testing.T, dataDir string) string {
	t.Helper()
	sqlDB, err := sql.Open(
		"sqlite",
		filepath.Join(dataDir, "metadata.sqlite"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	var marker string
	require.NoError(t, sqlDB.QueryRow(
		`SELECT value FROM node_settings_gate WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitGateName,
	).Scan(&marker))
	return marker
}

// alonzoPParamsCbor encodes an Alonzo protocol-parameter row holding the
// supplied key 17 value, the way the ledger persists one.
func alonzoPParamsCbor(t *testing.T, adaPerUtxoByte uint64) []byte {
	t.Helper()
	params := alonzo.AlonzoProtocolParameters{
		AdaPerUtxoByte: adaPerUtxoByte,
	}
	encoded, err := cbor.Encode(&params)
	require.NoError(t, err)
	return encoded
}

// alonzoRowKey17 reads key 17 back out of the single persisted Alonzo row.
func alonzoRowKey17(t *testing.T, dataDir string) uint64 {
	t.Helper()
	sqlDB, err := sql.Open(
		"sqlite",
		filepath.Join(dataDir, "metadata.sqlite"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	var stored []byte
	require.NoError(t, sqlDB.QueryRow(
		`SELECT cbor FROM pparams WHERE era_id = ?`,
		alonzo.EraIdAlonzo,
	).Scan(&stored))
	var params alonzo.AlonzoProtocolParameters
	_, err = cbor.Decode(stored, &params)
	require.NoError(t, err)
	return params.AdaPerUtxoByte
}

func TestRepairAlonzoPParamsUnitFromGenesis(t *testing.T) {
	t.Parallel()

	const genesisWord = 34482

	tests := []struct {
		name       string
		stored     uint64
		word       uint64
		wantMarker string
		wantKey17  uint64
		wantErr    string
	}{
		{
			name:       "lossy per-byte row is rewritten from genesis",
			stored:     genesisWord / 8,
			word:       genesisWord,
			wantMarker: nodesettings.AlonzoPParamsUnitWordV1,
			wantKey17:  genesisWord,
		},
		{
			// A crash between the row rewrite and the marker write leaves
			// this shape behind, and the next start has to finish rather
			// than demand a resync.
			name:       "already corrected row clears the marker",
			stored:     genesisWord,
			word:       genesisWord,
			wantMarker: nodesettings.AlonzoPParamsUnitWordV1,
			wantKey17:  genesisWord,
		},
		{
			name:      "chain-sourced row fails closed",
			stored:    genesisWord/8 + 1,
			word:      genesisWord,
			wantKey17: genesisWord/8 + 1,
			wantErr:   "came from an on-chain update",
		},
		{
			name:      "no genesis word supplied fails closed",
			stored:    genesisWord / 8,
			wantKey17: genesisWord / 8,
			wantErr:   "lovelacePerUTxOWord was not supplied",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			cfg := &Config{
				DataDir:                   dataDir,
				StorageMode:               "core",
				Network:                   "preprod",
				AlonzoLovelacePerUtxoWord: tt.word,
			}
			db, err := newTestDatabase(t, cfg)
			require.NoError(t, err)
			require.NoError(t, db.SetPParams(
				alonzoPParamsCbor(t, tt.stored),
				0,
				0,
				alonzo.EraIdAlonzo,
				nil,
			))
			require.NoError(t, closeTestDatabase(db))

			sqlDB, err := sql.Open(
				"sqlite",
				filepath.Join(dataDir, "metadata.sqlite"),
			)
			require.NoError(t, err)
			_, err = sqlDB.Exec(
				`UPDATE node_settings_gate SET value = ? WHERE name = ?`,
				nodesettings.AlonzoPParamsUnitLegacyByteV0,
				nodesettings.AlonzoPParamsUnitGateName,
			)
			require.NoError(t, err)
			require.NoError(t, sqlDB.Close())

			reopened, err := newTestDatabase(t, cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.ErrorContains(t, err, "resync from genesis")
				require.Equal(
					t,
					nodesettings.AlonzoPParamsUnitLegacyByteV0,
					alonzoPParamsUnitMarkerAt(t, dataDir),
				)
				require.Equal(t, tt.wantKey17, alonzoRowKey17(t, dataDir))
				return
			}
			require.NoError(t, err)
			require.Equal(
				t,
				tt.wantMarker,
				alonzoPParamsUnitMarkerAt(t, dataDir),
			)
			require.NoError(t, closeTestDatabase(reopened))
			require.Equal(t, tt.wantKey17, alonzoRowKey17(t, dataDir))
		})
	}
}

// TestRepairAlonzoPParamsUnitIsIdempotent reopens a repaired database to
// confirm the second start neither re-reports legacy units nor rewrites the
// row again.
func TestRepairAlonzoPParamsUnitIsIdempotent(t *testing.T) {
	t.Parallel()

	const genesisWord = 34482
	dataDir := t.TempDir()
	cfg := &Config{
		DataDir:                   dataDir,
		StorageMode:               "core",
		Network:                   "preprod",
		AlonzoLovelacePerUtxoWord: genesisWord,
	}
	db, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		alonzoPParamsCbor(t, genesisWord/8),
		0,
		0,
		alonzo.EraIdAlonzo,
		nil,
	))
	require.NoError(t, closeTestDatabase(db))

	sqlDB, err := sql.Open("sqlite", filepath.Join(dataDir, "metadata.sqlite"))
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`UPDATE node_settings_gate SET value = ? WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitLegacyByteV0,
		nodesettings.AlonzoPParamsUnitGateName,
	)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	for range 2 {
		reopened, err := newTestDatabase(t, cfg)
		require.NoError(t, err)
		require.NoError(t, closeTestDatabase(reopened))
		require.Equal(
			t,
			nodesettings.AlonzoPParamsUnitWordV1,
			alonzoPParamsUnitMarkerAt(t, dataDir),
		)
		require.Equal(t, uint64(genesisWord), alonzoRowKey17(t, dataDir))
	}
}

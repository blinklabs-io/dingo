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
	"bytes"
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

			sqlDB, err := openMetadataSQLite(dataDir)
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
				sqlDB, err := openMetadataSQLite(dataDir)
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
	sqlDB, err := openMetadataSQLite(dataDir)
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
	sqlDB, err := openMetadataSQLite(dataDir)
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

			sqlDB, err := openMetadataSQLite(dataDir)
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

	sqlDB, err := openMetadataSQLite(dataDir)
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

// alonzoRowCbor reads the single persisted Alonzo row's stored bytes back
// verbatim, so a test can assert the repair left them untouched rather than
// only that the decoded key 17 still reads the same.
func alonzoRowCbor(t *testing.T, dataDir string) []byte {
	t.Helper()
	sqlDB, err := openMetadataSQLite(dataDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	var stored []byte
	require.NoError(t, sqlDB.QueryRow(
		`SELECT cbor FROM pparams WHERE era_id = ?`,
		alonzo.EraIdAlonzo,
	).Scan(&stored))
	return stored
}

// TestRepairAlonzoPParamsUnitRefusesRowThatDoesNotRoundTrip pins the one
// repair property the rest of the suite cannot observe: a row that decodes
// as Alonzo, and whose key 17 holds exactly the lossy per-byte value the
// repair is looking for, is still refused when re-encoding it does not
// reproduce the stored bytes.
//
// The row here is the canonical encoding with key 17's minimal uint16 head
// widened to a non-minimal uint32 one. That is a real shape -- the value
// decodes unchanged, so every check before the round-trip gate passes -- and
// rewriting it would silently re-serialize the whole struct, replacing bytes
// this node never wrote and cannot prove equivalent. The gate exists to
// refuse that, and without it this database would be rewritten and blessed.
func TestRepairAlonzoPParamsUnitRefusesRowThatDoesNotRoundTrip(t *testing.T) {
	t.Parallel()

	const genesisWord = 34482
	canonical := alonzoPParamsCbor(t, genesisWord/8)
	minimalHead := []byte{0x19, 0x10, 0xd6}
	nonMinimalHead := []byte{0x1a, 0x00, 0x00, 0x10, 0xd6}
	require.Equal(
		t,
		1,
		bytes.Count(canonical, minimalHead),
		"key 17's minimal encoding must appear exactly once to widen it",
	)
	stored := bytes.Replace(canonical, minimalHead, nonMinimalHead, 1)
	var decoded alonzo.AlonzoProtocolParameters
	_, err := cbor.Decode(stored, &decoded)
	require.NoError(t, err, "the widened row must still decode as Alonzo")
	require.Equal(
		t,
		uint64(genesisWord/8),
		decoded.AdaPerUtxoByte,
		"the widened row must still hold the lossy per-byte value",
	)

	dataDir := t.TempDir()
	cfg := &Config{
		DataDir:                   dataDir,
		StorageMode:               "core",
		Network:                   "preprod",
		AlonzoLovelacePerUtxoWord: genesisWord,
	}
	db, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(stored, 0, 0, alonzo.EraIdAlonzo, nil))
	require.NoError(t, closeTestDatabase(db))
	require.Equal(
		t,
		stored,
		alonzoRowCbor(t, dataDir),
		"the store must keep the row's bytes verbatim for this to test anything",
	)

	sqlDB, err := openMetadataSQLite(dataDir)
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`UPDATE node_settings_gate SET value = ? WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitLegacyByteV0,
		nodesettings.AlonzoPParamsUnitGateName,
	)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = newTestDatabase(t, cfg)
	require.ErrorContains(
		t,
		err,
		"does not round-trip through the Alonzo codec",
	)
	require.ErrorContains(t, err, "resync from genesis")
	require.Equal(
		t,
		nodesettings.AlonzoPParamsUnitLegacyByteV0,
		alonzoPParamsUnitMarkerAt(t, dataDir),
	)
	require.Equal(t, stored, alonzoRowCbor(t, dataDir))
}

// TestRepairAlonzoPParamsUnitRefusesWordBelowEight pins the separability
// guard. A genesis word below 8 divides to a lossy form of 0, which a row
// legitimately holding key 17 = 0 cannot be told apart from, so the repair
// must refuse rather than rewrite it.
func TestRepairAlonzoPParamsUnitRefusesWordBelowEight(t *testing.T) {
	t.Parallel()

	const genesisWord = 4 // genesisWord / 8 == 0
	dataDir := t.TempDir()
	cfg := &Config{
		DataDir:                   dataDir,
		StorageMode:               "core",
		Network:                   "preprod",
		AlonzoLovelacePerUtxoWord: genesisWord,
	}
	db, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(
		t,
		db.SetPParams(alonzoPParamsCbor(t, 0), 0, 0, alonzo.EraIdAlonzo, nil),
	)
	require.NoError(t, closeTestDatabase(db))

	sqlDB, err := openMetadataSQLite(dataDir)
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`UPDATE node_settings_gate SET value = ? WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitLegacyByteV0,
		nodesettings.AlonzoPParamsUnitGateName,
	)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = newTestDatabase(t, cfg)
	require.ErrorContains(t, err, "is not separable from its per-byte form")
	require.ErrorContains(t, err, "resync from genesis")
	require.Equal(
		t,
		nodesettings.AlonzoPParamsUnitLegacyByteV0,
		alonzoPParamsUnitMarkerAt(t, dataDir),
		"a refused repair must leave the legacy marker in place",
	)
}

// openMetadataSQLite opens the store's metadata file for seeding or reading
// back a state the provider could not produce. Durability is irrelevant to a
// database the test discards, so synchronous=OFF skips the flush per write;
// journal_mode is left alone because the provider's WAL mode is file state.
func openMetadataSQLite(dataDir string) (*sql.DB, error) {
	return sql.Open(
		"sqlite",
		filepath.Join(dataDir, "metadata.sqlite")+
			"?_pragma=synchronous(OFF)",
	)
}

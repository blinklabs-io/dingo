// Copyright 2025 Blink Labs Software
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

package main

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/stretchr/testify/require"
)

// TestCheckResultErrOnPersistedOutcomeAlone guards against the bug where a
// fresh cached FAIL/ERROR (no epoch freshly (re)checked this run, so
// CheckResult is empty or nil) was reported as success. run.go now derives its
// exit-code input from koiosparity.EffectiveCheckOutcome(statuses, 0, 0)
// rather than the raw CheckResult returned by Check — this test exercises
// checkResultErr against exactly that kind of "zero fresh work, but persisted
// failure" result, for both FAIL and ERROR statuses.
func TestCheckResultErrOnPersistedOutcomeAlone(t *testing.T) {
	failOnly := koiosparity.EffectiveCheckOutcome(
		[]koiosparity.CheckEpochStatus{
			{Epoch: 5, Status: koiosparity.StatusFail},
		},
		0,
		0,
	)
	require.Zero(t, failOnly.EpochsChecked, "no epoch was freshly checked")
	require.Error(t, checkResultErr(failOnly))

	errorOnly := koiosparity.EffectiveCheckOutcome(
		[]koiosparity.CheckEpochStatus{
			{Epoch: 7, Status: koiosparity.StatusError},
		},
		0,
		0,
	)
	require.Error(t, checkResultErr(errorOnly))

	allPass := koiosparity.EffectiveCheckOutcome([]koiosparity.CheckEpochStatus{
		{Epoch: 1, Status: koiosparity.StatusPass},
	}, 0, 0)
	require.NoError(t, checkResultErr(allPass))
}

func TestCheckResultErrNilResult(t *testing.T) {
	require.NoError(t, checkResultErr(nil))
}

// TestDsnFromMetadataConfigPostgresProviderOnly guards against the bug where
// selecting the postgres provider with no config section at all (or one
// missing every discrete field) produced an empty DSN, which then tripped
// dingo_db.go's "--metadata-dsn is required" error even though the
// equivalent Dingo node configuration works — postgres.RegisterProvider's own
// descriptor default fills host/port/user/database/sslMode/timeZone before
// Start() ever runs. A nil cfg (provider selected, no config map produced at
// all) and an empty-but-non-nil map must both resolve to that same working
// default DSN.
func TestDsnFromMetadataConfigPostgresProviderOnly(t *testing.T) {
	want := "host=localhost user=postgres password= dbname=postgres port=5432 sslmode=disable TimeZone=UTC"

	require.Equal(t, want, dsnFromMetadataConfig("postgres", nil))
	require.Equal(t, want, dsnFromMetadataConfig("postgres", map[string]any{}))
}

// TestDsnFromMetadataConfigPostgresPartialOverride confirms a partially
// specified config layers explicit fields on top of the provider defaults
// rather than falling back to "" once any field is set.
func TestDsnFromMetadataConfigPostgresPartialOverride(t *testing.T) {
	dsn := dsnFromMetadataConfig(
		"postgres",
		map[string]any{"host": "db.example.com"},
	)
	require.Equal(
		t,
		"host=db.example.com user=postgres password= dbname=postgres port=5432 sslmode=disable TimeZone=UTC",
		dsn,
	)
}

// TestDsnFromMetadataConfigMysqlProviderOnly is the mysql counterpart of
// TestDsnFromMetadataConfigPostgresProviderOnly: selecting the mysql provider
// alone must resolve to mysql.RegisterProvider's own descriptor default
// (host=localhost, port=3306, user=root, database=dingo, timeZone=UTC) built
// via go-sql-driver/mysql's own Config/FormatDSN, matching
// database/plugin/metadata/mysql's Start() byte-for-byte, rather than an
// empty DSN.
func TestDsnFromMetadataConfigMysqlProviderOnly(t *testing.T) {
	want := "root@tcp(localhost:3306)/dingo?checkConnLiveness=false&parseTime=true&maxAllowedPacket=0&loc=UTC"

	require.Equal(t, want, dsnFromMetadataConfig("mysql", nil))
	require.Equal(t, want, dsnFromMetadataConfig("mysql", map[string]any{}))
}

// TestDsnFromMetadataConfigMysqlPartialOverride mirrors
// TestDsnFromMetadataConfigPostgresPartialOverride for mysql.
func TestDsnFromMetadataConfigMysqlPartialOverride(t *testing.T) {
	dsn := dsnFromMetadataConfig("mysql", map[string]any{"database": "myapp"})
	require.Equal(
		t,
		"root@tcp(localhost:3306)/myapp?checkConnLiveness=false&parseTime=true&maxAllowedPacket=0&loc=UTC",
		dsn,
	)
}

// TestDsnFromMetadataConfigDsnKeyTakesPrecedence confirms a flat "dsn" field
// is still used verbatim ahead of any discrete-field default, for both
// providers.
func TestDsnFromMetadataConfigDsnKeyTakesPrecedence(t *testing.T) {
	require.Equal(
		t,
		"custom-postgres-dsn",
		dsnFromMetadataConfig("postgres", map[string]any{
			"dsn": "custom-postgres-dsn", "host": "ignored",
		}),
	)
	require.Equal(
		t,
		"custom-mysql-dsn",
		dsnFromMetadataConfig("mysql", map[string]any{
			"dsn": "custom-mysql-dsn", "host": "ignored",
		}),
	)
}

// TestDsnFromMetadataConfigUnsupportedPlugin confirms sqlite (and any other
// non-postgres/mysql plugin) still resolves to "" — dsnFromMetadataConfig is
// only ever consulted for postgres/mysql; sqlite never uses a DSN.
func TestDsnFromMetadataConfigUnsupportedPlugin(t *testing.T) {
	require.Empty(t, dsnFromMetadataConfig("sqlite", nil))
	require.Empty(
		t,
		dsnFromMetadataConfig(
			"sqlite",
			map[string]any{"host": "db.example.com"},
		),
	)
}

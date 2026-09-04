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
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
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
	want := "host=localhost user=postgres password= dbname=postgres port=5432 sslmode=require TimeZone=UTC"

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
		"host=db.example.com user=postgres password= dbname=postgres port=5432 sslmode=require TimeZone=UTC",
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

// TestAccountsEnabledExplicitFlagWinsOverEnv guards against the precedence
// bug flagged in review: an explicit --accounts=false on the command line
// must win over KOIOS_PARITY_ACCOUNTS=true in the environment — a naive
// "flag value OR env value" resolution would let a true env var override an
// explicit opt-out. accountsEnabled must consult cmd.Flags().Changed
// ("accounts") to distinguish "explicitly set to false" from "left at its
// zero-value default."
func TestAccountsEnabledExplicitFlagWinsOverEnv(t *testing.T) {
	t.Setenv("KOIOS_PARITY_ACCOUNTS", "true")

	cmd := &cobra.Command{}
	addAccountsFlag(cmd)
	require.NoError(t, cmd.Flags().Set("accounts", "false"))
	require.True(t, cmd.Flags().Changed("accounts"))

	require.False(
		t,
		accountsEnabled(cmd),
		"an explicit --accounts=false must win over KOIOS_PARITY_ACCOUNTS=true",
	)
}

// TestAccountsEnabledFallsBackToEnvWhenFlagUnset proves the env var is
// consulted only when the flag was never explicitly set.
func TestAccountsEnabledFallsBackToEnvWhenFlagUnset(t *testing.T) {
	cmd := &cobra.Command{}
	addAccountsFlag(cmd)

	t.Setenv("KOIOS_PARITY_ACCOUNTS", "true")
	require.True(t, accountsEnabled(cmd))

	t.Setenv("KOIOS_PARITY_ACCOUNTS", "")
	require.False(t, accountsEnabled(cmd))
}

// TestAccountsEnabledExplicitTrueFlagWinsOverEnvFalse is the mirror case:
// an explicit --accounts=true must win even when the environment says
// otherwise (or is simply unset).
func TestAccountsEnabledExplicitTrueFlagWinsOverEnvFalse(t *testing.T) {
	cmd := &cobra.Command{}
	addAccountsFlag(cmd)
	require.NoError(t, cmd.Flags().Set("accounts", "true"))

	require.True(t, accountsEnabled(cmd))
}

// TestResolveGraceHoursRejectsNegative is a regression test for the reviewer
// finding that a negative --grace-hours reached FetchAccountRewardsForEpoch's
// zero-row/lag gate, where graceHours <= 0 disables the grace/reference-lag
// protection the same way an explicit, documented 0 does — but silently,
// without the operator ever having opted out. resolveGraceHours must reject
// it consistently with internal/config/validate.go's identical
// koiosParity.graceHours check ("must not be negative").
func TestResolveGraceHoursRejectsNegative(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Int("grace-hours", defaultGraceHours, "")
	require.NoError(t, cmd.Flags().Set("grace-hours", "-1"))

	_, err := resolveGraceHours(cmd)
	require.Error(t, err)
	require.ErrorContains(t, err, "--grace-hours must not be negative")
}

// TestResolveGraceHoursAcceptsZeroAndPositive proves resolveGraceHours only
// rejects negative values -- 0 is the documented, explicit way to disable
// the grace/reference-lag window, and any positive value is a normal
// operator-configured window; neither should be treated as invalid input.
func TestResolveGraceHoursAcceptsZeroAndPositive(t *testing.T) {
	for _, v := range []int{0, 1, defaultGraceHours, 168} {
		cmd := &cobra.Command{}
		cmd.Flags().Int("grace-hours", defaultGraceHours, "")
		require.NoError(t, cmd.Flags().Set("grace-hours", fmt.Sprintf("%d", v)))

		got, err := resolveGraceHours(cmd)
		require.NoError(t, err)
		require.Equal(t, v, got)
	}
}

// TestSubcommandsRejectNegativeGraceHours is an end-to-end regression test
// for the reviewer finding on cmd/koios-parity/fetch.go: fetch, check, run,
// and watch all expose --grace-hours (per this PR's description) and must
// all reject a negative value before it ever reaches the fetch/check
// library calls, not just fetch. Each RunE is invoked directly (bypassing
// cobra's Execute/os.Exit) with --skip-fetch/--skip-check/--interval set so
// a passing case would otherwise need network or Dingo DB access; a
// negative --grace-hours must fail before any of that is ever reached.
func TestSubcommandsRejectNegativeGraceHours(t *testing.T) {
	withGlobalFlags(t, "preview", filepath.Join(t.TempDir(), "cache.db"))

	tests := []struct {
		name string
		cmd  *cobra.Command
		run  func(cmd *cobra.Command, args []string) error
	}{
		{"fetch", fetchCommand(), fetchRun},
		{"check", checkCommand(), checkRun},
		{"run", func() *cobra.Command {
			c := &cobra.Command{Use: "run"}
			addRunFlags(c)
			return c
		}(), runCommand},
		{"watch", watchCommand(), watchRun},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.cmd.Flags().Set("grace-hours", "-1"))
			if tt.cmd.Flags().Lookup("skip-fetch") != nil {
				require.NoError(t, tt.cmd.Flags().Set("skip-fetch", "true"))
			}
			if tt.cmd.Flags().Lookup("skip-check") != nil {
				require.NoError(t, tt.cmd.Flags().Set("skip-check", "true"))
			}
			if tt.cmd.Flags().Lookup("interval") != nil {
				require.NoError(t, tt.cmd.Flags().Set("interval", "1h"))
			}
			tt.cmd.SetContext(context.Background())

			err := tt.run(tt.cmd, nil)
			require.Error(t, err)
			require.ErrorContains(t, err, "--grace-hours must not be negative")
		})
	}
}

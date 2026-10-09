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

package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDevnetValidatesRootFlagsAndStateBeforeCreatingLock(t *testing.T) {
	// Not t.Parallel: this exercises run(), which reads and replaces process globals.
	originalArgs := os.Args
	originalDevnetOptions := devnetOptions
	originalGlobalFlags := globalFlags
	originalConfigFile := configFile
	originalLogger := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() {
		os.Args = originalArgs
		devnetOptions = originalDevnetOptions
		globalFlags = originalGlobalFlags
		configFile = originalConfigFile
		slog.SetDefault(originalLogger)
	})

	tests := []struct {
		name      string
		flags     []string
		state     string
		wantError string
	}{
		{
			name:      "network flag is rejected",
			flags:     []string{"--network", "preview"},
			state:     "unmarked",
			wantError: "dingo devnet does not support root flags: --network",
		},
		{
			name:      "config flag is rejected",
			flags:     []string{"--config", "custom.yaml"},
			state:     "unmarked",
			wantError: "dingo devnet does not support root flags: --config",
		},
		{
			name:      "debug flag is accepted",
			flags:     []string{"--debug"},
			state:     "unmarked",
			wantError: "refusing to initialize non-empty directory",
		},
		{
			name:      "unknown marker is rejected before locking",
			state:     "unknown marker",
			wantError: "not a recognized dingo devnet state directory",
		},
		{
			name:      "incomplete state is rejected before locking",
			state:     "incomplete",
			wantError: "is incomplete",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output.Reset()
			runDir := t.TempDir()
			switch test.state {
			case "unmarked":
				require.NoError(t, os.WriteFile(filepath.Join(runDir, "keep.txt"), []byte("user data"), 0o600))
			case "unknown marker":
				require.NoError(t, os.WriteFile(
					filepath.Join(runDir, devnetStateMarker),
					[]byte("unknown\n"),
					0o600,
				))
			case "incomplete":
				require.NoError(t, os.WriteFile(
					filepath.Join(runDir, devnetStateMarker),
					[]byte(devnetStateVersion),
					0o600,
				))
			}

			os.Args = append([]string{"dingo"}, test.flags...)
			os.Args = append(os.Args, "devnet", "--data-dir", runDir)
			require.Equal(t, 1, run())
			require.Contains(t, output.String(), test.wantError)
			requirePathMissing(t, filepath.Join(runDir, devnetStateLock))
			if test.state == "unmarked" {
				data, err := os.ReadFile(filepath.Join(runDir, "keep.txt"))
				require.NoError(t, err)
				require.Equal(t, "user data", string(data))
			}
		})
	}
}

func TestPrepareDevnetStateRejectsIncompleteState(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	markerPath := filepath.Join(runDir, devnetStateMarker)
	require.NoError(t, os.WriteFile(markerPath, []byte(devnetStateVersion), 0o600))

	err := prepareDevnetState(runDir, true, false, time.Now())
	require.ErrorContains(t, err, "devnet state at")
	require.ErrorContains(t, err, "is incomplete")
}

func TestPrepareDevnetStateResetKeepsUnmanagedFiles(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	startTime := time.Date(2026, time.October, 8, 22, 0, 0, 0, time.UTC)
	require.NoError(t, prepareDevnetState(runDir, true, false, startTime))
	userFile := filepath.Join(runDir, "keep.txt")
	require.NoError(t, os.WriteFile(userFile, []byte("user data"), 0o600))

	require.NoError(t, prepareDevnetState(runDir, true, true, startTime.Add(time.Minute)))
	require.FileExists(t, userFile)
	data, err := os.ReadFile(userFile)
	require.NoError(t, err)
	require.Equal(t, "user data", string(data))
	require.DirExists(t, filepath.Join(runDir, "data"))
	require.FileExists(t, filepath.Join(runDir, "dingo.yaml"))
}

func TestPrepareDevnetStateRewritesPathsForCopiedDirectory(t *testing.T) {
	t.Parallel()

	sourceDir := t.TempDir()
	destinationDir := t.TempDir()
	startTime := time.Date(2026, time.October, 8, 22, 0, 0, 0, time.UTC)
	require.NoError(t, prepareDevnetState(sourceDir, true, false, startTime))
	for _, name := range []string{"vrf.skey", "kes.skey", "opcert.cert"} {
		require.FileExists(
			t,
			filepath.Join(sourceDir, "cardano", "keys", name),
		)
	}
	require.NoError(t, os.CopyFS(destinationDir, os.DirFS(sourceDir)))
	require.NoError(t, prepareDevnetState(destinationDir, true, false, startTime))

	configData, err := os.ReadFile(filepath.Join(destinationDir, "dingo.yaml"))
	require.NoError(t, err)
	var config localDevnetConfig
	require.NoError(t, yaml.Unmarshal(configData, &config))
	require.Equal(t, "dev", config.RunMode)
	require.True(t, config.BlockProducer, "the devnet uses the standard keyed block producer")
	require.Equal(t, filepath.Join(destinationDir, "data"), config.DatabasePath)
	require.Equal(t, filepath.Join(destinationDir, "cardano", "config.json"), config.CardanoConfig)
	require.Equal(t, filepath.Join(destinationDir, "cardano", "keys", "vrf.skey"), config.ShelleyVRFKey)
	require.Equal(t, filepath.Join(destinationDir, "cardano", "keys", "kes.skey"), config.ShelleyKESKey)
	require.Equal(
		t,
		filepath.Join(destinationDir, "cardano", "keys", "opcert.cert"),
		config.ShelleyOperationalCertificate,
	)
}

func TestAcquireDevnetStateLockRejectsSecondLock(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	release, err := acquireDevnetStateLock(runDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, release()) })

	_, err = acquireDevnetStateLock(runDir)
	require.ErrorIs(t, err, errDevnetStateInUse)
}

func TestReplaceJSONField(t *testing.T) {
	t.Parallel()

	byron, err := replaceJSONField([]byte(`{"startTime":0,"unchanged":true}`), "startTime", int64(1234))
	require.NoError(t, err)
	var byronDocument map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(byron, &byronDocument))
	var startTime int64
	require.NoError(t, json.Unmarshal(byronDocument["startTime"], &startTime))
	require.Equal(t, int64(1234), startTime)
	require.Equal(t, "true", string(byronDocument["unchanged"]))

	shelley, err := replaceJSONField(
		[]byte(`{"systemStart":"old","unchanged":true}`),
		"systemStart",
		"2026-10-08T22:00:00Z",
	)
	require.NoError(t, err)
	var shelleyDocument map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(shelley, &shelleyDocument))
	var systemStart string
	require.NoError(t, json.Unmarshal(shelleyDocument["systemStart"], &systemStart))
	require.Equal(t, "2026-10-08T22:00:00Z", systemStart)

	_, err = replaceJSONField([]byte(`{"other":1}`), "startTime", int64(1234))
	require.ErrorContains(t, err, `genesis JSON has no "startTime" field`)
}

func requirePathMissing(t *testing.T, path string) {
	t.Helper()
	_, err := os.Lstat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	devnetGenesisLeadTime = 30 * time.Second
	devnetStateMarker     = ".dingo-devnet-state"
	devnetStateLock       = ".dingo-devnet.lock"
	devnetStateVersion    = "dingo-devnet-state-v1\n"
)

var errDevnetStateInUse = errors.New("devnet state is already in use")

type devnetSignalCause struct {
	signal os.Signal
}

func (cause devnetSignalCause) Error() string {
	return "received " + cause.signal.String()
}

var devnetOptions struct {
	dataDir string
	reset   bool
}

type localDevnetConfig struct {
	RunMode                       string `yaml:"runMode"`
	Network                       string `yaml:"network"`
	NetworkMagic                  uint32 `yaml:"networkMagic"`
	DatabasePath                  string `yaml:"databasePath"`
	CardanoConfig                 string `yaml:"cardanoConfig"`
	BlockProducer                 bool   `yaml:"blockProducer"`
	ShelleyVRFKey                 string `yaml:"shelleyVrfKey"`
	ShelleyKESKey                 string `yaml:"shelleyKesKey"`
	ShelleyOperationalCertificate string `yaml:"shelleyOperationalCertificate"`
}

func devnetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "devnet",
		Short: "Run a single-node Cardano devnet",
		Args:  cobra.NoArgs,
		RunE:  devnetRun,
	}
	cmd.Flags().StringVar(
		&devnetOptions.dataDir,
		"data-dir",
		"",
		"persist devnet state in this directory for reuse between runs",
	)
	cmd.Flags().BoolVar(
		&devnetOptions.reset,
		"reset",
		false,
		"reset the saved devnet chain before starting",
	)
	return cmd
}

func devnetRun(cmd *cobra.Command, _ []string) (retErr error) {
	persistent := devnetOptions.dataDir != ""
	if devnetOptions.reset && !persistent {
		return errors.New("--reset requires --data-dir")
	}
	runDir, err := devnetRunDirectory(devnetOptions.dataDir)
	if err != nil {
		return err
	}
	if !persistent {
		defer func() {
			if err := os.RemoveAll(runDir); err != nil {
				retErr = errors.Join(
					retErr,
					fmt.Errorf("removing temporary devnet directory: %w", err),
				)
			}
		}()
	}
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return fmt.Errorf("creating devnet data directory: %w", err)
	}
	startTime := time.Now().UTC().Add(devnetGenesisLeadTime).Truncate(time.Second)
	releaseStateLock, err := prepareLockedDevnetState(
		runDir,
		persistent,
		devnetOptions.reset,
		startTime,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := releaseStateLock(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("releasing devnet state lock: %w", err))
		}
	}()
	configPath := filepath.Join(runDir, "dingo.yaml")

	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locating dingo executable: %w", err)
	}

	signalCtx, cancelSignal := context.WithCancelCause(cmd.Context())
	defer cancelSignal(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, devnetSignals()...)
	defer signal.Stop(signals)
	go func() {
		select {
		case received := <-signals:
			cancelSignal(devnetSignalCause{signal: received})
		case <-signalCtx.Done():
		}
	}()

	child := exec.CommandContext(
		signalCtx,
		executable,
		"--config",
		configPath,
	)
	child.Cancel = func() error {
		if child.Process == nil {
			return nil
		}
		childSignal := os.Signal(os.Interrupt)
		if cause, ok := errors.AsType[devnetSignalCause](
			context.Cause(signalCtx),
		); ok {
			childSignal = cause.signal
		}
		err := signalDevnetChild(child.Process, childSignal)
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	child.WaitDelay = time.Minute
	prepareDevnetChild(child)
	child.Env = devnetEnvironment()
	child.Stdin = cmd.InOrStdin()
	child.Stdout = cmd.OutOrStdout()
	child.Stderr = cmd.ErrOrStderr()

	dataKind := "Temporary data"
	if persistent {
		dataKind = "Persistent data"
	}
	if _, err := fmt.Fprintf(
		cmd.OutOrStdout(),
		"Starting single-node devnet (network magic 42). Press Ctrl+C to stop.\n%s: %s\n",
		dataKind,
		runDir,
	); err != nil {
		return fmt.Errorf("writing devnet startup message: %w", err)
	}
	if globalFlags.debug {
		child.Args = append(child.Args, "--debug")
	}
	if err := child.Run(); err != nil {
		if signalCtx.Err() != nil {
			return nil
		}
		return fmt.Errorf("running devnet node: %w", err)
	}
	return nil
}

func devnetRunDirectory(dataDir string) (string, error) {
	if dataDir == "" {
		runDir, err := os.MkdirTemp("", "dingo-devnet-")
		if err != nil {
			return "", fmt.Errorf("creating temporary devnet directory: %w", err)
		}
		return runDir, nil
	}
	runDir, err := filepath.Abs(dataDir)
	if err != nil {
		return "", fmt.Errorf("resolving devnet data directory: %w", err)
	}
	return runDir, nil
}

func prepareLockedDevnetState(
	runDir string,
	persistent, reset bool,
	startTime time.Time,
) (func() error, error) {
	if persistent {
		if _, err := checkDevnetStateDirectory(runDir, reset); err != nil {
			return nil, err
		}
	}
	releaseStateLock, err := acquireDevnetStateLock(runDir)
	if err != nil {
		return nil, err
	}
	if err := prepareDevnetState(runDir, persistent, reset, startTime); err != nil {
		if releaseErr := releaseStateLock(); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("releasing devnet state lock: %w", releaseErr))
		}
		return nil, err
	}
	return releaseStateLock, nil
}

func prepareDevnetState(
	runDir string,
	persistent, reset bool,
	startTime time.Time,
) error {
	if !persistent {
		if err := os.MkdirAll(runDir, 0o700); err != nil {
			return fmt.Errorf("creating temporary devnet directory: %w", err)
		}
		return initializeDevnetState(runDir, startTime)
	}
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return fmt.Errorf("creating devnet data directory: %w", err)
	}
	hasMarker, err := checkDevnetStateDirectory(runDir, reset)
	if err != nil {
		return err
	}
	if !hasMarker {
		if err := createDevnetStateMarker(filepath.Join(runDir, devnetStateMarker)); err != nil {
			return err
		}
		if !reset {
			return initializeDevnetState(runDir, startTime)
		}
	}
	if reset {
		for _, path := range []string{
			filepath.Join(runDir, "data"),
			filepath.Join(runDir, "cardano"),
			filepath.Join(runDir, "dingo.yaml"),
		} {
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("resetting devnet state at %q: %w", path, err)
			}
		}
		return initializeDevnetState(runDir, startTime)
	}
	return writeLocalDevnetConfig(
		filepath.Join(runDir, "dingo.yaml"),
		filepath.Join(runDir, "cardano"),
		filepath.Join(runDir, "data"),
	)
}

func checkDevnetStateDirectory(runDir string, reset bool) (bool, error) {
	markerPath := filepath.Join(runDir, devnetStateMarker)
	marker, err := os.ReadFile(markerPath)
	switch {
	case err == nil:
		if string(marker) != devnetStateVersion {
			return true, fmt.Errorf(
				"%q is not a recognized dingo devnet state directory",
				runDir,
			)
		}
		if reset {
			return true, nil
		}
		if err := validateDevnetState(runDir); err != nil {
			return true, fmt.Errorf(
				"devnet state at %q is incomplete; use --reset to recreate it: %w",
				runDir,
				err,
			)
		}
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		entries, readErr := os.ReadDir(runDir)
		if readErr != nil {
			return false, fmt.Errorf("reading devnet data directory: %w", readErr)
		}
		for _, entry := range entries {
			if entry.Name() == devnetStateLock {
				continue
			}
			return false, fmt.Errorf(
				"refusing to initialize non-empty directory %q without a Dingo devnet marker",
				runDir,
			)
		}
		return false, nil
	default:
		return false, fmt.Errorf("reading devnet state marker: %w", err)
	}
}

func createDevnetStateMarker(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("creating Dingo devnet state marker: %w", err)
	}
	if _, err := file.WriteString(devnetStateVersion); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing Dingo devnet state marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing Dingo devnet state marker: %w", err)
	}
	return nil
}

func validateDevnetState(runDir string) error {
	for _, path := range []struct {
		name  string
		isDir bool
	}{
		{name: filepath.Join(runDir, "data"), isDir: true},
		{name: filepath.Join(runDir, "cardano"), isDir: true},
		{name: filepath.Join(runDir, "cardano", "config.json")},
		{name: filepath.Join(runDir, "cardano", "keys", "vrf.skey")},
		{name: filepath.Join(runDir, "cardano", "keys", "kes.skey")},
		{name: filepath.Join(runDir, "cardano", "keys", "opcert.cert")},
		{name: filepath.Join(runDir, "dingo.yaml")},
	} {
		info, err := os.Stat(path.name)
		if err != nil {
			return fmt.Errorf("checking %q: %w", path.name, err)
		}
		if path.isDir && !info.IsDir() {
			return fmt.Errorf("%q is not a directory", path.name)
		}
		if !path.isDir && !info.Mode().IsRegular() {
			return fmt.Errorf("%q is not a regular file", path.name)
		}
	}
	return nil
}

func initializeDevnetState(runDir string, startTime time.Time) error {
	configDir := filepath.Join(runDir, "cardano")
	if err := materializeDevnetConfig(configDir, startTime); err != nil {
		return err
	}
	dataDir := filepath.Join(runDir, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("creating devnet database directory: %w", err)
	}
	configPath := filepath.Join(runDir, "dingo.yaml")
	if err := writeLocalDevnetConfig(configPath, configDir, dataDir); err != nil {
		return err
	}
	return nil
}

func materializeDevnetConfig(destination string, startTime time.Time) error {
	devnetFS, err := fs.Sub(cardano.EmbeddedConfigFS, "devnet")
	if err != nil {
		return fmt.Errorf("opening embedded devnet config: %w", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return fmt.Errorf("creating devnet config directory: %w", err)
	}
	if err := fs.WalkDir(devnetFS, ".", func(
		name string,
		entry fs.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if name == "." {
			return nil
		}
		path := filepath.Join(destination, filepath.FromSlash(name))
		if entry.IsDir() {
			return os.MkdirAll(path, 0o700)
		}
		data, err := fs.ReadFile(devnetFS, name)
		if err != nil {
			return fmt.Errorf("reading embedded devnet file %q: %w", name, err)
		}
		switch name {
		case "byron-genesis.json":
			data, err = replaceJSONField(data, "startTime", startTime.Unix())
		case "shelley-genesis.json":
			data, err = replaceJSONField(
				data,
				"systemStart",
				startTime.Format(time.RFC3339),
			)
		}
		if err != nil {
			return fmt.Errorf("updating devnet %s: %w", name, err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return fmt.Errorf("writing devnet file %q: %w", name, err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("materializing embedded devnet config: %w", err)
	}
	return nil
}

func replaceJSONField(data []byte, field string, value any) ([]byte, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("decoding genesis JSON: %w", err)
	}
	if _, ok := document[field]; !ok {
		return nil, fmt.Errorf("genesis JSON has no %q field", field)
	}
	encodedValue, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encoding %q: %w", field, err)
	}
	document[field] = encodedValue
	encoded, err := json.MarshalIndent(document, "", "    ")
	if err != nil {
		return nil, fmt.Errorf("encoding genesis JSON: %w", err)
	}
	return append(encoded, '\n'), nil
}

func writeLocalDevnetConfig(path, configDir, dataDir string) error {
	config := localDevnetConfig{
		RunMode:                       "dev",
		Network:                       "devnet",
		NetworkMagic:                  42,
		DatabasePath:                  dataDir,
		CardanoConfig:                 filepath.Join(configDir, "config.json"),
		BlockProducer:                 true,
		ShelleyVRFKey:                 filepath.Join(configDir, "keys", "vrf.skey"),
		ShelleyKESKey:                 filepath.Join(configDir, "keys", "kes.skey"),
		ShelleyOperationalCertificate: filepath.Join(configDir, "keys", "opcert.cert"),
	}
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("encoding devnet node config: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("writing devnet node config: %w", err)
	}
	return nil
}

func devnetEnvironment() []string {
	environment := os.Environ()
	filtered := make([]string, 0, len(environment))
	for _, value := range environment {
		name, _, _ := strings.Cut(value, "=")
		if strings.HasPrefix(name, "CARDANO_") ||
			strings.HasPrefix(name, "DINGO_") {
			continue
		}
		filtered = append(filtered, value)
	}
	return filtered
}

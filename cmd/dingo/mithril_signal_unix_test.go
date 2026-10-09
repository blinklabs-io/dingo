//go:build unix

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

const mithrilSignalHelperEnv = "DINGO_TEST_MITHRIL_SIGNAL_HELPER"

func TestMithrilSyncSignalCancelsCommand(t *testing.T) {
	if os.Getenv(mithrilSignalHelperEnv) == "1" {
		runMithrilSignalHelper(t)
		return
	}
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		os.Args[0],
		"-test.run=^TestMithrilSyncSignalCancelsCommand$",
	)
	cmd.Env = append(os.Environ(), mithrilSignalHelperEnv+"=1")
	readyReader, readyWriter, err := os.Pipe()
	require.NoError(t, err)
	defer readyReader.Close()
	defer readyWriter.Close()
	cmd.ExtraFiles = []*os.File{readyWriter}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	require.NoError(t, cmd.Start())
	defer func() {
		if cmd.ProcessState == nil {
			cancel()
			_ = cmd.Wait()
		}
	}()
	require.NoError(t, readyWriter.Close())

	ready := make(chan error, 1)
	go func() {
		line, readErr := bufio.NewReader(readyReader).ReadString('\n')
		if readErr == nil && strings.TrimSpace(line) != "MITHRIL_SIGNAL_READY" {
			readErr = fmt.Errorf("unexpected helper readiness %q", line)
		}
		ready <- readErr
	}()
	if readyErr := testutil.RequireReceive(
		t, ready, 10*time.Second, "Mithril request start",
	); readyErr != nil {
		waitErr := cmd.Wait()
		require.NoErrorf(
			t,
			readyErr,
			"helper exited before readiness: wait=%v output=%s",
			waitErr,
			output.String(),
		)
		return
	}
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, cmd.Wait(), output.String())
}

func runMithrilSignalHelper(t *testing.T) {
	readyFile := os.NewFile(3, "mithril-signal-ready")
	require.NotNil(t, readyFile)
	defer readyFile.Close()
	runMithrilSyncForCommand = func(
		ctx context.Context,
		_ *config.Config,
		_ *slog.Logger,
		_ string,
		_ *boundHealthProbe,
	) error {
		if _, err := fmt.Fprintln(
			readyFile, "MITHRIL_SIGNAL_READY",
		); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}

	cfg := &config.Config{
		Network: "preprod",
	}
	cmd := &cobra.Command{Use: "mithril-signal-helper", RunE: mithrilSyncRunE}
	cmd.SetArgs([]string{})
	cmd.SetContext(config.WithContext(context.Background(), cfg))
	require.ErrorIs(t, executeWithSignalContext(cmd), context.Canceled)
}

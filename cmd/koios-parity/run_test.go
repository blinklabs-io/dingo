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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// newRunTestCmd builds a cobra.Command wired the same way rootCmd wires
// runCommand (via addRunFlags), with --skip-fetch/--skip-check forced on so
// tests never hit the network or a real Dingo database — only the
// report-writing tail of runCommand is under test here.
func newRunTestCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "run"}
	addRunFlags(cmd)
	require.NoError(t, cmd.Flags().Set("skip-fetch", "true"))
	require.NoError(t, cmd.Flags().Set("skip-check", "true"))
	cmd.SetContext(context.Background())
	return cmd
}

// withGlobalFlags sets globalFlags.network/cachePath for the duration of a
// test and restores the zero value afterward, since globalFlags is
// package-level state shared across every subcommand.
func withGlobalFlags(t *testing.T, network, cachePath string) {
	t.Helper()
	prevNetwork := globalFlags.network
	prevCache := globalFlags.cachePath
	globalFlags.network = network
	globalFlags.cachePath = cachePath
	t.Cleanup(func() {
		globalFlags.network = prevNetwork
		globalFlags.cachePath = prevCache
	})
}

// TestRunCommandReportDirUnwritableReturnsError is a regression test for the
// reviewer finding that an unwritable/uncreatable --report-dir (os.MkdirAll
// failure) was only logged, letting runCommand return nil as long as the
// parity check itself was PASS. --report-dir here has a *file* (not a
// directory) as one of its path components, so os.MkdirAll fails
// deterministically regardless of user/permission bits (unlike chmod-based
// tricks, which root ignores).
func TestRunCommandReportDirUnwritableReturnsError(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker-file")
	require.NoError(t, os.WriteFile(blocker, []byte("not a directory"), 0o644))
	reportDir := filepath.Join(
		blocker,
		"reports",
	) // blocker is a file: MkdirAll must fail

	withGlobalFlags(t, "preview", filepath.Join(t.TempDir(), "cache.db"))
	cmd := newRunTestCmd(t)
	require.NoError(t, cmd.Flags().Set("report-dir", reportDir))

	err := runCommand(cmd, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "create report dir")
}

// TestRunCommandReportFileCreateFailureReturnsError is a regression test for
// the reviewer finding that a report-file creation failure (os.Create) was
// only logged. The target report path is pre-created as a directory, so
// os.Create deterministically fails with "is a directory" regardless of
// permission bits.
func TestRunCommandReportFileCreateFailureReturnsError(t *testing.T) {
	reportDir := t.TempDir()
	const network = "preview"
	reportFileName := fmt.Sprintf(
		"report-%s-%s.json",
		network,
		time.Now().Format("2006-01-02"),
	)
	require.NoError(
		t,
		os.Mkdir(filepath.Join(reportDir, reportFileName), 0o750),
	)

	withGlobalFlags(t, network, filepath.Join(t.TempDir(), "cache.db"))
	cmd := newRunTestCmd(t)
	require.NoError(t, cmd.Flags().Set("report-dir", reportDir))

	err := runCommand(cmd, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "create report file")
}

// TestRunCommandWritesReportAndReturnsNilOnCleanRun is a sanity check that
// the report-error propagation added by the two tests above doesn't regress
// the ordinary success path: a writable --report-dir against an empty cache
// (nothing fetched, nothing checked, so no FAIL/ERROR) must still produce a
// valid JSON report on disk and a nil error.
func TestRunCommandWritesReportAndReturnsNilOnCleanRun(t *testing.T) {
	reportDir := t.TempDir()
	const network = "preview"

	withGlobalFlags(t, network, filepath.Join(t.TempDir(), "cache.db"))
	cmd := newRunTestCmd(t)
	require.NoError(t, cmd.Flags().Set("report-dir", reportDir))

	err := runCommand(cmd, nil)
	require.NoError(t, err)

	reportFileName := fmt.Sprintf(
		"report-%s-%s.json",
		network,
		time.Now().Format("2006-01-02"),
	)
	data, readErr := os.ReadFile(filepath.Join(reportDir, reportFileName))
	require.NoError(t, readErr)
	require.Contains(t, string(data), `"network": "preview"`)
}

// fakeReportWriteCloser lets tests simulate a WriteJSONReport/Close failure
// deterministically (no real disk-full or double-close tricks required).
type fakeReportWriteCloser struct {
	buf      bytes.Buffer
	writeErr error
	closeErr error
}

func (f *fakeReportWriteCloser) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.buf.Write(p)
}

func (f *fakeReportWriteCloser) Close() error { return f.closeErr }

// TestWriteParityReportBuildFailure is a regression test for the reviewer
// finding that a BuildJSONReport failure was only logged, not returned.
// create/build are injected specifically so this (and the two tests below)
// can force each failure mode deterministically — reproducing a genuine
// BuildJSONReport/WriteJSONReport/Close failure through a live runCommand +
// cache round-trip is not practical, since the metadata migration repairs any
// missing cache table on the very next open.
func TestWriteParityReportBuildFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")
	wantErr := errors.New("build boom")

	err := writeParityReport(
		slog.New(slog.DiscardHandler),
		dir, path,
		func(p string) (io.WriteCloser, error) { return os.Create(p) },
		func() (*koiosparity.JSONReport, error) { return nil, wantErr },
	)
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "build report")
}

// TestWriteParityReportWriteFailure is a regression test for the reviewer
// finding that a WriteJSONReport failure was only logged, not returned.
func TestWriteParityReportWriteFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")
	fake := &fakeReportWriteCloser{writeErr: errors.New("write boom")}

	err := writeParityReport(
		slog.New(slog.DiscardHandler),
		dir,
		path,
		func(p string) (io.WriteCloser, error) { return fake, nil },
		func() (*koiosparity.JSONReport, error) { return &koiosparity.JSONReport{Network: "preview"}, nil },
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "write report")
}

// TestWriteParityReportCloseFailureAfterSuccessfulWrite is a regression test
// for the reviewer's "any file close error" callout: a Close failure after a
// perfectly successful build+write must still surface as a non-nil error,
// since a report that failed to flush/close cleanly cannot be trusted as
// complete on disk.
func TestWriteParityReportCloseFailureAfterSuccessfulWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")
	fake := &fakeReportWriteCloser{closeErr: errors.New("close boom")}

	err := writeParityReport(
		slog.New(slog.DiscardHandler),
		dir,
		path,
		func(p string) (io.WriteCloser, error) { return fake, nil },
		func() (*koiosparity.JSONReport, error) { return &koiosparity.JSONReport{Network: "preview"}, nil },
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "close report file")
}

// TestWriteParityReportJoinsWriteAndCloseErrors confirms a simultaneous
// write failure and close failure are combined (via errors.Join) rather than
// one silently discarding the other.
func TestWriteParityReportJoinsWriteAndCloseErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")
	fake := &fakeReportWriteCloser{
		writeErr: errors.New("write boom"),
		closeErr: errors.New("close boom"),
	}

	err := writeParityReport(
		slog.New(slog.DiscardHandler),
		dir,
		path,
		func(p string) (io.WriteCloser, error) { return fake, nil },
		func() (*koiosparity.JSONReport, error) { return &koiosparity.JSONReport{Network: "preview"}, nil },
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "write report")
	require.ErrorContains(t, err, "close report file")
}

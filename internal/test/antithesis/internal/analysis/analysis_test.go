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

package analysis

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAnalyzerRunSignalsSetupCompleteWithoutForgedBlocks(t *testing.T) {
	called := make(chan struct{}, 1)
	analyzer := NewAnalyzer(&Config{
		InitialWait:   0,
		CheckInterval: time.Hour,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	analyzer.setupComplete = func() {
		called <- struct{}{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- analyzer.Run(ctx)
	}()

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("setup complete was not signaled")
	}

	cancel()
	select {
	case err := <-runDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("analyzer did not stop after context cancellation")
	}
}

func TestChainTipRange_Empty(t *testing.T) {
	min, max, ok := chainTipRange(map[string]uint64{})
	require.False(t, ok)
	require.Zero(t, min)
	require.Zero(t, max)
}

func TestChainTipRange_NonEmpty(t *testing.T) {
	min, max, ok := chainTipRange(map[string]uint64{
		"p1": 42,
		"p2": 7,
		"p3": 99,
	})
	require.True(t, ok)
	require.Equal(t, uint64(7), min)
	require.Equal(t, uint64(99), max)
}

func TestAnalyzerReadNewLines_DiscoversNestedRotatedAndIncompleteLogs(
	t *testing.T,
) {
	logDir := t.TempDir()
	nestedDir := filepath.Join(logDir, "p1")
	require.NoError(t, os.Mkdir(nestedDir, 0o755))

	rotated := filepath.Join(logDir, "p1.log.1")
	require.NoError(t, os.WriteFile(rotated, []byte(
		`{"msg":"block produced","slot":10,"block_hash":"h10"}`+"\n"), 0o644))
	current := filepath.Join(nestedDir, "p1.log")
	require.NoError(t, os.WriteFile(current, []byte(
		`{"msg":"block produced","slot":11,"block_hash":"h11"}`), 0o644))

	analyzer := NewAnalyzer(
		&Config{LogDir: logDir},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	analyzer.readNewLines()
	snap := analyzer.metrics.Snapshot()
	require.Equal(t, 1, snap.TotalBlocksForged)
	require.Equal(t, 1, snap.BlocksByNode["p1"])
	require.Positive(t, analyzer.ingestion.nodeBytes)
	require.Len(t, analyzer.ingestion.nodeFiles, 2)

	f, err := os.OpenFile(current, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString("\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	analyzer.readNewLines()

	snap = analyzer.metrics.Snapshot()
	require.Equal(t, 2, snap.TotalBlocksForged)
	require.Equal(t, 2, snap.BlocksByNode["p1"])
}

func TestAnalyzerReadNewLines_DoesNotReprocessRenamedLog(t *testing.T) {
	logDir := t.TempDir()
	active := filepath.Join(logDir, "p1.log")
	record := func(slot int, hash string) []byte {
		return []byte(
			fmt.Sprintf(
				`{"msg":"block produced","slot":%d,"block_hash":"%s"}`+"\n",
				slot,
				hash,
			),
		)
	}
	require.NoError(t, os.WriteFile(active, record(10, "h10"), 0o644))
	analyzer := NewAnalyzer(
		&Config{LogDir: logDir},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	analyzer.readNewLines()

	require.NoError(t, os.Rename(active, active+".1"))
	require.NoError(t, os.WriteFile(active, record(11, "h11"), 0o644))
	analyzer.readNewLines()

	snap := analyzer.metrics.Snapshot()
	require.Equal(t, 2, snap.TotalBlocksForged)
	require.Equal(t, 2, snap.BlocksByNode["p1"])
}

// TestAnalyzerReadNewLines_TouchedLogIsNotReplayed reproduces a node restart:
// the entrypoint touches the log before appending, which changes its mtime
// without changing its content. Re-reading it from the start would count
// every earlier forged block again and report each as a slot regression.
func TestAnalyzerReadNewLines_TouchedLogIsNotReplayed(t *testing.T) {
	logDir := t.TempDir()
	active := filepath.Join(logDir, "p1.log")
	record := func(slot int) string {
		return fmt.Sprintf(
			`{"msg":"block produced","slot":%d,"block_hash":"h%d"}`+"\n",
			slot, slot,
		)
	}
	require.NoError(t, os.WriteFile(active, []byte(record(10)+record(11)), 0o644))
	analyzer := NewAnalyzer(
		&Config{LogDir: logDir},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	analyzer.readNewLines()

	later := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(active, later, later))
	analyzer.readNewLines()

	snap := analyzer.metrics.Snapshot()
	require.Equal(t, 2, snap.TotalBlocksForged)
	require.Empty(t, snap.SlotRegressions)
}

// TestAnalyzerReadNewLines_RewrittenLogIsReadFromStart checks that a log a
// restarted process truncates and rewrites is read from its start, even when
// the new content is no shorter than what was already consumed.
func TestAnalyzerReadNewLines_RewrittenLogIsReadFromStart(t *testing.T) {
	logDir := t.TempDir()
	active := filepath.Join(logDir, "p1.log")
	record := func(slot int) string {
		return fmt.Sprintf(
			`{"msg":"block produced","slot":%d,"block_hash":"h%d"}`+"\n",
			slot, slot,
		)
	}
	require.NoError(t, os.WriteFile(active, []byte(record(10)), 0o644))
	analyzer := NewAnalyzer(
		&Config{LogDir: logDir},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	analyzer.readNewLines()

	require.NoError(t, os.WriteFile(active, []byte(record(20)+record(21)), 0o644))
	analyzer.readNewLines()

	snap := analyzer.metrics.Snapshot()
	require.Equal(t, 3, snap.TotalBlocksForged)
	require.Equal(t, uint64(21), snap.MaxSlotByNode["p1"])
}

// TestAnalyzerReadNewLines_RewrittenLogWithSameBannerIsReadFromStart covers
// cardano-node, which truncates its log on restart and always begins it with
// the same long configuration banner.
func TestAnalyzerReadNewLines_RewrittenLogWithSameBannerIsReadFromStart(t *testing.T) {
	logDir := t.TempDir()
	active := filepath.Join(logDir, "p2.log")
	banner := "Node configuration: " + strings.Repeat("x", 600) + "\n"
	forged := func(slot int) string {
		return fmt.Sprintf(
			`{"msg":"","ns":["cardano.node.Forge"],"data":{"val":{"kind":"TraceForgedBlock","slot":%d,"block":"h%d"}}}`+"\n",
			slot, slot,
		)
	}
	require.NoError(t, os.WriteFile(active, []byte(banner+forged(10)), 0o644))
	analyzer := NewAnalyzer(
		&Config{LogDir: logDir},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	analyzer.readNewLines()

	require.NoError(t, os.WriteFile(active, []byte(banner+forged(20)+forged(21)), 0o644))
	analyzer.readNewLines()

	snap := analyzer.metrics.Snapshot()
	require.Equal(t, 3, snap.BlocksByNode["p2"])
	require.Empty(t, snap.SlotRegressions)
}

func TestLogRole_RecognizesRotationsAndRejectsUnrelatedFiles(t *testing.T) {
	role, nodeID, ok := logRole("/logs/p3.log.2")
	require.True(t, ok)
	require.Equal(t, "node", role)
	require.Equal(t, "p3", nodeID)

	role, nodeID, ok = logRole("/logs/txpump.log.1")
	require.True(t, ok)
	require.Equal(t, "txpump", role)
	require.Equal(t, "txpump", nodeID)

	_, _, ok = logRole("/logs/application.log")
	require.False(t, ok)
}

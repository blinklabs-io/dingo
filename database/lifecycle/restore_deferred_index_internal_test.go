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

package lifecycle

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// plainIndexManager implements only metadata.DeferredIndexManager: the
// fallback a store without context support takes.
type plainIndexManager struct {
	built int
}

func (m *plainIndexManager) DropDeferredIndexes() error          { return nil }
func (m *plainIndexManager) BuildCriticalDeferredIndexes() error { return nil }
func (m *plainIndexManager) BuildDeferredIndexes() error {
	m.built++
	return nil
}

func (m *plainIndexManager) HasDeferredIndexesPending() (bool, error) {
	return false, nil
}

// ctxIndexManager also implements metadata.ContextDeferredIndexBuilder and
// metadata.MissingDeferredIndexLister.
type ctxIndexManager struct {
	plainIndexManager
	missing    []string
	ctxBuilds  int
	sawErr     error
	sawListErr error
	loggedAt   string
	logBuffer  *bytes.Buffer
}

func (m *ctxIndexManager) MissingDeferredIndexes() ([]string, error) {
	return m.missing, nil
}

func (m *ctxIndexManager) MissingDeferredIndexesContext(
	ctx context.Context,
) ([]string, error) {
	m.sawListErr = ctx.Err()
	return m.missing, m.sawListErr
}

func (m *ctxIndexManager) BuildDeferredIndexesContext(
	ctx context.Context,
) error {
	m.ctxBuilds++
	if m.logBuffer != nil {
		m.loggedAt = m.logBuffer.String()
	}
	m.sawErr = ctx.Err()
	return ctx.Err()
}

type progressIndexManager struct {
	*ctxIndexManager
	progressBuilds int
}

func (m *progressIndexManager) BuildDeferredIndexesContextWithProgress(
	ctx context.Context,
	before func(string),
	after func(string, time.Duration),
) error {
	m.progressBuilds++
	before("idx_asset_fingerprint")
	after("idx_asset_fingerprint", time.Millisecond)
	return ctx.Err()
}

// TestRebuildRestoredDeferredIndexesHonoursCancellation pins the property the
// restore path's interruption-safety contract depends on: a cancelled restore
// must not be stuck behind a full manifest rebuild.
func TestRebuildRestoredDeferredIndexesHonoursCancellation(t *testing.T) {
	t.Parallel()
	manager := &ctxIndexManager{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rebuildRestoredDeferredIndexes(ctx, manager, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, manager.sawListErr, context.Canceled)
	require.Equal(t, 1, manager.ctxBuilds)
	require.Zero(
		t, manager.built,
		"the context-free build must not run when the store supports ctx",
	)
}

// TestRebuildRestoredDeferredIndexesFallsBackWithoutContextSupport keeps a
// store that implements only DeferredIndexManager working.
func TestRebuildRestoredDeferredIndexesFallsBackWithoutContextSupport(
	t *testing.T,
) {
	t.Parallel()
	manager := &plainIndexManager{}

	require.NoError(
		t,
		rebuildRestoredDeferredIndexes(context.Background(), manager, nil),
	)
	require.Equal(t, 1, manager.built)
}

// TestRebuildRestoredDeferredIndexesNamesMissingBeforeBuilding requires the
// names to reach the log before the build starts, the same ordering the
// startup repair holds: the build itself is silent for as long as it runs.
func TestRebuildRestoredDeferredIndexesNamesMissingBeforeBuilding(
	t *testing.T,
) {
	t.Parallel()
	var buf bytes.Buffer
	manager := &ctxIndexManager{
		missing:   []string{"idx_asset_fingerprint"},
		logBuffer: &buf,
	}

	require.NoError(t, rebuildRestoredDeferredIndexes(
		context.Background(),
		manager,
		slog.New(slog.NewTextHandler(&buf, nil)),
	))

	require.Contains(
		t, manager.loggedAt, "idx_asset_fingerprint",
		"the rebuild must be announced before it starts",
	)
	require.Contains(
		t, buf.String(),
		"deferred metadata index repair complete in the restored database",
	)
}

func TestRebuildRestoredDeferredIndexesLogsProgressCallbacks(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	manager := &progressIndexManager{
		ctxIndexManager: &ctxIndexManager{
			missing:   []string{"idx_asset_fingerprint"},
			logBuffer: &buf,
		},
	}
	err := rebuildRestoredDeferredIndexes(
		context.Background(),
		manager,
		slog.New(slog.NewTextHandler(&buf, nil)),
	)
	require.NoError(t, err)
	require.Equal(t, 1, manager.progressBuilds)
	require.Contains(t, buf.String(), "building deferred metadata index")
	require.Contains(t, buf.String(), "deferred metadata index ready")
}

// TestRebuildRestoredDeferredIndexesTolerateNilLogger keeps the logger
// optional: nothing else in this package logs, so callers may leave it unset.
func TestRebuildRestoredDeferredIndexesTolerateNilLogger(t *testing.T) {
	t.Parallel()
	manager := &ctxIndexManager{missing: []string{"idx_asset_fingerprint"}}

	require.NoError(t, rebuildRestoredDeferredIndexes(
		context.Background(),
		manager,
		nil,
	))
	require.Equal(t, 1, manager.ctxBuilds)
}

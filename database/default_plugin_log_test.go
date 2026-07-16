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
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNew_DefaultPluginSelectionIsLogged verifies that silently defaulting
// BlobPlugin/MetadataPlugin to badger/sqlite when unset is still observable
// via a debug log, rather than happening with zero trace.
func TestNew_DefaultPluginSelectionIsLogged(t *testing.T) {
	var logBuf bytes.Buffer
	config := &Config{
		DataDir: "", // in-memory
		Logger: slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}
	db, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "no blob plugin configured, using default") {
		t.Errorf("expected default blob plugin log, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "no metadata plugin configured, using default") {
		t.Errorf("expected default metadata plugin log, got: %s", logOutput)
	}
}

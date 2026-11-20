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

package mithril

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResumeStartImmutableHonoursRetiredBoundOnResumeOnly(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		StorageMode: "core",
	})
	require.NoError(t, err)

	start, err := resumeStartImmutable(db, syncModeResume, 0)
	require.NoError(t, err)
	assert.Zero(t, start, "no marker, no change")

	require.NoError(t, setImmutableRetiredBelow(db, 7))
	start, err = resumeStartImmutable(db, syncModeResume, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), start)
	start, err = resumeStartImmutable(db, syncModeResume, 9)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), start, "a higher catch-up start wins")
	start, err = resumeStartImmutable(db, syncModeBootstrap, 0)
	require.NoError(t, err)
	assert.Zero(t, start, "only a resume reads the marker")
}

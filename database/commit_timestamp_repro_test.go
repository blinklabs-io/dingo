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
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestCheckCommitTimestamp_FreshBlobAndMetadata verifies that a brand-new
// database opens cleanly: a missing blob commit-timestamp key must be
// treated like a missing metadata row (return 0), not as a fatal error.
func TestCheckCommitTimestamp_FreshBlobAndMetadata(t *testing.T) {
	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer closeTestDatabase(db)

	bts, bErr := db.Blob().GetCommitTimestamp()
	require.NoError(t, bErr, "blob.GetCommitTimestamp on fresh blob must not error")
	require.Equal(t, int64(0), bts, "fresh blob should report timestamp 0")
}

// TestCheckCommitTimestamp_MetadataOnly reproduces the user-reported failure
// path: metadata has a commit timestamp but the blob does not. After the
// fix, opening must surface this as a recoverable CommitTimestampError
// (so the existing recovery path in node.Run can run), not as the raw
// "blob key not found" plumbing leak.
func TestCheckCommitTimestamp_MetadataOnly(t *testing.T) {
	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	dataDir := db.config.DataDir

	metaTxn := db.Metadata().Transaction()
	require.NoError(t, db.Metadata().SetCommitTimestamp(123456789, metaTxn))
	require.NoError(t, metaTxn.Commit())
	require.NoError(t, closeTestDatabase(db))

	db2, err := newTestDatabase(t, &Config{
		DataDir: dataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	if db2 != nil {
		defer closeTestDatabase(db2)
	}
	require.Error(t, err)
	var cte CommitTimestampError
	require.ErrorAs(t, err, &cte, "expected recoverable CommitTimestampError, got: %v", err)
	require.Equal(t, int64(123456789), cte.MetadataTimestamp)
	require.Equal(t, int64(0), cte.BlobTimestamp)
	require.False(
		t,
		errors.Is(err, types.ErrBlobKeyNotFound),
		"missing blob timestamp must not surface as raw ErrBlobKeyNotFound",
	)
}

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

package dbinfo_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/stretchr/testify/require"
)

func TestWriteThenReadRoundTrips(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  1,
		MetadataPlugin: "postgres",
	}))
	info, err := dbinfo.Read(dir)
	require.NoError(t, err)
	require.Equal(t, "postgres", info.MetadataPlugin)
}

func TestReadMissingFileIsNotAnError(t *testing.T) {
	// The sidecar is advisory: a database predating it must still open.
	info, err := dbinfo.Read(t.TempDir())
	require.NoError(t, err)
	require.Empty(t, info.MetadataPlugin)
}

func TestReadRejectsUnknownFormatVersion(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  99,
		MetadataPlugin: "sqlite",
	}))
	_, err := dbinfo.Read(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "format version")
}

func TestFileNameIsStable(t *testing.T) {
	require.Equal(t, "dingo.dbinfo", dbinfo.FileName)
	require.Equal(
		t, filepath.Join("data", "dingo.dbinfo"), dbinfo.Path("data"),
	)
}

// TestWriteRejectsEmptyDataDir guards against os.CreateTemp(dataDir, ...)
// resolving to the system temp directory when dataDir is empty, while
// Path("") is the bare relative name "dingo.dbinfo" -- the rename that
// follows would then either create a stray file in the process working
// directory or fail cross-device. Every production caller derives dataDir
// from DatabasePath, so this is defence-in-depth rather than a reachable
// path today.
func TestWriteRejectsEmptyDataDir(t *testing.T) {
	err := dbinfo.Write("", dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: "sqlite",
	})
	require.Error(t, err)
}

// TestReadDetectsMissingMetadataPluginField pins the P1 fix: a sidecar
// whose JSON simply omits "metadataPlugin" decodes MetadataPlugin to its
// Go zero value, the empty string -- identical to what Read returns for a
// file that is not there at all. Read must distinguish the two via
// ErrIncompleteSidecar rather than silently returning the same "nothing to
// see" zero Info both ways.
func TestReadDetectsMissingMetadataPluginField(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, dbinfo.FileName),
		[]byte(`{"formatVersion":1}`),
		0o644,
	))
	_, err := dbinfo.Read(dir)
	require.Error(t, err)
	require.True(t, errors.Is(err, dbinfo.ErrIncompleteSidecar))
}

// TestReadDetectsNullMetadataPluginField is the same gap as the missing-
// field case above, for a sidecar that explicitly writes JSON null instead
// of omitting the key -- json.Unmarshal decodes null into the same empty-
// string zero value either way, so the fix must catch this form too.
func TestReadDetectsNullMetadataPluginField(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, dbinfo.FileName),
		[]byte(`{"formatVersion":1,"metadataPlugin":null}`),
		0o644,
	))
	_, err := dbinfo.Read(dir)
	require.Error(t, err)
	require.True(t, errors.Is(err, dbinfo.ErrIncompleteSidecar))
}

// TestReadDetectsEmptyStringMetadataPluginField is the third form of the
// same gap: a sidecar that explicitly writes "" for metadataPlugin, e.g. via
// Write called with a zero-value Info.
func TestReadDetectsEmptyStringMetadataPluginField(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: "",
	}))
	_, err := dbinfo.Read(dir)
	require.Error(t, err)
	require.True(t, errors.Is(err, dbinfo.ErrIncompleteSidecar))
}

// TestWriteThenReadStillRoundTripsWithNonEmptyPlugin guards against the
// ErrIncompleteSidecar fix being over-broad: a well-formed sidecar with a
// real plugin name must still read back cleanly with no error, exactly as
// TestWriteThenReadRoundTrips already covers -- this pins the same
// round-trip using a different plugin name so the fix's added check
// (`info.MetadataPlugin == ""`) is verified not to trip on any non-empty
// value, not just "postgres".
func TestWriteThenReadStillRoundTripsWithNonEmptyPlugin(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: "mysql",
	}))
	info, err := dbinfo.Read(dir)
	require.NoError(t, err)
	require.Equal(t, "mysql", info.MetadataPlugin)
}

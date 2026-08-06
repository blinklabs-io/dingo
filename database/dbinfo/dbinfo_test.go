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

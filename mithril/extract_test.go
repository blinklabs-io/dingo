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
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// createTestArchive creates a zstd-compressed tar archive in memory
// containing the specified files.
func createTestArchive(
	t *testing.T,
	files map[string]string,
) []byte {
	t.Helper()
	var buf bytes.Buffer

	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	tw := tar.NewWriter(zw)

	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0o640,
			Size: int64(len(content)),
		}
		err := tw.WriteHeader(hdr)
		require.NoError(t, err)
		_, err = tw.Write([]byte(content))
		require.NoError(t, err)
	}

	err = tw.Close()
	require.NoError(t, err)
	err = zw.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

func TestExtractArchive(t *testing.T) {
	files := map[string]string{
		"immutable/00000.chunk":     "chunk0 data",
		"immutable/00000.primary":   "primary0 data",
		"immutable/00000.secondary": "secondary0 data",
		"immutable/00001.chunk":     "chunk1 data",
	}

	archiveData := createTestArchive(t, files)

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, archiveData, 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extracted")
	result, err := ExtractArchive(archivePath, extractDir, nil)
	require.NoError(t, err)
	require.Equal(t, extractDir, result)

	// Verify extracted files
	for name, content := range files {
		data, err := os.ReadFile(
			filepath.Join(extractDir, name),
		)
		require.NoError(t, err)
		require.Equal(t, content, string(data))
	}
}

func TestExtractArchiveDirectoryTraversal(t *testing.T) {
	// Create an archive with a path traversal attempt
	var buf bytes.Buffer

	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	tw := tar.NewWriter(zw)

	hdr := &tar.Header{
		Name: "../../../etc/passwd",
		Mode: 0o640,
		Size: 4,
	}
	err = tw.WriteHeader(hdr)
	require.NoError(t, err)
	_, err = tw.Write([]byte("evil"))
	require.NoError(t, err)

	err = tw.Close()
	require.NoError(t, err)
	err = zw.Close()
	require.NoError(t, err)

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "evil.tar.zst")
	err = os.WriteFile(archivePath, buf.Bytes(), 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extracted")
	_, err = ExtractArchive(archivePath, extractDir, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid path")
}

func TestExtractArchiveWithDirectories(t *testing.T) {
	var buf bytes.Buffer

	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	tw := tar.NewWriter(zw)

	// Add a directory entry
	err = tw.WriteHeader(&tar.Header{
		Name:     "db/",
		Typeflag: tar.TypeDir,
		Mode:     0o750,
	})
	require.NoError(t, err)

	// Add a file in the directory
	content := "file content"
	err = tw.WriteHeader(&tar.Header{
		Name: "db/test.txt",
		Mode: 0o640,
		Size: int64(len(content)),
	})
	require.NoError(t, err)
	_, err = tw.Write([]byte(content))
	require.NoError(t, err)

	err = tw.Close()
	require.NoError(t, err)
	err = zw.Close()
	require.NoError(t, err)

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "dirs.tar.zst")
	err = os.WriteFile(archivePath, buf.Bytes(), 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extracted")
	_, err = ExtractArchive(archivePath, extractDir, nil)
	require.NoError(t, err)

	data, err := os.ReadFile(
		filepath.Join(extractDir, "db", "test.txt"),
	)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}

func TestValidRelPath(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		valid bool
	}{
		{"normal file", "immutable/00000.chunk", true},
		{"nested path", "db/immutable/00000.chunk", true},
		{"parent traversal", "../etc/passwd", false},
		{"embedded traversal", "foo/../bar", false},
		{"standalone dotdot", "..", false},
		{"trailing dotdot", "foo/..", false},
		{"absolute path", "/etc/passwd", false},
		{"backslash", `foo\bar`, false},
		{"empty", "", false},
		{"dot", ".", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.valid, validRelPath(tt.path))
		})
	}
}

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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrSnapshotPayloadMismatch marks a snapshot backup file whose size or digest
// differs from what the manifest declares.
var ErrSnapshotPayloadMismatch = errors.New(
	"snapshot payload does not match its manifest",
)

// payloadFile is one backup file a manifest declares.
type payloadFile struct {
	name   string
	size   int64
	sha256 string
}

// payloadFiles returns the backup files m declares. A snapshot holds exactly
// these two beside its manifest; a restore fetches nothing else.
func (m Manifest) payloadFiles() []payloadFile {
	return []payloadFile{
		{BlobBackupFileName, m.BlobBytes, m.BlobSHA256},
		{MetadataBackupFileName, m.MetadataBytes, m.MetadataSHA256},
	}
}

// payloadDownloads returns the cloud objects to fetch for m, each bounded by
// the size m declares.
func (m Manifest) payloadDownloads() []DownloadFile {
	files := m.payloadFiles()
	downloads := make([]DownloadFile, 0, len(files))
	for _, f := range files {
		downloads = append(
			downloads,
			DownloadFile{Name: f.name, MaxBytes: f.size},
		)
	}
	return downloads
}

// hashFile returns the hex SHA-256 digest of the file at path and the number
// of bytes read.
func hashFile(path string) (digest string, size int64, err error) {
	return hashFileContext(context.Background(), path)
}

func hashFileContext(ctx context.Context, path string) (digest string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	size, err = io.Copy(h, payloadContextReader{ctx: ctx, reader: f})
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// verifyPayloads checks the backup files in dir against m's declared sizes and
// digests. requireDigests rejects a manifest that declares no digest, which is
// how a trust-keyed restore refuses a manifest that cannot vouch for its
// payloads. A declared digest is checked against the bytes actually read, so
// the size check needs no separate stat.
func (m Manifest) verifyPayloads(dir string, requireDigests bool) error {
	for _, f := range m.payloadFiles() {
		path := filepath.Join(dir, f.name)
		if f.sha256 == "" {
			if requireDigests {
				return fmt.Errorf(
					"%w: manifest declares no digest for %s",
					ErrSnapshotPayloadMismatch, f.name,
				)
			}
			info, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("snapshot payload %q: %w", f.name, err)
			}
			if info.Size() != f.size {
				return fmt.Errorf(
					"%w: %s is %d bytes, manifest declares %d",
					ErrSnapshotPayloadMismatch, f.name, info.Size(), f.size,
				)
			}
			continue
		}
		got, size, err := hashFile(path)
		if err != nil {
			return fmt.Errorf("hash snapshot payload %q: %w", f.name, err)
		}
		if size != f.size {
			return fmt.Errorf(
				"%w: %s is %d bytes, manifest declares %d",
				ErrSnapshotPayloadMismatch, f.name, size, f.size,
			)
		}
		if !strings.EqualFold(got, f.sha256) {
			return fmt.Errorf(
				"%w: %s digest %s, manifest declares %s",
				ErrSnapshotPayloadMismatch, f.name, got, f.sha256,
			)
		}
	}
	return nil
}

// payloadContextReader checks cancellation between bounded file reads.
type payloadContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r payloadContextReader) Read(buf []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buf)
}

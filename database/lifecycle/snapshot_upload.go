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

type snapshotUploadFile struct {
	name string
	file *os.File
}

// openSnapshotUploadFiles opens exactly the files declared by a snapshot,
// with the manifest last so it remains the remote completion marker. The root
// and each file are held by handle while names are checked, so replacing the
// directory or an entry cannot redirect a later upload.
func openSnapshotUploadFiles(
	ctx context.Context,
	dir string,
	maxManifestBytes int64,
	opts ...ManifestOption,
) (_ []snapshotUploadFile, _ Manifest, retErr error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, Manifest{}, fmt.Errorf("open snapshot directory %q: %w", dir, err)
	}
	var files []snapshotUploadFile
	defer func() {
		retErr = errors.Join(retErr, root.Close())
		if retErr != nil {
			retErr = errors.Join(retErr, closeSnapshotUploadFiles(files))
		}
	}()

	manifestFile, err := openSnapshotUploadFile(root, ManifestFileName)
	if err != nil {
		return nil, Manifest{}, err
	}
	files = []snapshotUploadFile{{name: ManifestFileName, file: manifestFile}}

	data, err := readManifestData(
		manifestFile,
		[]ManifestOption{WithManifestMaxBytes(maxManifestBytes)},
	)
	if err != nil {
		return nil, Manifest{}, fmt.Errorf("read snapshot manifest for upload: %w", err)
	}
	idOpts := append(
		[]ManifestOption{WithManifestID(snapshotIDForDir(dir))},
		opts...,
	)
	idOpts = append(idOpts, WithManifestMaxBytes(maxManifestBytes))
	manifest, err := ParseManifest(data, idOpts...)
	if err != nil {
		return nil, Manifest{}, fmt.Errorf("parse snapshot manifest for upload: %w", err)
	}
	if _, err := manifestFile.Seek(0, io.SeekStart); err != nil {
		return nil, Manifest{}, fmt.Errorf("rewind snapshot manifest for upload: %w", err)
	}

	ordered := make([]snapshotUploadFile, 0, len(manifest.payloadFiles())+1)
	for _, payload := range manifest.payloadFiles() {
		file, err := openSnapshotUploadFile(root, payload.name)
		if err != nil {
			return nil, Manifest{}, err
		}
		files = append(files, snapshotUploadFile{name: payload.name, file: file})
		if payload.sha256 == "" {
			return nil, Manifest{}, fmt.Errorf(
				"%w: manifest declares no digest for %s",
				ErrSnapshotPayloadMismatch,
				payload.name,
			)
		}
		digest, size, err := hashSnapshotUploadFile(ctx, file)
		if err != nil {
			return nil, Manifest{}, fmt.Errorf(
				"hash snapshot upload file %q: %w", payload.name, err,
			)
		}
		if size != payload.size {
			return nil, Manifest{}, fmt.Errorf(
				"%w: %s is %d bytes, manifest declares %d",
				ErrSnapshotPayloadMismatch,
				payload.name,
				size,
				payload.size,
			)
		}
		if !strings.EqualFold(digest, payload.sha256) {
			return nil, Manifest{}, fmt.Errorf(
				"%w: %s digest %s, manifest declares %s",
				ErrSnapshotPayloadMismatch,
				payload.name,
				digest,
				payload.sha256,
			)
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, Manifest{}, fmt.Errorf(
				"rewind snapshot upload file %q: %w", payload.name, err,
			)
		}
		ordered = append(ordered, snapshotUploadFile{name: payload.name, file: file})
	}
	ordered = append(ordered, files[0])
	return ordered, manifest, nil
}

func hashSnapshotUploadFile(
	ctx context.Context,
	file *os.File,
) (string, int64, error) {
	h := sha256.New()
	size, err := io.Copy(h, payloadContextReader{ctx: ctx, reader: file})
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

func openSnapshotUploadFile(root *os.Root, name string) (*os.File, error) {
	file, err := root.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open snapshot upload file %q: %w", name, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect opened snapshot upload file %q: %w", name, err)
	}
	entry, err := root.Lstat(name)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect snapshot upload entry %q: %w", name, err)
	}
	if !info.Mode().IsRegular() || !entry.Mode().IsRegular() ||
		!os.SameFile(info, entry) {
		_ = file.Close()
		return nil, fmt.Errorf("snapshot upload file %q is not a stable regular file", name)
	}
	return file, nil
}

func closeSnapshotUploadFiles(files []snapshotUploadFile) error {
	var err error
	for _, file := range files {
		err = errors.Join(err, file.file.Close())
	}
	return err
}

// uploadStagedSnapshot serves a destination that implements only UploadDir,
// which reads pathnames rather than the verified handles. It copies the
// declared files from those handles into a private directory beside dir,
// authenticates the copy, and uploads the copy, so replacing an entry of dir
// after verification cannot change the uploaded bytes. The copy keeps dir's
// snapshot ID as its base name, so the signed manifest ID still matches.
func uploadStagedSnapshot(
	ctx context.Context,
	dest CloudDestination,
	dir string,
	opts ...ManifestOption,
) (_ Manifest, retErr error) {
	limit, err := manifestByteLimit(opts)
	if err != nil {
		return Manifest{}, err
	}
	files, _, err := openSnapshotUploadFiles(ctx, dir, limit, opts...)
	if err != nil {
		return Manifest{}, err
	}
	defer func() {
		retErr = errors.Join(retErr, closeSnapshotUploadFiles(files))
	}()
	tempDir, err := os.MkdirTemp(filepath.Dir(dir), ".dingo-cloud-upload-*")
	if err != nil {
		return Manifest{}, fmt.Errorf(
			"create staging directory for snapshot upload: %w", err,
		)
	}
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(tempDir))
	}()
	stagedDir := filepath.Join(tempDir, snapshotIDForDir(dir))
	if err := os.Mkdir(stagedDir, 0o700); err != nil {
		return Manifest{}, fmt.Errorf(
			"create staged snapshot directory: %w", err,
		)
	}
	for _, file := range files {
		if err := copySnapshotUploadFile(
			ctx, filepath.Join(stagedDir, file.name), file.file,
		); err != nil {
			return Manifest{}, err
		}
	}
	manifest, err := ReadManifest(stagedDir, opts...)
	if err != nil {
		return Manifest{}, err
	}
	if err := manifest.verifyPayloads(ctx, stagedDir, true); err != nil {
		return Manifest{}, err
	}
	if err := dest.UploadDir(ctx, stagedDir); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func copySnapshotUploadFile(
	ctx context.Context,
	path string,
	src *os.File,
) (retErr error) {
	dst, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create staged snapshot file %q: %w", path, err)
	}
	defer func() {
		if err := dst.Close(); err != nil {
			retErr = errors.Join(
				retErr,
				fmt.Errorf("close staged snapshot file %q: %w", path, err),
			)
		}
	}()
	if _, err := io.Copy(
		dst, payloadContextReader{ctx: ctx, reader: src},
	); err != nil {
		return fmt.Errorf("stage snapshot file %q: %w", path, err)
	}
	return nil
}

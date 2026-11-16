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

	"golang.org/x/sync/errgroup"
)

// snapshotUploadWorkers bounds how many deferred chunk files are open, and
// transferring, at once.
const snapshotUploadWorkers = 8

// forEachSnapshotUploadFile calls fn on each file's verified handle in
// publication order, except that each run of consecutive deferred chunk files
// is processed by at most workers concurrent goroutines, each holding one open
// file. The run completes before the next file starts, so the manifest, which
// is last, is only handled after every chunk. The first error cancels the
// context passed to the other calls and is returned after they finish.
func forEachSnapshotUploadFile(
	ctx context.Context,
	root *os.Root,
	files []snapshotUploadFile,
	workers int,
	fn func(context.Context, snapshotUploadFile, *os.File) error,
) error {
	for i := 0; i < len(files); {
		if files[i].file != nil {
			if err := fn(ctx, files[i], files[i].file); err != nil {
				return err
			}
			i++
			continue
		}
		j := i
		for j < len(files) && files[j].file == nil {
			j++
		}
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(workers)
		for _, f := range files[i:j] {
			if gctx.Err() != nil {
				break
			}
			g.Go(func() error {
				return f.with(gctx, root, func(h *os.File) error {
					return fn(gctx, f, h)
				})
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// snapshotUploadFile is one file of a snapshot upload. A file with a nil
// handle is a deferred chunk file: it holds no descriptor until with opens
// and verifies it, so descriptors stay bounded however many chunks a
// snapshot declares.
type snapshotUploadFile struct {
	name     string
	file     *os.File
	declared payloadFile
}

// with runs fn on the file's verified handle. A deferred chunk file is opened
// through root, checked against its declared size and digest, rewound, passed
// to fn and closed before with returns, so the bytes fn reads are the bytes
// that were verified.
func (f snapshotUploadFile) with(
	ctx context.Context,
	root *os.Root,
	fn func(*os.File) error,
) (retErr error) {
	if f.file != nil {
		return fn(f.file)
	}
	file, err := openSnapshotUploadFile(root, f.name)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			retErr = errors.Join(
				retErr,
				fmt.Errorf("close snapshot upload file %q: %w", f.name, err),
			)
		}
	}()
	if err := verifySnapshotUploadFile(ctx, f.declared, file); err != nil {
		return err
	}
	return fn(file)
}

// verifySnapshotUploadFile checks file against its declared size and digest
// and rewinds it.
func verifySnapshotUploadFile(
	ctx context.Context,
	payload payloadFile,
	file *os.File,
) error {
	if payload.sha256 == "" {
		return fmt.Errorf(
			"%w: manifest declares no digest for %s",
			ErrSnapshotPayloadMismatch,
			payload.name,
		)
	}
	digest, size, err := hashSnapshotUploadFile(ctx, file)
	if err != nil {
		return fmt.Errorf(
			"hash snapshot upload file %q: %w", payload.name, err,
		)
	}
	if size != payload.size {
		return fmt.Errorf(
			"%w: %s is %d bytes, manifest declares %d",
			ErrSnapshotPayloadMismatch,
			payload.name,
			size,
			payload.size,
		)
	}
	if !strings.EqualFold(digest, payload.sha256) {
		return fmt.Errorf(
			"%w: %s digest %s, manifest declares %s",
			ErrSnapshotPayloadMismatch,
			payload.name,
			digest,
			payload.sha256,
		)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf(
			"rewind snapshot upload file %q: %w", payload.name, err,
		)
	}
	return nil
}

type snapshotUploadResult struct {
	manifest Manifest
	root     *os.Root
}

func (r *snapshotUploadResult) close() error {
	if r.root == nil {
		return nil
	}
	err := r.root.Close()
	r.root = nil
	return err
}

// openSnapshotUploadFiles opens exactly the files declared by a snapshot,
// with the manifest last so it remains the remote completion marker. The root
// and each file remain held by handle, so replacing the directory or an entry
// cannot redirect a later upload. The caller owns the returned files and root.
func openSnapshotUploadFiles(
	ctx context.Context,
	dir string,
	maxManifestBytes int64,
	opts ...ManifestOption,
) (_ []snapshotUploadFile, _ Manifest, _ *os.Root, retErr error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, Manifest{}, nil, fmt.Errorf(
			"open snapshot directory %q: %w", dir, err,
		)
	}
	var files []snapshotUploadFile
	defer func() {
		if retErr != nil {
			retErr = errors.Join(
				retErr,
				closeSnapshotUploadFiles(files),
				root.Close(),
			)
		}
	}()
	if err := snapshotRootMatchesPath(root, dir); err != nil {
		return nil, Manifest{}, nil, err
	}

	manifestFile, err := openSnapshotUploadFile(root, ManifestFileName)
	if err != nil {
		return nil, Manifest{}, nil, err
	}
	files = []snapshotUploadFile{{name: ManifestFileName, file: manifestFile}}

	data, err := readManifestData(
		manifestFile,
		[]ManifestOption{WithManifestMaxBytes(maxManifestBytes)},
	)
	if err != nil {
		return nil, Manifest{}, nil, fmt.Errorf(
			"read snapshot manifest for upload: %w", err,
		)
	}
	idOpts := append(
		[]ManifestOption{WithManifestID(snapshotIDForDir(dir))},
		opts...,
	)
	idOpts = append(idOpts, WithManifestMaxBytes(maxManifestBytes))
	manifest, err := ParseManifest(data, idOpts...)
	if err != nil {
		return nil, Manifest{}, nil, fmt.Errorf(
			"parse snapshot manifest for upload: %w", err,
		)
	}
	if _, err := manifestFile.Seek(0, io.SeekStart); err != nil {
		return nil, Manifest{}, nil, fmt.Errorf(
			"rewind snapshot manifest for upload: %w", err,
		)
	}

	payloads := manifest.payloadFiles()
	ordered := make([]snapshotUploadFile, 0, len(payloads)+1)
	for _, payload := range payloads {
		file, err := openSnapshotUploadFile(root, payload.name)
		if err != nil {
			return nil, Manifest{}, nil, err
		}
		files = append(files, snapshotUploadFile{name: payload.name, file: file})
		if err := verifySnapshotUploadFile(ctx, payload, file); err != nil {
			return nil, Manifest{}, nil, err
		}
		ordered = append(ordered, snapshotUploadFile{name: payload.name, file: file})
		if payload.name == ImmutableIndexFileName {
			// Parse the verified handle, not the path, so the chunk list
			// is the one whose digest the manifest vouched for.
			chunkPayloads, err := readImmutableIndexFrom(file)
			if err != nil {
				return nil, Manifest{}, nil, err
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return nil, Manifest{}, nil, fmt.Errorf(
					"rewind snapshot upload file %q: %w", payload.name, err,
				)
			}
			// Chunk files are left unopened: a snapshot holds thousands,
			// more than the descriptor limit, so each is opened, verified,
			// used and closed by snapshotUploadFile.with.
			for _, chunk := range chunkPayloads {
				if chunk.sha256 == "" {
					return nil, Manifest{}, nil, fmt.Errorf(
						"%w: manifest declares no digest for %s",
						ErrSnapshotPayloadMismatch, chunk.name,
					)
				}
				ordered = append(ordered, snapshotUploadFile{
					name: chunk.name, declared: chunk,
				})
			}
		}
	}
	ordered = append(ordered, files[0])
	return ordered, manifest, root, nil
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
		if file.file == nil {
			continue
		}
		err = errors.Join(err, file.file.Close())
	}
	return err
}

func finishSnapshotUpload(
	files []snapshotUploadFile,
	root *os.Root,
	result *snapshotUploadResult,
	retErr *error,
) {
	*retErr = errors.Join(*retErr, closeSnapshotUploadFiles(files))
	if *retErr == nil {
		return
	}
	*retErr = errors.Join(*retErr, root.Close())
	*result = snapshotUploadResult{}
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
) (result snapshotUploadResult, retErr error) {
	limit, err := manifestByteLimit(opts)
	if err != nil {
		return snapshotUploadResult{}, err
	}
	files, _, root, err := openSnapshotUploadFiles(ctx, dir, limit, opts...)
	if err != nil {
		return snapshotUploadResult{}, err
	}
	defer finishSnapshotUpload(files, root, &result, &retErr)
	tempDir, err := os.MkdirTemp(filepath.Dir(dir), ".dingo-cloud-upload-*")
	if err != nil {
		return snapshotUploadResult{}, fmt.Errorf(
			"create staging directory for snapshot upload: %w", err,
		)
	}
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(tempDir))
	}()
	stagedDir := filepath.Join(tempDir, snapshotIDForDir(dir))
	if err := os.Mkdir(stagedDir, 0o700); err != nil {
		return snapshotUploadResult{}, fmt.Errorf(
			"create staged snapshot directory: %w", err,
		)
	}
	if err := forEachSnapshotUploadFile(
		ctx, root, files, snapshotUploadWorkers,
		func(ctx context.Context, file snapshotUploadFile, src *os.File) error {
			return copySnapshotUploadFile(
				ctx, filepath.Join(stagedDir, file.name), src,
			)
		},
	); err != nil {
		return snapshotUploadResult{}, err
	}
	manifest, err := ReadManifest(stagedDir, opts...)
	if err != nil {
		return snapshotUploadResult{}, err
	}
	if err := manifest.verifyPayloads(ctx, stagedDir, true); err != nil {
		return snapshotUploadResult{}, err
	}
	if err := dest.UploadDir(ctx, stagedDir); err != nil {
		return snapshotUploadResult{}, err
	}
	return snapshotUploadResult{
		manifest: manifest,
		root:     root,
	}, nil
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

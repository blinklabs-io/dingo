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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
)

// ImmutableIndexFileName is the snapshot file that lists the ImmutableDB chunk
// files a snapshot carries. The list is a payload of its own because a
// mainnet-sized chunk list does not fit the manifest's size bound.
const ImmutableIndexFileName = "immutable.index"

// immutableFilePrefix distinguishes a chunk file from the manifest, blob and
// metadata files in a snapshot directory, which holds every file flat so a
// cloud mirror can upload it without directories.
const immutableFilePrefix = "immutable-"

const (
	immutableIndexVersion = 1
	// maxImmutableIndexBytes bounds the index read before JSON decoding.
	maxImmutableIndexBytes = 64 << 20
)

// immutableIndexFile is one captured chunk, primary or secondary file.
type immutableIndexFile struct {
	// Name is the chunk file name, for example "00012.chunk". The snapshot
	// stores it as immutableFilePrefix + Name.
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// immutableIndex records the ImmutableDB as it stood under the commit barrier.
type immutableIndex struct {
	Version       int                  `json:"version"`
	ChunkSlots    uint64               `json:"chunkSlots"`
	CanContainEBB bool                 `json:"canContainEbb"`
	TipSlot       uint64               `json:"tipSlot"`
	TipHash       []byte               `json:"tipHash"`
	TipIndex      uint64               `json:"tipIndex"`
	Files         []immutableIndexFile `json:"files"`
}

func (f immutableIndexFile) snapshotName() string {
	return immutableFilePrefix + f.Name
}

func (idx immutableIndex) validate() error {
	if idx.Version != immutableIndexVersion {
		return fmt.Errorf("unsupported immutable index version %d", idx.Version)
	}
	seen := make(map[string]struct{}, len(idx.Files))
	for _, f := range idx.Files {
		if _, ok := immutable.ParseFileName(f.Name); !ok {
			return fmt.Errorf("immutable index names invalid file %q", f.Name)
		}
		if _, dup := seen[f.Name]; dup {
			return fmt.Errorf("immutable index lists %q twice", f.Name)
		}
		seen[f.Name] = struct{}{}
		if f.Size < 0 || f.SHA256 == "" {
			return fmt.Errorf("immutable index entry %q is incomplete", f.Name)
		}
	}
	return nil
}

// readImmutableIndex parses the index in dir. The caller has already verified
// its size and digest against the manifest.
func readImmutableIndex(dir string) (immutableIndex, error) {
	f, err := os.Open(filepath.Join(dir, ImmutableIndexFileName))
	if err != nil {
		return immutableIndex{}, fmt.Errorf("open immutable index: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only
	return decodeImmutableIndex(f)
}

// readImmutableIndexFrom parses an already verified index and returns the
// chunk files it declares.
func readImmutableIndexFrom(r io.Reader) ([]payloadFile, error) {
	idx, err := decodeImmutableIndex(r)
	if err != nil {
		return nil, err
	}
	return idx.payloadFiles(), nil
}

func decodeImmutableIndex(r io.Reader) (immutableIndex, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxImmutableIndexBytes+1))
	if err != nil {
		return immutableIndex{}, fmt.Errorf("read immutable index: %w", err)
	}
	if len(data) > maxImmutableIndexBytes {
		return immutableIndex{}, fmt.Errorf(
			"%w: immutable index exceeds %d bytes",
			ErrSnapshotPayloadMismatch, maxImmutableIndexBytes,
		)
	}
	var idx immutableIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return immutableIndex{}, fmt.Errorf("parse immutable index: %w", err)
	}
	if err := idx.validate(); err != nil {
		return immutableIndex{}, err
	}
	return idx, nil
}

// payloadFiles returns the chunk files the index declares.
func (idx immutableIndex) payloadFiles() []payloadFile {
	files := make([]payloadFile, 0, len(idx.Files))
	for _, f := range idx.Files {
		files = append(files, payloadFile{f.snapshotName(), f.Size, f.SHA256})
	}
	return files
}

// immutablePayloadFiles returns the chunk files m's index declares, or nil for
// a snapshot without one. dir must already hold a verified index.
func (m Manifest) immutablePayloadFiles(dir string) ([]payloadFile, error) {
	if m.ImmutableIndexSHA256 == "" {
		return nil, nil
	}
	idx, err := readImmutableIndex(dir)
	if err != nil {
		return nil, err
	}
	return idx.payloadFiles(), nil
}

// capturedImmutable is the chunk file set copied or linked into a snapshot
// directory under the commit barrier.
type capturedImmutable struct {
	state database.ImmutableState
	files []immutable.File
}

// captureImmutable places the ImmutableDB files in the snapshot directory. The
// caller holds database.LockImmutable and the commit barrier, so the recorded
// tip and the file set belong together.
//
// Local chunk files below the highest chunk are complete and never written
// again, so they are hard-linked; the store copies a linked file before it
// edits it. The highest chunk is still being appended to and is copied up to
// the length read here. Where a link is impossible, as across filesystems, the
// file is copied. A remote provider's files are streamed from object storage:
// its local directory only stages the writable tail.
func captureImmutable(
	ctx context.Context,
	db *database.Database,
	snapshotDir string,
) (*capturedImmutable, error) {
	layout, ok, err := db.ImmutableLayout()
	if err != nil {
		return nil, fmt.Errorf("read immutable layout: %w", err)
	}
	if !ok {
		return nil, nil
	}
	local := db.ImmutableFilesLocal()
	var top uint64
	for _, f := range layout.Files {
		top = max(top, f.Chunk)
	}
	for _, f := range layout.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dst := filepath.Join(snapshotDir, immutableFilePrefix+f.Name)
		if !local {
			if err := captureImmutableFile(ctx, db, f.Name, dst, f.Size); err != nil {
				return nil, fmt.Errorf("capture immutable file %q: %w", f.Name, err)
			}
			continue
		}
		src := filepath.Join(layout.Dir, f.Name)
		if f.Chunk < top {
			if err := os.Link(src, dst); err == nil {
				continue
			}
		}
		if err := copyPrefix(ctx, src, dst, f.Size); err != nil {
			return nil, fmt.Errorf("capture immutable file %q: %w", f.Name, err)
		}
	}
	return &capturedImmutable{state: layout.State, files: layout.Files}, nil
}

// captureImmutableFile streams a remote provider's file and verifies its
// length before it becomes part of a snapshot.
func captureImmutableFile(
	ctx context.Context,
	db *database.Database,
	name string,
	dst string,
	size int64,
) error {
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	n, readErr := db.ReadImmutableFile(ctx, name, &payloadContextWriter{
		ctx: ctx, writer: out,
	})
	if readErr == nil && n != size {
		readErr = fmt.Errorf("file is %d bytes, expected %d", n, size)
	}
	syncErr := out.Sync()
	return errors.Join(readErr, syncErr, out.Close())
}

type payloadContextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w *payloadContextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}

// copyPrefix copies the first size bytes of src to a new file dst.
func copyPrefix(ctx context.Context, src, dst string, size int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close() //nolint:errcheck // read-only
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	n, copyErr := io.Copy(
		out, &payloadContextReader{ctx: ctx, reader: io.LimitReader(in, size)},
	)
	if copyErr == nil && n != size {
		copyErr = fmt.Errorf("file is %d bytes, expected %d", n, size)
	}
	syncErr := out.Sync()
	return errors.Join(copyErr, syncErr, out.Close())
}

// writeImmutableIndex hashes the captured files, writes the index beside them
// and returns the manifest fields that declare it.
func writeImmutableIndex(
	ctx context.Context,
	root *os.Root,
	captured *capturedImmutable,
) (size int64, digest string, err error) {
	idx := immutableIndex{
		Version:       immutableIndexVersion,
		ChunkSlots:    captured.state.Geometry.ChunkSlots,
		CanContainEBB: captured.state.Geometry.CanContainEBB,
		TipSlot:       captured.state.TipSlot,
		TipHash:       captured.state.TipHash,
		TipIndex:      captured.state.TipIndex,
	}
	for _, f := range captured.files {
		name := immutableFilePrefix + f.Name
		sum, err := hashSnapshotRootFile(ctx, root, name)
		if err != nil {
			return 0, "", fmt.Errorf("hash %q: %w", name, err)
		}
		info, err := root.Stat(name)
		if err != nil {
			return 0, "", fmt.Errorf("stat %q: %w", name, err)
		}
		if info.Size() != f.Size {
			return 0, "", fmt.Errorf(
				"captured %q is %d bytes, expected %d",
				name,
				info.Size(),
				f.Size,
			)
		}
		idx.Files = append(idx.Files, immutableIndexFile{
			Name: f.Name, Size: f.Size, SHA256: sum,
		})
	}
	data, err := json.Marshal(idx)
	if err != nil {
		return 0, "", fmt.Errorf("marshal immutable index: %w", err)
	}
	if len(data) > maxImmutableIndexBytes {
		return 0, "", fmt.Errorf("immutable index is %d bytes", len(data))
	}
	file, err := root.OpenFile(
		ImmutableIndexFileName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		return 0, "", fmt.Errorf("create immutable index: %w", err)
	}
	_, werr := file.Write(data)
	serr := syncSnapshotFile(file)
	cerr := file.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		return 0, "", fmt.Errorf("write immutable index: %w", err)
	}
	sum, err := hashSnapshotRootFile(ctx, root, ImmutableIndexFileName)
	if err != nil {
		return 0, "", err
	}
	return int64(len(data)), sum, nil
}

// placeImmutableChunks moves the verified chunk files of a snapshot into the
// ImmutableDB directory of the restored data directory. The files are private
// copies on the same volume, so each moves with a rename.
func placeImmutableChunks(
	ctx context.Context,
	m Manifest,
	payloadDir string,
	stagingDir string,
) error {
	if m.ImmutableIndexSHA256 == "" {
		return nil
	}
	idx, err := readImmutableIndex(payloadDir)
	if err != nil {
		return err
	}
	target := filepath.Join(stagingDir, database.ImmutableDirName)
	if err := os.MkdirAll(target, 0o755); err != nil {
		return fmt.Errorf("create immutable directory: %w", err)
	}
	for _, f := range idx.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		src := filepath.Join(payloadDir, f.snapshotName())
		dst := filepath.Join(target, f.Name)
		if err := os.Rename(src, dst); err != nil {
			if err := copyPrefix(ctx, src, dst, f.Size); err != nil {
				return fmt.Errorf("place immutable file %q: %w", f.Name, err)
			}
		} else if err := syncPlacedFile(dst); err != nil {
			// The staged copy was written without a sync; the markers the
			// restore activates must not name bytes a power loss can drop.
			return fmt.Errorf("place immutable file %q: %w", f.Name, err)
		}
		// Staged payload files are private; chunk files are read by other
		// tools with the mode cardano-node gives them.
		if err := os.Chmod(dst, 0o644); err != nil { // #nosec G302
			return err
		}
		info, err := os.Stat(dst)
		if err != nil {
			return err
		}
		if info.Size() != f.Size {
			return fmt.Errorf(
				"%w: placed %q is %d bytes, index declares %d",
				ErrSnapshotPayloadMismatch, f.Name, info.Size(), f.Size,
			)
		}
	}
	return syncDir(target)
}

// syncPlacedFile opens path for writing because Windows flushes a file only
// through a handle with write access.
func syncPlacedFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// verifyImmutableFiles checks the chunk files in dir against the sizes and
// digests of the manifest's index.
func (m Manifest) verifyImmutableFiles(ctx context.Context, dir string) error {
	files, err := m.immutablePayloadFiles(dir)
	if err != nil {
		return err
	}
	return verifyPayloadList(ctx, dir, files, true)
}

// immutableDownloads returns the chunk files to fetch for m, each bounded by
// the size its index entry declares. dir holds the already fetched index; its
// digest is checked here, before anything it lists is trusted.
func (m Manifest) immutableDownloads(
	ctx context.Context,
	dir string,
) ([]DownloadFile, error) {
	if m.ImmutableIndexSHA256 == "" {
		return nil, nil
	}
	if err := verifyPayloadList(ctx, dir, []payloadFile{{
		ImmutableIndexFileName, m.ImmutableIndexBytes, m.ImmutableIndexSHA256,
	}}, true); err != nil {
		return nil, err
	}
	files, err := m.immutablePayloadFiles(dir)
	if err != nil {
		return nil, err
	}
	downloads := make([]DownloadFile, 0, len(files))
	for _, f := range files {
		downloads = append(
			downloads,
			DownloadFile{Name: f.name, MaxBytes: f.size},
		)
	}
	return downloads, nil
}

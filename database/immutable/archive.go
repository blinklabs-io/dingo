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

package immutable

import (
	"archive/tar"
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	"github.com/klauspost/compress/zstd"
)

// ArchiveExtension is the suffix of a completed chunk stored compressed: its
// .chunk, .primary and .secondary files as one zstd-compressed tar, the form
// Mithril publishes an immutable file in.
const ArchiveExtension = ".tar.zst"

// archiveDirName is the directory the archived files are named under, as in
// a Mithril immutable archive.
const archiveDirName = "immutable"

// archiveFileExtensions lists a chunk's files in the order an archive holds
// them.
var archiveFileExtensions = []string{".chunk", ".primary", ".secondary"}

// maxArchiveWindow and maxArchiveMemory bound what decoding one archive may
// allocate, so a corrupt or hostile archive cannot exhaust memory.
const (
	maxArchiveWindow = 128 << 20
	maxArchiveMemory = 1 << 30
)

// DefaultArchiveCacheBytes is the default bound on decompressed archived
// chunks held in memory for block reads.
const DefaultArchiveCacheBytes = 256 << 20

// ArchiveName returns the file name of chunk num's archive.
func ArchiveName(num uint64) string {
	return ChunkName(num) + ArchiveExtension
}

// chunkFiles holds the three files of one chunk.
type chunkFiles struct {
	chunk, primary, secondary []byte
}

func (c *chunkFiles) size() int64 {
	return int64(len(c.chunk) + len(c.primary) + len(c.secondary))
}

func (c *chunkFiles) file(ext string) ([]byte, error) {
	switch ext {
	case ".chunk":
		return c.chunk, nil
	case ".primary":
		return c.primary, nil
	case ".secondary":
		return c.secondary, nil
	}
	return nil, fmt.Errorf("immutable store: no %s file in an archive", ext)
}

// WriteChunkArchive writes chunk num's three files from dir to w in the
// Mithril immutable archive layout. A single encoder goroutine keeps the
// output bytes independent of the host's CPU count.
func WriteChunkArchive(w io.Writer, dir string, num uint64) error {
	zw, err := zstd.NewWriter(w, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zw)
	for _, ext := range archiveFileExtensions {
		if err := writeArchiveEntry(tw, dir, num, ext); err != nil {
			_ = zw.Close()
			return err
		}
	}
	if err := tw.Close(); err != nil {
		_ = zw.Close()
		return err
	}
	return zw.Close()
}

func writeArchiveEntry(tw *tar.Writer, dir string, num uint64, ext string) error {
	f, err := os.Open(filepath.Join(dir, ChunkName(num)+ext))
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Name:     archiveDirName + "/" + ChunkName(num) + ext,
		Typeflag: tar.TypeReg,
		Mode:     0o644,
		Size:     st.Size(),
		ModTime:  time.Unix(0, 0),
		Format:   tar.FormatUSTAR,
	}); err != nil {
		return err
	}
	n, err := io.Copy(tw, f)
	if err != nil {
		return err
	}
	if n != st.Size() {
		return fmt.Errorf(
			"immutable store: %s changed while archiving",
			ChunkName(num)+ext,
		)
	}
	return nil
}

// readChunkArchive decodes chunk num's archive at path.
func readChunkArchive(path string, num uint64) (*chunkFiles, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	chunk, primary, secondary, err := ReadChunkArchive(f, num)
	if err != nil {
		return nil, fmt.Errorf("archive %s: %w", path, err)
	}
	return &chunkFiles{chunk: chunk, primary: primary, secondary: secondary}, nil
}

// ReadChunkArchive decodes chunk num's archive from r and returns its .chunk,
// .primary and .secondary files. Every one of them must be present, and
// nothing else.
func ReadChunkArchive(
	r io.Reader,
	num uint64,
) (chunk, primary, secondary []byte, err error) {
	zr, err := zstd.NewReader(
		r,
		zstd.WithDecoderMaxWindow(maxArchiveWindow),
		zstd.WithDecoderMaxMemory(maxArchiveMemory),
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	defer zr.Close()
	want := map[string]*[]byte{}
	targets := []*[]byte{&chunk, &primary, &secondary}
	for i, ext := range archiveFileExtensions {
		want[archiveDirName+"/"+ChunkName(num)+ext] = targets[i]
	}
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%w: %w", ErrStoreCorrupt, err)
		}
		dst, ok := want[hdr.Name]
		if !ok || hdr.Typeflag != tar.TypeReg || *dst != nil {
			return nil, nil, nil, fmt.Errorf(
				"%w: unexpected archive entry %q", ErrStoreCorrupt, hdr.Name,
			)
		}
		if hdr.Size < 0 || hdr.Size > maxArchiveMemory {
			return nil, nil, nil, fmt.Errorf(
				"%w: archive entry %q has size %d",
				ErrStoreCorrupt, hdr.Name, hdr.Size,
			)
		}
		buf := make([]byte, hdr.Size)
		if _, err := io.ReadFull(tr, buf); err != nil {
			return nil, nil, nil, fmt.Errorf("%w: %w", ErrStoreCorrupt, err)
		}
		*dst = buf
	}
	for name, dst := range want {
		if *dst == nil {
			return nil, nil, nil, fmt.Errorf(
				"%w: archive lacks %s", ErrStoreCorrupt, name,
			)
		}
	}
	return chunk, primary, secondary, nil
}

// archiveCache holds decompressed archived chunks, least recently used first
// out, up to a byte bound. Entries are keyed by path, size and modification
// time, so an archive replaced on disk is never served from a stale entry.
type archiveCache struct {
	mu       sync.Mutex
	maxBytes int64
	used     int64
	order    *list.List
	entries  map[archiveKey]*list.Element
}

type archiveKey struct {
	path  string
	size  int64
	mtime int64
}

type archiveEntry struct {
	key   archiveKey
	files *chunkFiles
}

var sharedArchiveCache = newArchiveCache(DefaultArchiveCacheBytes)

func newArchiveCache(maxBytes int64) *archiveCache {
	return &archiveCache{
		maxBytes: maxBytes,
		order:    list.New(),
		entries:  map[archiveKey]*list.Element{},
	}
}

// SetArchiveCacheBytes sets the bound on decompressed archived chunks held in
// memory for block reads. Zero or less disables the cache: every read of an
// archived chunk decompresses it.
func SetArchiveCacheBytes(n int64) {
	c := sharedArchiveCache
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxBytes = n
	c.evictLocked()
}

func (c *archiveCache) get(path string, num uint64) (*chunkFiles, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	key := archiveKey{path: path, size: st.Size(), mtime: st.ModTime().UnixNano()}
	c.mu.Lock()
	if el, ok := c.entries[key]; ok {
		c.order.MoveToFront(el)
		files := el.Value.(*archiveEntry).files
		c.mu.Unlock()
		return files, nil
	}
	c.mu.Unlock()
	files, err := readChunkArchive(path, num)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; !ok && files.size() <= c.maxBytes {
		c.entries[key] = c.order.PushFront(&archiveEntry{key: key, files: files})
		c.used += files.size()
		c.evictLocked()
	}
	return files, nil
}

// forget drops every entry for path, for an archive about to be removed.
func (c *archiveCache) forget(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, el := range c.entries {
		if key.path == path {
			c.remove(el)
		}
	}
}

func (c *archiveCache) evictLocked() {
	for c.used > c.maxBytes {
		el := c.order.Back()
		if el == nil {
			return
		}
		c.remove(el)
	}
}

func (c *archiveCache) remove(el *list.Element) {
	entry := el.Value.(*archiveEntry)
	c.order.Remove(el)
	delete(c.entries, entry.key)
	c.used -= entry.files.size()
}

// readChunkFile returns the whole ext file of chunk num in dir, from the plain
// file when the chunk has one and from its archive otherwise.
func readChunkFile(dir string, num uint64, ext string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, ChunkName(num)+ext))
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return data, err
	}
	files, aerr := sharedArchiveCache.get(
		filepath.Join(dir, ArchiveName(num)), num,
	)
	if aerr != nil {
		if errors.Is(aerr, os.ErrNotExist) {
			return nil, err
		}
		return nil, aerr
	}
	return files.file(ext)
}

// openChunkReader returns a reader over chunk num's .chunk file in dir, the
// plain file when the chunk has one and its archived bytes otherwise.
func openChunkReader(dir string, num uint64) (io.ReaderAt, func(), error) {
	f, err := os.Open(filepath.Join(dir, ChunkName(num)+chunkFileExtension))
	if err == nil {
		return f, func() { _ = f.Close() }, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	files, aerr := sharedArchiveCache.get(
		filepath.Join(dir, ArchiveName(num)), num,
	)
	if aerr != nil {
		if errors.Is(aerr, os.ErrNotExist) {
			return nil, nil, err
		}
		return nil, nil, aerr
	}
	return bytes.NewReader(files.chunk), func() {}, nil
}

// packChunk replaces chunk num's plain files in dir with its archive. The
// archive is written under a temporary name and renamed into place before the
// plain files are removed, so a crash leaves one complete form or the other.
func packChunk(dir string, num uint64, sync func(*os.File) error) error {
	final := filepath.Join(dir, ArchiveName(num))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	werr := WriteChunkArchive(f, dir, num)
	var serr error
	if werr == nil {
		if sync != nil {
			serr = sync(f)
		} else {
			serr = f.Sync()
		}
	}
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("immutable store: archive chunk %d: %w", num, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := fsyncdir.Sync(dir); err != nil {
		return err
	}
	if err := removePlainChunk(dir, num); err != nil {
		return err
	}
	return fsyncdir.Sync(dir)
}

// ArchiveCompletedChunk replaces chunk num's plain files in dir with its
// archive, unless it is already archived or is the highest chunk in dir,
// which may still be appended to.
func ArchiveCompletedChunk(dir string, num uint64) error {
	s := &Store{dir: dir}
	nums, err := s.listChunks()
	if err != nil {
		return err
	}
	if len(nums) == 0 || num >= nums[len(nums)-1] {
		return nil
	}
	archived, err := s.isArchived(num)
	if err != nil || archived {
		return err
	}
	return packChunk(dir, num, nil)
}

// unpackChunk replaces chunk num's archive in dir with its plain files, so the
// chunk can be appended to or truncated.
func unpackChunk(dir string, num uint64) error {
	final := filepath.Join(dir, ArchiveName(num))
	files, err := readChunkArchive(final, num)
	if err != nil {
		return err
	}
	for _, ext := range archiveFileExtensions {
		data, err := files.file(ext)
		if err != nil {
			return err
		}
		if err := writeFileSynced(
			filepath.Join(dir, ChunkName(num)+ext), data,
		); err != nil {
			return err
		}
	}
	if err := fsyncdir.Sync(dir); err != nil {
		return err
	}
	sharedArchiveCache.forget(final)
	if err := os.Remove(final); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return fsyncdir.Sync(dir)
}

func writeFileSynced(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func removePlainChunk(dir string, num uint64) error {
	for _, ext := range archiveFileExtensions {
		err := os.Remove(filepath.Join(dir, ChunkName(num)+ext))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// chunkFileSize returns the size of chunk num's .chunk file in dir, plain or
// archived.
func chunkFileSize(dir string, num uint64) (uint64, error) {
	st, err := os.Stat(filepath.Join(dir, ChunkName(num)+chunkFileExtension))
	if err == nil {
		return uint64(st.Size()), nil // #nosec G115 -- non-negative
	}
	if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	files, aerr := sharedArchiveCache.get(
		filepath.Join(dir, ArchiveName(num)), num,
	)
	if aerr != nil {
		if errors.Is(aerr, os.ErrNotExist) {
			return 0, err
		}
		return 0, aerr
	}
	return uint64(len(files.chunk)), nil
}

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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

// File is one chunk, primary or secondary file in a chunk directory.
type File struct {
	// Name is the file name, for example "00012.chunk".
	Name string
	// Chunk is the chunk number the file belongs to.
	Chunk uint64
	Size  int64
}

// ParseFileName returns the chunk number of a chunk, primary or secondary file
// name, and false for anything else in the directory.
func ParseFileName(name string) (uint64, bool) {
	base, ext, ok := strings.Cut(name, ".")
	if !ok || !isCanonicalChunkName(base) {
		return 0, false
	}
	switch "." + ext {
	case chunkFileExtension, primaryFileExtension, secondaryFileExtension:
	default:
		return 0, false
	}
	num, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return num, true
}

// ListFiles returns the chunk, primary and secondary files in dir, ordered by
// chunk number and then name. A missing directory has none.
func ListFiles(dir string) ([]File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var files []File
	for _, e := range entries {
		num, ok := ParseFileName(e.Name())
		if !ok || !e.Type().IsRegular() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		files = append(
			files,
			File{Name: e.Name(), Chunk: num, Size: info.Size()},
		)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Chunk != files[j].Chunk {
			return files[i].Chunk < files[j].Chunk
		}
		return files[i].Name < files[j].Name
	})
	return files, nil
}

// unshare replaces path with a private copy when another directory entry
// links to the same bytes. Snapshots hard-link completed chunks, so a file
// that is about to be modified in place must first stop being shared or the
// snapshot would change with it.
func unshare(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if !hasOtherLinks(info) {
		return nil
	}
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	tmp := path + ".unshare"
	dst, err := os.OpenFile(
		tmp,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		info.Mode().Perm(),
	)
	if err != nil {
		_ = src.Close()
		return err
	}
	_, cerr := io.Copy(dst, src)
	serr := dst.Sync()
	clerr := dst.Close()
	// Windows refuses to replace a file another handle still holds open, so
	// the source is closed before the rename.
	srcErr := src.Close()
	if err := errors.Join(cerr, serr, clerr, srcErr); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("immutable store: unshare %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return fsyncdir.Sync(filepath.Dir(path))
}

func unshareChunk(s *Store, num uint64) error {
	for _, ext := range []string{chunkFileExtension, secondaryFileExtension, primaryFileExtension} {
		if err := unshare(s.path(num, ext)); err != nil {
			return err
		}
	}
	return nil
}

// RemoveChunksBefore removes every chunk file whose number is below num.
// Missing files are ignored, making a partially completed removal safe to
// retry. The caller must serialize removal with readers and writers.
func RemoveChunksBefore(dir string, num uint64) error {
	files, err := ListFiles(dir)
	if err != nil {
		return fmt.Errorf("immutable store: list files to remove: %w", err)
	}
	var (
		changed   bool
		removeErr error
	)
	for _, file := range files {
		if file.Chunk >= num {
			continue
		}
		err := os.Remove(filepath.Join(dir, file.Name))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			removeErr = errors.Join(removeErr, fmt.Errorf(
				"immutable store: remove %s: %w", file.Name, err,
			))
			continue
		}
		changed = true
	}
	if changed {
		removeErr = errors.Join(removeErr, fsyncdir.Sync(dir))
	}
	return removeErr
}

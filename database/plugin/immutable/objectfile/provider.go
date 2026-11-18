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

package objectfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// Config is empty because the selected blob provider owns the bucket,
// endpoint, prefix, credentials and namespace.
type Config struct{}

// RegisterProvider registers an object-backed chunk-file provider under the
// same name as its S3 or GCS blob provider.
func RegisterProvider(host *hostplugin.Host, name string) error {
	if name != immplugin.S3ProviderName && name != immplugin.GCSProviderName {
		return fmt.Errorf("unsupported ImmutableDB object provider %q", name)
	}
	return hostplugin.Register(
		host,
		hostplugin.Descriptor{
			Capability:   hostplugin.CapabilityStorageImmutable,
			Name:         name,
			Description:  "cardano-node compatible chunk files in object storage",
			DurableClose: true,
		},
		func() Config { return Config{} },
		func(
			_ context.Context,
			_ Config,
			deps immplugin.ProviderDependencies,
		) (immplugin.ImmutableDB, hostplugin.Instance, error) {
			if deps.ObjectFiles == nil {
				return nil, nil, fmt.Errorf(
					"ImmutableDB provider %q requires object-file operations from its blob provider",
					name,
				)
			}
			dir := filepath.Join(deps.DataDir, immplugin.DirName)
			owned := deps.DataDir == ""
			if owned {
				var err error
				dir, err = os.MkdirTemp("", "dingo-immutable-objects-")
				if err != nil {
					return nil, nil, err
				}
			} else if err := os.RemoveAll(filepath.Join(dir, ".read-cache")); err != nil {
				return nil, nil, fmt.Errorf("clear stale ImmutableDB read cache: %w", err)
			}
			svc := &service{
				dir: dir, owned: owned,
				objects: deps.ObjectFiles,
				dirty:   make(map[uint64]struct{}),
				cache:   make(map[uint64]*readCacheEntry),
			}
			return svc, hostplugin.Lifecycle{StopFunc: svc.stop}, nil
		},
	)
}

type service struct {
	dir     string
	owned   bool
	objects blob.ImmutableObjects

	mu          sync.Mutex
	open        *store
	dirty       map[uint64]struct{}
	cache       map[uint64]*readCacheEntry
	clock       uint64
	activeReads int
	stopped     bool
}

const maxCachedReadChunks = 2

type readCacheEntry struct {
	dir     string
	refs    int
	used    uint64
	invalid bool
}

func (s *service) Dir() string { return s.dir }

func (s *service) Open(geom immutable.Geometry) (immplugin.Store, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, errors.New("immutable object store provider is stopped")
	}
	if s.open != nil {
		return nil, errors.New("immutable object store is already open")
	}
	if !geom.Valid() {
		return nil, fmt.Errorf("invalid ImmutableDB geometry %+v", geom)
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return nil, fmt.Errorf("create ImmutableDB staging directory: %w", err)
	}

	for num, entry := range s.cache {
		delete(s.cache, num)
		entry.invalid = true
		if entry.refs == 0 {
			if err := os.RemoveAll(entry.dir); err != nil {
				return nil, err
			}
		}
	}
	remote, err := s.remoteFiles()
	if err != nil {
		return nil, err
	}
	if err := clearLocalChunks(s.dir); err != nil {
		return nil, err
	}
	s.dirty = make(map[uint64]struct{})
	var inner *immutable.Store
	for {
		var tail uint64
		if len(remote) != 0 {
			tail = remote[len(remote)-1].Chunk
			if err := s.downloadChunk(tail); err != nil {
				return nil, fmt.Errorf(
					"stage ImmutableDB tail chunk %d: %w",
					tail,
					err,
				)
			}
		}
		inner, err = immutable.OpenStore(s.dir, geom)
		if err != nil {
			return nil, err
		}
		if _, ok := inner.Tip(); ok || len(remote) == 0 {
			if len(remote) != 0 {
				s.dirty[tail] = struct{}{}
			}
			break
		}
		if err := inner.Close(); err != nil {
			return nil, err
		}
		// A new chunk is published body first and primary last. With no primary
		// entry surviving recovery, no database marker could have committed for
		// this chunk. Remove the partial upload and recover the previous tail.
		if err := s.deleteRemoteChunk(tail); err != nil {
			return nil, fmt.Errorf(
				"remove incomplete ImmutableDB chunk %d: %w", tail, err,
			)
		}
		for len(remote) != 0 && remote[len(remote)-1].Chunk == tail {
			remote = remote[:len(remote)-1]
		}
	}
	s.open = &store{Store: inner, owner: s, geom: geom}
	return s.open, nil
}

func (s *service) ReadBlockAt(loc immutable.Location) (uint, []byte, error) {
	dir, cleanup, err := s.readChunkDir(loc.Chunk)
	if err != nil {
		return 0, nil, err
	}
	defer cleanup()
	return immutable.ReadBlockAt(dir, loc)
}

func (s *service) ListFiles() ([]immutable.File, error) {
	return s.remoteFiles()
}

func (s *service) ReadFile(
	ctx context.Context,
	name string,
	dst io.Writer,
) (int64, error) {
	if _, ok := immutable.ParseFileName(name); !ok {
		return 0, fmt.Errorf("invalid ImmutableDB file name %q", name)
	}
	return s.objects.ReadImmutableFile(ctx, name, dst)
}

func (s *service) PublishChunk(num uint64) error {
	if err := s.publishChunk(num); err != nil {
		return err
	}
	return s.clearReadCache(func(chunk uint64) bool { return chunk == num })
}

func (s *service) ReleaseChunk(num uint64) error {
	var removeErr error
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		path := filepath.Join(s.dir, immutable.ChunkName(num)+ext)
		if err := os.Remove(path); err != nil &&
			!errors.Is(err, fs.ErrNotExist) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	return errors.Join(removeErr, syncDir(s.dir))
}

func (s *service) publishChunk(num uint64) error {
	for _, ext := range []string{".chunk", ".secondary", ".primary"} {
		name := immutable.ChunkName(num) + ext
		path := filepath.Join(s.dir, name)
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open staged file %q: %w", name, err)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return fmt.Errorf("stat staged file %q: %w", name, statErr)
		}
		writeErr := s.objects.WriteImmutableFile(
			context.Background(), name, file, info.Size(),
		)
		closeErr := file.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			return fmt.Errorf("publish staged file %q: %w", name, err)
		}
	}
	return nil
}

// deleteRemoteChunk removes one chunk's objects in the reverse of their
// publication order, primary index first, so an interrupted removal leaves
// what an interrupted publication would.
func (s *service) deleteRemoteChunk(num uint64) error {
	for _, ext := range []string{".primary", ".secondary", ".chunk"} {
		name := immutable.ChunkName(num) + ext
		if err := s.objects.DeleteImmutableFile(
			context.Background(), name,
		); err != nil {
			return fmt.Errorf("delete ImmutableDB object %q: %w", name, err)
		}
	}
	return nil
}

// remoteChunksDescending returns the distinct remote chunk numbers, highest
// first.
func (s *service) remoteChunksDescending() ([]uint64, error) {
	files, err := s.remoteFiles()
	if err != nil {
		return nil, err
	}
	chunks := make([]uint64, 0, len(files))
	for _, file := range files {
		chunks = append(chunks, file.Chunk)
	}
	chunks = slices.Compact(chunks)
	slices.Reverse(chunks)
	return chunks, nil
}

func (s *service) remoteFiles() ([]immutable.File, error) {
	objects, err := s.objects.ListImmutableFiles(context.Background())
	if err != nil {
		return nil, fmt.Errorf("list ImmutableDB objects: %w", err)
	}
	files := make([]immutable.File, 0, len(objects))
	for _, object := range objects {
		chunk, ok := immutable.ParseFileName(object.Name)
		if !ok {
			continue
		}
		files = append(files, immutable.File{
			Name: object.Name, Chunk: chunk, Size: object.Size,
		})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Chunk != files[j].Chunk {
			return files[i].Chunk < files[j].Chunk
		}
		return files[i].Name < files[j].Name
	})
	return files, nil
}

func (s *service) downloadChunk(num uint64) error {
	files, err := s.remoteFiles()
	if err != nil {
		return err
	}
	wanted := make(map[string]int64, 3)
	for _, file := range files {
		if file.Chunk == num {
			wanted[file.Name] = file.Size
		}
	}
	for _, ext := range []string{".chunk", ".secondary", ".primary"} {
		name := immutable.ChunkName(num) + ext
		size, exists := wanted[name]
		if !exists {
			continue
		}
		path := filepath.Join(s.dir, name)
		tmp, err := os.CreateTemp(s.dir, name+".download-*")
		if err != nil {
			return err
		}
		n, readErr := s.objects.ReadImmutableFile(
			context.Background(), name, tmp,
		)
		syncErr := tmp.Sync()
		closeErr := tmp.Close()
		if err := errors.Join(readErr, syncErr, closeErr); err != nil {
			_ = os.Remove(tmp.Name())
			return fmt.Errorf("download ImmutableDB file %q: %w", name, err)
		}
		if n != size {
			_ = os.Remove(tmp.Name())
			return fmt.Errorf(
				"download ImmutableDB file %q: received %d bytes, listed %d",
				name, n, size,
			)
		}
		if err := os.Rename(tmp.Name(), path); err != nil {
			_ = os.Remove(tmp.Name())
			return fmt.Errorf("stage ImmutableDB file %q: %w", name, err)
		}
	}
	return syncDir(s.dir)
}

func (s *service) readChunkDir(num uint64) (string, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return "", nil, errors.New("immutable object store provider is stopped")
	}
	if entry := s.cache[num]; entry != nil && !entry.invalid {
		s.clock++
		entry.used = s.clock
		entry.refs++
		s.activeReads++
		return entry.dir, func() { s.releaseReadChunk(entry) }, nil
	}

	cacheRoot := filepath.Join(s.dir, ".read-cache")
	if err := os.MkdirAll(cacheRoot, 0o755); err != nil {
		return "", nil, err
	}
	dir, err := os.MkdirTemp(cacheRoot, immutable.ChunkName(num)+"-*")
	if err != nil {
		return "", nil, err
	}
	// The three names are fixed, so a miss reads them directly rather than
	// listing every ImmutableDB object.
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		name := immutable.ChunkName(num) + ext
		path := filepath.Join(dir, name)
		out, err := os.Create(path)
		if err != nil {
			_ = os.RemoveAll(dir)
			return "", nil, err
		}
		_, readErr := s.objects.ReadImmutableFile(
			context.Background(), name, out,
		)
		syncErr := out.Sync()
		closeErr := out.Close()
		if errors.Is(readErr, fs.ErrNotExist) {
			if err := os.Remove(path); err != nil {
				_ = os.RemoveAll(dir)
				return "", nil, err
			}
			continue
		}
		if err := errors.Join(readErr, syncErr, closeErr); err != nil {
			_ = os.RemoveAll(dir)
			return "", nil, fmt.Errorf(
				"download ImmutableDB file %q: %w", name, err,
			)
		}
	}
	if len(s.cache) >= maxCachedReadChunks {
		var victim uint64
		var oldest uint64
		found := false
		for chunk, entry := range s.cache {
			if entry.refs == 0 && (!found || entry.used < oldest) {
				victim, oldest, found = chunk, entry.used, true
			}
		}
		if !found {
			s.activeReads++
			return dir, func() { s.releaseTransientRead(dir) }, nil
		}
		if evicted := s.cache[victim]; evicted != nil {
			evicted.invalid = true
			_ = os.RemoveAll(evicted.dir)
		}
		delete(s.cache, victim)
	}
	s.clock++
	entry := &readCacheEntry{dir: dir, refs: 1, used: s.clock}
	s.cache[num] = entry
	s.activeReads++
	return dir, func() { s.releaseReadChunk(entry) }, nil
}

func (s *service) releaseReadChunk(entry *readCacheEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.refs > 0 {
		entry.refs--
		s.activeReads--
	}
	if entry.invalid && entry.refs == 0 {
		_ = os.RemoveAll(entry.dir)
	}
	if s.stopped && s.activeReads == 0 {
		_ = s.cleanupStopped()
	}
}

func (s *service) releaseTransientRead(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeReads--
	_ = os.RemoveAll(dir)
	if s.stopped && s.activeReads == 0 {
		_ = s.cleanupStopped()
	}
}

func (s *service) clearReadCache(predicate func(uint64) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var clearErr error
	for num, entry := range s.cache {
		if !predicate(num) {
			continue
		}
		delete(s.cache, num)
		entry.invalid = true
		if entry.refs == 0 {
			clearErr = errors.Join(clearErr, os.RemoveAll(entry.dir))
		}
	}
	return clearErr
}

func (s *service) stop(context.Context) error {
	s.mu.Lock()
	s.stopped = true
	st := s.open
	s.open = nil
	s.mu.Unlock()
	var stopErr error
	if st != nil {
		stopErr = st.Close()
	}
	stopErr = errors.Join(
		stopErr,
		s.clearReadCache(func(uint64) bool { return true }),
	)
	s.mu.Lock()
	if s.activeReads == 0 {
		stopErr = errors.Join(
			stopErr,
			s.cleanupStopped(),
		)
	}
	s.mu.Unlock()
	return stopErr
}

func clearLocalChunks(dir string) error {
	files, err := immutable.ListFiles(dir)
	if err != nil {
		return err
	}
	var removeErr error
	for _, file := range files {
		if err := os.Remove(filepath.Join(dir, file.Name)); err != nil &&
			!errors.Is(err, fs.ErrNotExist) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	if removeErr != nil {
		return removeErr
	}
	return syncDir(dir)
}

// removeStagedChunksBefore drops local copies of chunks below num once they
// are published; only the writable tail stays staged.
func removeStagedChunksBefore(dir string, num uint64) error {
	files, err := immutable.ListFiles(dir)
	if err != nil {
		return err
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
			removeErr = errors.Join(removeErr, err)
			continue
		}
		changed = true
	}
	if changed {
		removeErr = errors.Join(removeErr, syncDir(dir))
	}
	return removeErr
}

func syncDir(dir string) error {
	return fsyncdir.Sync(dir)
}

type store struct {
	*immutable.Store
	owner *service
	geom  immutable.Geometry
}

func (s *store) Append(b immutable.WriteBlock) (immutable.Location, error) {
	before, hasBefore := s.Tip()
	loc, err := s.Store.Append(b)
	if err != nil {
		return immutable.Location{}, err
	}
	// The writer fills every chunk it skips with empty files, as
	// cardano-node does, so the whole range is published, not just its ends.
	var first uint64
	if hasBefore {
		first = before.Slot / s.geom.ChunkSlots
	}
	for num := first; num <= loc.Chunk; num++ {
		s.owner.dirty[num] = struct{}{}
	}
	return loc, nil
}

func (s *store) Sync() error {
	if err := s.Store.Sync(); err != nil {
		return err
	}
	chunks := make([]uint64, 0, len(s.owner.dirty))
	for num := range s.owner.dirty {
		chunks = append(chunks, num)
	}
	slices.Sort(chunks)
	for _, num := range chunks {
		if err := s.owner.publishChunk(num); err != nil {
			return err
		}
		delete(s.owner.dirty, num)
		if err := s.owner.clearReadCache(func(chunk uint64) bool { return chunk == num }); err != nil {
			return err
		}
	}
	tip, ok := s.Tip()
	if ok {
		return removeStagedChunksBefore(
			s.owner.dir,
			tip.Slot/s.geom.ChunkSlots,
		)
	}
	return nil
}

func (s *store) ReadLocation(loc immutable.Location) (uint, []byte, error) {
	dir, cleanup, err := s.owner.readChunkDir(loc.Chunk)
	if err != nil {
		return 0, nil, err
	}
	defer cleanup()
	return immutable.ReadBlockAt(dir, loc)
}

func (s *store) GetBlock(
	slot uint64,
	hash []byte,
) (*immutable.Block, *immutable.Location, error) {
	dir, cleanup, err := s.owner.readChunkDir(slot / s.geom.ChunkSlots)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	return immutable.ReadBlockByPoint(dir, s.geom, slot, hash)
}

func (s *store) TruncateAfter(slot uint64, hash []byte) error {
	chunk := slot / s.geom.ChunkSlots
	if err := s.owner.clearReadCache(func(c uint64) bool { return c >= chunk }); err != nil {
		return err
	}
	// Remove later chunks first, highest down, so an interruption never leaves
	// a gap below a surviving tail; the next Open stages whichever chunk is
	// then highest and reconciliation repeats the truncation.
	later, err := s.owner.remoteChunksDescending()
	if err != nil {
		return err
	}
	for _, num := range later {
		if num <= chunk {
			break
		}
		if err := s.owner.deleteRemoteChunk(num); err != nil {
			return err
		}
	}
	// The underlying writer only has the writable tail staged. Reopen it on
	// the requested chunk before asking it to locate and truncate the
	// rollback point.
	if err := s.Store.Close(); err != nil {
		return err
	}
	if err := clearLocalChunks(s.owner.dir); err != nil {
		return err
	}
	if err := s.owner.downloadChunk(chunk); err != nil {
		return err
	}
	inner, err := immutable.OpenStore(s.owner.dir, s.geom)
	if err != nil {
		return err
	}
	s.Store = inner
	if err := s.Store.TruncateAfter(slot, hash); err != nil {
		return err
	}
	s.owner.dirty = map[uint64]struct{}{chunk: {}}
	return s.Sync()
}

func (s *store) TruncateAll() error {
	if err := s.owner.clearReadCache(func(uint64) bool { return true }); err != nil {
		return err
	}
	if err := s.Store.TruncateAll(); err != nil {
		return err
	}
	s.owner.dirty = make(map[uint64]struct{})
	chunks, err := s.owner.remoteChunksDescending()
	if err != nil {
		return err
	}
	for _, num := range chunks {
		if err := s.owner.deleteRemoteChunk(num); err != nil {
			return err
		}
	}
	return clearLocalChunks(s.owner.dir)
}

func (s *store) Close() error {
	err := s.Store.Close()
	s.owner.mu.Lock()
	if s.owner.open == s {
		s.owner.open = nil
	}
	s.owner.mu.Unlock()
	return err
}

func (s *service) cleanupStopped() error {
	if s.owned {
		return os.RemoveAll(s.dir)
	}
	return os.RemoveAll(filepath.Join(s.dir, ".read-cache"))
}

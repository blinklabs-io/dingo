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

// Package chunkfile is the built-in storage.immutable provider. It keeps the
// ImmutableDB in cardano-node compatible chunk files under the data directory.
package chunkfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/blinklabs-io/dingo/database/immutable"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// ProviderName is the name the built-in provider registers under.
const ProviderName = "chunkfile"

// ErrClosed reports an Open after the provider has stopped.
var ErrClosed = errors.New("immutable chunk store provider is stopped")

// ErrStoreOpen reports an Open while a writer is already open.
var ErrStoreOpen = errors.New("immutable chunk store is already open")

// Config is the provider configuration. The provider has no settings of its
// own: the directory derives from the data directory.
// Config configures the local chunk-file ImmutableDB.
type Config struct {
	// Compression is how chunks the store completes are kept: "none" (the
	// default) as plain .chunk, .primary and .secondary files, or "mithril"
	// as one zstd-compressed tar per chunk in the Mithril immutable archive
	// layout. Chunks already on disk keep their form; both are read.
	Compression string `yaml:"compression"`
	// ArchiveCacheMB bounds, in MiB, the decompressed archived chunks kept in
	// memory for block reads. Zero uses immutable.DefaultArchiveCacheBytes.
	ArchiveCacheMB int `yaml:"archiveCacheMb"`
}

// Compression values.
const (
	CompressionNone    = "none"
	CompressionMithril = "mithril"
)

func (c Config) compress() (bool, error) {
	switch c.Compression {
	case "", CompressionNone:
		return false, nil
	case CompressionMithril:
		return true, nil
	}
	return false, fmt.Errorf(
		"invalid ImmutableDB compression %q (want %q or %q)",
		c.Compression, CompressionNone, CompressionMithril,
	)
}

// RegisterProvider registers the chunk-file provider with host. It is
// registered in every build.
func RegisterProvider(host *hostplugin.Host) error {
	return hostplugin.Register(
		host,
		hostplugin.Descriptor{
			Capability:   hostplugin.CapabilityStorageImmutable,
			Name:         ProviderName,
			Description:  "cardano-node compatible chunk files",
			DurableClose: true,
		},
		func() Config { return Config{} },
		func(
			_ context.Context,
			cfg Config,
			deps immplugin.ProviderDependencies,
		) (immplugin.ImmutableDB, hostplugin.Instance, error) {
			compress, err := cfg.compress()
			if err != nil {
				return nil, nil, err
			}
			if cfg.ArchiveCacheMB < 0 {
				return nil, nil, fmt.Errorf(
					"invalid ImmutableDB archiveCacheMb %d", cfg.ArchiveCacheMB,
				)
			}
			if cfg.ArchiveCacheMB > 0 {
				immutable.SetArchiveCacheBytes(int64(cfg.ArchiveCacheMB) << 20)
			}
			dataDir := deps.DataDir
			var temporary string
			if dataDir == "" {
				temporary, err = os.MkdirTemp("", "dingo-immutable-")
				if err != nil {
					return nil, nil, fmt.Errorf("create ephemeral ImmutableDB: %w", err)
				}
				dataDir = temporary
			}
			svc := &service{
				dir:       filepath.Join(dataDir, immplugin.DirName),
				temporary: temporary,
				compress:  compress,
			}
			return svc, hostplugin.Lifecycle{StopFunc: svc.stop}, nil
		},
	)
}

type service struct {
	dir       string
	temporary string
	compress  bool

	mu      sync.Mutex
	open    *store
	stopped bool
}

func (s *service) Dir() string { return s.dir }

func (s *service) Open(geom immutable.Geometry) (immplugin.Store, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, ErrClosed
	}
	if s.open != nil {
		return nil, ErrStoreOpen
	}
	inner, err := immutable.OpenStoreWithOptions(
		s.dir, geom, immutable.StoreOptions{Compress: s.compress},
	)
	if err != nil {
		return nil, err
	}
	s.open = &store{Store: inner, owner: s}
	return s.open, nil
}

func (s *service) ReadBlockAt(loc immutable.Location) (uint, []byte, error) {
	return immutable.ReadBlockAt(s.dir, loc)
}

// ArchiveChunk archives completed chunk num when the provider compresses.
func (s *service) ArchiveChunk(num uint64) error {
	if !s.compress {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return immutable.ArchiveCompletedChunk(s.dir, num)
}

func (s *service) ListFiles() ([]immutable.File, error) {
	return immutable.ListFiles(s.dir)
}

func (s *service) ReadFile(
	ctx context.Context,
	name string,
	dst io.Writer,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if _, ok := immutable.ParseFileName(name); !ok {
		return 0, fmt.Errorf("invalid ImmutableDB file name %q", name)
	}
	file, err := os.Open(filepath.Join(s.dir, name))
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return io.Copy(dst, file)
}

// stop closes the writer a holder left open and refuses later opens.
func (s *service) stop(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	var err error
	if s.open != nil {
		st := s.open
		s.open = nil
		err = st.Store.Close()
	}
	if s.temporary != "" {
		err = errors.Join(err, os.RemoveAll(s.temporary))
	}
	return err
}

// release forgets st once its holder closed it, so Open can hand out the
// next writer.
func (s *service) release(st *store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open == st {
		s.open = nil
	}
}

type store struct {
	*immutable.Store
	owner *service
}

func (s *store) Close() error {
	err := s.Store.Close()
	s.owner.release(s)
	return err
}

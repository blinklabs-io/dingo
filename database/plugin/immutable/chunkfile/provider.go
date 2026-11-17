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
type Config struct{}

// RegisterProvider registers the chunk-file provider with host. It is
// registered in every build and is the only storage.immutable provider.
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
			_ Config,
			deps immplugin.ProviderDependencies,
		) (immplugin.ImmutableDB, hostplugin.Instance, error) {
			svc := &service{
				dir: filepath.Join(deps.DataDir, immplugin.DirName),
			}
			return svc, hostplugin.Lifecycle{StopFunc: svc.stop}, nil
		},
	)
}

type service struct {
	dir string

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
	inner, err := immutable.OpenStore(s.dir, geom)
	if err != nil {
		return nil, err
	}
	s.open = &store{Store: inner, owner: s}
	return s.open, nil
}

func (s *service) ReadBlockAt(loc immutable.Location) (uint, []byte, error) {
	return immutable.ReadBlockAt(s.dir, loc)
}

func (s *service) ListFiles() ([]immutable.File, error) {
	return immutable.ListFiles(s.dir)
}

// stop closes the writer a holder left open and refuses later opens.
func (s *service) stop(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	if s.open == nil {
		return nil
	}
	st := s.open
	s.open = nil
	return st.Store.Close()
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

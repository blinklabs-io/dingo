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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ErrArtifactNotFound reports a key that no object occupies.
var ErrArtifactNotFound = errors.New("artifact not found")

// ArtifactStore holds the objects of produced Mithril snapshot artifacts.
// Keys are slash-separated relative paths; a key's directory-like prefix
// groups the objects of one snapshot.
type ArtifactStore interface {
	// Put writes the object at key, replacing any existing object. A
	// reader of key sees either the previous or the complete new object.
	Put(ctx context.Context, key string, r io.Reader) error
	// Open returns a seekable reader over the object at key, or an error
	// wrapping ErrArtifactNotFound.
	Open(ctx context.Context, key string) (io.ReadSeekCloser, error)
	// Subdirs returns the sorted names of the immediate directory-like
	// children of prefix ("" for the store root).
	Subdirs(ctx context.Context, prefix string) ([]string, error)
	// DeletePrefix removes the object whose key is prefix and every object
	// under prefix + "/". Removing nothing is not an error.
	DeletePrefix(ctx context.Context, prefix string) error
}

// OpenArtifactStore opens the store named by location: a filesystem
// directory, or an s3:// or gcs:// URI when the binary is built with
// dingo_extra_plugins. Only the artifact-producing and serving commands call
// it, so a mistyped scheme fails there rather than during a sync.
func OpenArtifactStore(
	ctx context.Context,
	location string,
) (ArtifactStore, error) {
	if location == "" {
		return nil, errors.New("artifact store location is empty")
	}
	u, err := url.Parse(location)
	if err == nil && u.Scheme != "" && u.Host != "" {
		return openRemoteArtifactStore(ctx, u)
	}
	return newLocalArtifactStore(location)
}

// localArtifactStore keeps artifacts under one directory, resolving every key
// through an os.Root so a crafted key cannot leave it.
type localArtifactStore struct {
	root *os.Root
}

func newLocalArtifactStore(dir string) (*localArtifactStore, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating artifact directory: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("opening artifact directory: %w", err)
	}
	return &localArtifactStore{root: root}, nil
}

// validKey reports whether key is a slash-separated relative path with no
// empty, "." or ".." segments.
func validKey(key string) bool {
	return key != "" && fs.ValidPath(key) && key != "."
}

func localName(key string) (string, error) {
	if key != "" && !fs.ValidPath(key) {
		return "", fmt.Errorf("invalid artifact key %q", key)
	}
	if key == "" {
		return ".", nil
	}
	return filepath.FromSlash(key), nil
}

func (s *localArtifactStore) Put(
	ctx context.Context,
	key string,
	r io.Reader,
) (err error) {
	if !validKey(key) {
		return fmt.Errorf("invalid artifact key %q", key)
	}
	name := filepath.FromSlash(key)
	if err := s.root.MkdirAll(filepath.Dir(name), 0o750); err != nil {
		return fmt.Errorf("creating artifact directory: %w", err)
	}
	// Written under a temporary name and renamed, so a concurrent reader
	// never serves a partial object.
	tmp := name + ".partial"
	f, err := s.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("creating artifact %s: %w", key, err)
	}
	defer func() {
		if err != nil {
			_ = s.root.Remove(tmp)
		}
	}()
	if _, err = io.Copy(f, &contextReader{ctx: ctx, r: r}); err != nil {
		_ = f.Close()
		return fmt.Errorf("writing artifact %s: %w", key, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("closing artifact %s: %w", key, err)
	}
	if err = s.root.Rename(tmp, name); err != nil {
		return fmt.Errorf("publishing artifact %s: %w", key, err)
	}
	return nil
}

func (s *localArtifactStore) Open(
	_ context.Context,
	key string,
) (io.ReadSeekCloser, error) {
	if !validKey(key) {
		return nil, fmt.Errorf("invalid artifact key %q", key)
	}
	f, err := s.root.Open(filepath.FromSlash(key))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrArtifactNotFound, key)
		}
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && info.IsDir() {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrArtifactNotFound, key)
	}
	return f, nil
}

func (s *localArtifactStore) Subdirs(
	_ context.Context,
	prefix string,
) ([]string, error) {
	name, err := localName(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return nil, err
	}
	entries, err := fs.ReadDir(s.root.FS(), filepath.ToSlash(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	return dirs, nil
}

func (s *localArtifactStore) DeletePrefix(
	_ context.Context,
	prefix string,
) error {
	name, err := localName(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return err
	}
	if name == "." {
		return errors.New("refusing to delete the artifact store root")
	}
	return s.root.RemoveAll(name)
}

// contextReader fails a copy once ctx is done, so cancelling a produce run
// stops an upload that is waiting on a slow source.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *contextReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

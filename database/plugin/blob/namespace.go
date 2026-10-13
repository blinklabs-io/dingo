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

package blob

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

const (
	// NamespaceFileName selects the cloud blob namespace paired with a local
	// metadata directory. Its inclusion in the directory swap makes the pair
	// become active at the same boundary.
	NamespaceFileName = ".dingo-blob-namespace"
	namespaceBytes    = 16
)

var syncNamespaceDir = fsyncdir.Sync

// namespacePublishedError reports a directory sync failure after the namespace
// marker was atomically replaced. Callers that are compensating a failed
// operation can still use the published marker to clean up its namespace.
type namespacePublishedError struct {
	err error
}

func (e *namespacePublishedError) Error() string {
	return e.err.Error()
}

func (e *namespacePublishedError) Unwrap() error {
	return e.err
}

// NamespacePublished reports whether WriteNamespace published the marker
// before returning err.
func NamespacePublished(err error) bool {
	var publishedErr *namespacePublishedError
	return errors.As(err, &publishedErr)
}

// NewNamespace returns a random identifier suitable for an isolated cloud
// blob namespace.
func NewNamespace() (string, error) {
	value := make([]byte, namespaceBytes)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate blob namespace: %w", err)
	}
	return hex.EncodeToString(value), nil
}

// WriteNamespace records namespace in dataDir. Existing markers are replaced
// so a metadata restore that recreates the staging directory can restore the
// same selection before the blob store is opened.
func WriteNamespace(dataDir string, namespace string) error {
	if err := validateNamespace(namespace); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create blob namespace directory: %w", err)
	}
	path := filepath.Join(dataDir, NamespaceFileName)
	tmp, err := os.CreateTemp(dataDir, NamespaceFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary blob namespace: %w", err)
	}
	tmpPath := tmp.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set blob namespace permissions: %w", err)
	}
	if _, err := tmp.WriteString(namespace + "\n"); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write blob namespace: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync blob namespace: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close blob namespace: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish blob namespace: %w", err)
	}
	renamed = true
	if err := syncNamespaceDir(dataDir); err != nil {
		return &namespacePublishedError{
			err: fmt.Errorf("sync blob namespace directory: %w", err),
		}
	}
	return nil
}

// ReadNamespace returns the namespace selected by dataDir. A missing marker
// selects the legacy object layout.
func ReadNamespace(dataDir string) (string, error) {
	if dataDir == "" {
		return "", nil
	}
	path := filepath.Join(dataDir, NamespaceFileName)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("open blob namespace: %w", err)
	}
	defer file.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(file, namespaceBytes*2+2))
	if err != nil {
		return "", fmt.Errorf("read blob namespace: %w", err)
	}
	if len(data) != namespaceBytes*2+1 || data[len(data)-1] != '\n' {
		return "", errors.New("invalid blob namespace marker length")
	}
	namespace := string(data[:len(data)-1])
	if err := validateNamespace(namespace); err != nil {
		return "", err
	}
	return namespace, nil
}

// NamespacePrefix appends namespace beneath a provider's configured prefix.
// An empty namespace preserves the legacy object layout byte for byte.
func NamespacePrefix(prefix string, namespace string) string {
	if namespace == "" {
		return prefix
	}
	return prefix + ".dingo-namespaces/" + namespace + "/"
}

func validateNamespace(namespace string) error {
	if len(namespace) != namespaceBytes*2 {
		return errors.New("invalid blob namespace identifier")
	}
	decoded, err := hex.DecodeString(namespace)
	if err != nil || hex.EncodeToString(decoded) != namespace {
		return errors.New("invalid blob namespace identifier")
	}
	return nil
}

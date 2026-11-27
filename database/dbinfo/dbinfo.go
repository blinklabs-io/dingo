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

// Package dbinfo reads and writes dingo.dbinfo, the small JSON file in a
// data directory that holds the ledger's persisted node settings: every gate
// database/nodesettings enforces, the metadata plugin among them.
//
// It lives outside every store so the configuration layer can read it before
// it opens anything. Resolving a metadata provider runs its migrations as a
// side effect, so opening the wrong one would create an empty database
// beside the real one; Badger takes a directory lock the real open needs a
// moment later; a cloud blob store needs credentials. The data directory is
// always local, whatever the blob provider: the cloud providers keep their
// namespace marker in it too.
//
// A data directory v0.94.2 wrote holds format 1, which records only the
// metadata plugin; its gates are still in the metadata store, and the first
// open by this release moves them here.
package dbinfo

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

// FileName is the file's fixed name within a data directory.
const FileName = "dingo.dbinfo"

// CurrentFormatVersion is the format this build writes. It holds the gates.
const CurrentFormatVersion = 2

// ReleasedFormatVersion is the format v0.94.2 writes, holding only the
// metadata plugin. Read accepts it so the first open can move the gates.
const ReleasedFormatVersion = 1

// ErrIncompleteSidecar marks a format 1 file whose metadata plugin is
// missing, null or empty. Read returns it instead of a zero Info, which is
// what an absent file reads as: a file that exists but never got its plugin
// name must not let the caller open whatever plugin is configured.
var ErrIncompleteSidecar = errors.New(
	"dbinfo: sidecar is missing its metadata plugin",
)

// Info is the file's content. It holds no credentials, connection strings
// or hostnames: a data directory is copied and inspected without any
// expectation that this file is sensitive.
type Info struct {
	FormatVersion int `json:"formatVersion"`
	// MetadataPlugin is format 1's only value. Format 2 records the plugin
	// as the metadata_plugin gate instead.
	MetadataPlugin string `json:"metadataPlugin,omitempty"`
	// Gates is format 2's node settings, by gate name.
	Gates nodesettings.Values `json:"gates,omitempty"`
}

// HoldsGates reports whether the file holds the node settings, which every
// file this build wrote does.
func (i Info) HoldsGates() bool {
	return i.FormatVersion == CurrentFormatVersion
}

// Plugin returns the recorded metadata plugin, or the empty string when none
// is recorded yet.
func (i Info) Plugin() string {
	if i.HoldsGates() {
		return i.Gates["metadata_plugin"]
	}
	return i.MetadataPlugin
}

// Path returns the file's path within dataDir.
func Path(dataDir string) string {
	return filepath.Join(dataDir, FileName)
}

// WriteGates replaces the file in dataDir with gates in the current format.
func WriteGates(dataDir string, gates nodesettings.Values) error {
	if gates == nil {
		gates = nodesettings.Values{}
	}
	return Write(dataDir, Info{
		FormatVersion: CurrentFormatVersion,
		Gates:         maps.Clone(gates),
	})
}

// Write persists info to the file in dataDir via a temp file and a rename,
// then fsyncs the directory (internal/fsyncdir) so the new entry survives a
// crash and not just its content. A reader sees the old file or the new one,
// never a mix. It creates dataDir when it does not exist yet.
//
// Write does not stamp or validate info.FormatVersion.
func Write(dataDir string, info Info) error {
	if dataDir == "" {
		return errors.New("dbinfo: dataDir must not be empty")
	}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal dbinfo: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create data directory %q: %w", dataDir, err)
	}

	path := Path(dataDir)
	tmp, err := os.CreateTemp(dataDir, FileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp dbinfo in %q: %w", dataDir, err)
	}
	tmpPath := tmp.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp dbinfo %q: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp dbinfo %q: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp dbinfo %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp dbinfo to %q: %w", path, err)
	}
	renamed = true
	return fsyncdir.Sync(dataDir)
}

// Read reads and validates the file in dataDir. A missing file returns a
// zero Info and a nil error: a fresh data directory has none. An
// unrecognised FormatVersion is an error, and so is a format 1 file without
// its plugin (ErrIncompleteSidecar).
func Read(dataDir string) (Info, error) {
	path := Path(dataDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Info{}, nil
		}
		return Info{}, fmt.Errorf("read dbinfo %q: %w", path, err)
	}
	var info Info
	if err := json.Unmarshal(data, &info); err != nil {
		return Info{}, fmt.Errorf("parse dbinfo %q: %w", path, err)
	}
	switch info.FormatVersion {
	case CurrentFormatVersion:
		if info.Gates == nil {
			info.Gates = nodesettings.Values{}
		}
	case ReleasedFormatVersion:
		if info.MetadataPlugin == "" {
			return Info{}, fmt.Errorf("dbinfo %q: %w", path, ErrIncompleteSidecar)
		}
	default:
		return Info{}, fmt.Errorf(
			"dbinfo %q: unrecognised format version %d",
			path, info.FormatVersion,
		)
	}
	return info, nil
}

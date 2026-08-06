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

// Package dbinfo records, in a small JSON sidecar file beside a dingo data
// directory, which metadata plugin produced the database it belongs to.
//
// The metadata store itself is the single source of truth for every
// persisted node setting, including which plugin owns the data --
// specifically the metadata_plugin gate in node_settings_gate (see
// database/nodesettings). This sidecar exists only so the configuration
// layer can identify the right plugin to open *before* it has opened
// anything: resolving a metadata provider runs its own migrations as a side
// effect of merely starting it, so opening the wrong provider would
// silently create a brand new, empty database beside the real one instead
// of ever reaching the gate table that would have caught the mismatch.
//
// A database that predates this sidecar, or one that has otherwise lost it,
// has no way to signal that up front, so its absence is never an error --
// it just means the caller falls back to opening its configured plugin
// directly, the same as it always has.
package dbinfo

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

// FileName is the sidecar's fixed name within a data directory.
const FileName = "dingo.dbinfo"

// CurrentFormatVersion is the only FormatVersion this build writes and
// accepts on Read.
const CurrentFormatVersion = 1

// ErrIncompleteSidecar marks a sidecar whose FormatVersion this build
// recognises but whose MetadataPlugin is missing, JSON null, or the empty
// string. Read returns it (wrapped with the sidecar's path) instead of a
// zero Info and a nil error, because a zero Info is exactly what Read
// returns for a sidecar that is simply absent -- and the caller,
// internal/settingsresolve's checkMetadataPluginSidecar, treats "absent" as
// "nothing to check, proceed." A sidecar file that exists but never got a
// plugin name written into it (interrupted Write, hand-edited, or a future
// writer bug) must not be indistinguishable from that, since proceeding
// resolves whatever metadata plugin is configured and runs its migrations
// as a side effect -- silently creating a fresh, empty database beside the
// real one if the configured plugin is wrong. Callers that want the old
// "advisory, ignore it" behavior for this case specifically should not get
// it; see checkMetadataPluginSidecar's errors.Is handling.
var ErrIncompleteSidecar = errors.New(
	"dbinfo: sidecar is missing its metadata plugin",
)

// Info is the sidecar's entire content. It deliberately carries nothing
// beyond a format version and the metadata plugin name -- no credentials,
// connection string, or hostname -- since a data directory may be backed
// up, copied, or inspected without any expectation that this file holds
// anything sensitive.
type Info struct {
	FormatVersion  int    `json:"formatVersion"`
	MetadataPlugin string `json:"metadataPlugin"`
}

// Path returns the sidecar's path within dataDir.
func Path(dataDir string) string {
	return filepath.Join(dataDir, FileName)
}

// Write persists info to the sidecar file in dataDir via a temp-file-then-
// rename, fsyncing the directory afterward (internal/fsyncdir) so the new
// file's directory entry is durable across a crash and not just its own
// content -- matching database/lifecycle/manifest.go's WriteManifest.
//
// Write does not stamp or validate info.FormatVersion; callers pass
// CurrentFormatVersion themselves.
func Write(dataDir string, info Info) error {
	if dataDir == "" {
		return errors.New("dbinfo: dataDir must not be empty")
	}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal dbinfo: %w", err)
	}

	path := Path(dataDir)
	tmp, err := os.CreateTemp(dataDir, FileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp dbinfo in %q: %w", dataDir, err)
	}
	tmpPath := tmp.Name()
	renamed := false
	// Best-effort: don't leave the temp file behind for a future Write or
	// directory listing to trip over if anything below fails before the
	// rename. No-op once renamed, since tmpPath no longer exists.
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

	// A file's own fsync does not guarantee its directory entry is
	// persisted; sync the directory itself so the rename above is durable
	// too, not just atomic.
	return fsyncdir.Sync(dataDir)
}

// Read reads and validates the sidecar at dataDir. A missing file returns a
// zero Info and a nil error: the sidecar is advisory, and a database that
// predates it (or has otherwise lost it) must still open normally. An
// unrecognised FormatVersion is an error, since this build has no way to
// know what a newer or unknown format actually means. A sidecar whose
// FormatVersion this build does recognise but whose MetadataPlugin is
// missing, JSON null, or empty returns ErrIncompleteSidecar (wrapped with
// path) rather than a zero Info and a nil error -- see ErrIncompleteSidecar's
// doc comment for why that distinction matters.
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
	if info.FormatVersion != CurrentFormatVersion {
		return Info{}, fmt.Errorf(
			"dbinfo %q: unrecognised format version %d",
			path, info.FormatVersion,
		)
	}
	if info.MetadataPlugin == "" {
		return Info{}, fmt.Errorf("dbinfo %q: %w", path, ErrIncompleteSidecar)
	}
	return info, nil
}

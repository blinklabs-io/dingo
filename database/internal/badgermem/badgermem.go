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

// Package badgermem repairs badger memtable files an interrupted shutdown
// leaves behind, before badger opens the directory.
package badgermem

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// RemoveEmpty deletes zero-length memtable write-ahead files (*.mem) in dir
// and returns the names it removed. A missing dir is not an error.
//
// A process killed while badger creates a new memtable file leaves it empty.
// badger's openMemTables (memtable.go) opens every existing *.mem, and for an
// empty one the mmap helper returns z.NewFile, which openMemTables treats as
// fatal ("while opening fid: N err: Create a new file"), so the store cannot
// open. An empty write-ahead file holds no entries, and badger would treat it
// as new anyway, so removing it loses nothing.
func RemoveEmpty(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".mem") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return removed, err
		}
		if info.Size() != 0 {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return removed, err
		}
		removed = append(removed, e.Name())
	}
	return removed, nil
}

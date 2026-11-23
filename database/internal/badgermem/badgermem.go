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

// Package badgermem opens badger stores that an interrupted shutdown left
// with an empty memtable file.
package badgermem

import (
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

// Open opens the store described by opts. When the first attempt fails only
// because of an empty memtable file, it opens once more and reports retried.
//
// A process killed while badger creates a new memtable write-ahead file (*.mem)
// leaves it empty. badger's openMemTables (memtable.go) treats the z.NewFile
// result for such a file as fatal ("while opening fid: N err: Create a new
// file"), but the failed attempt has already extended the file to its working
// size, so the second attempt opens it as an empty memtable. That repair
// happens inside badger while it holds its directory lock, so a directory
// another process has open is never modified: its lock check fails first, and
// that error is not retried.
func Open(opts badger.Options) (db *badger.DB, retried bool, err error) {
	db, err = badger.Open(opts)
	if err == nil || !emptyMemtable(err) {
		return db, false, err
	}
	db, err = badger.Open(opts)
	return db, true, err
}

// emptyMemtable reports whether err is badger's empty-memtable open failure.
// badger flattens the cause into the message (y.Wrap formats it with %+v
// outside debug builds), so errors.Is cannot see z.NewFile.
func emptyMemtable(err error) bool {
	msg := err.Error()
	return strings.HasPrefix(msg, "while opening memtables") &&
		strings.HasSuffix(msg, z.NewFile.Error())
}

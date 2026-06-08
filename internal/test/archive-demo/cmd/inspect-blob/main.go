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

// inspect-blob is a CLI used by the archive-demo integration test to
// verify whether a block (slot, hash) is present in a Dingo node's
// local Badger blob store. It exits 0 if present, 1 if absent, 2 on
// any other error.
//
// The Dingo container running the blob store must be stopped before
// invoking this tool (Badger is a single-writer database).
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
)

func main() {
	dir := flag.String("dir", "", "path to badger blob data directory")
	slot := flag.Uint64("slot", 0, "block slot")
	hashHex := flag.String("hash", "", "block hash (hex)")
	flag.Parse()

	if *dir == "" || *hashHex == "" {
		fmt.Fprintln(
			os.Stderr,
			"usage: inspect-blob -dir DIR -slot N -hash HEX",
		)
		os.Exit(2)
	}
	hash, err := hex.DecodeString(*hashHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad hash: %v\n", err)
		os.Exit(2)
	}

	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob, "badger", "data-dir", *dir,
	); err != nil {
		fmt.Fprintf(os.Stderr, "set data-dir: %v\n", err)
		os.Exit(2)
	}
	// Run badger in API storage mode so we don't trip over core-mode
	// mmap defaults that assume the dingo node owns the process.
	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob, "badger", "storage-mode", "api",
	); err != nil {
		fmt.Fprintf(os.Stderr, "set storage-mode: %v\n", err)
		os.Exit(2)
	}

	store, err := blob.New("badger")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open badger: %v\n", err)
		os.Exit(2)
	}
	defer func() { _ = store.Close() }()

	txn := store.NewTransaction(false)
	defer func() { _ = txn.Rollback() }()

	cbor, _, err := store.GetBlock(txn, *slot, hash)
	switch {
	case err == nil && len(cbor) > 0:
		fmt.Printf("present: slot=%d hash=%s bytes=%d\n", *slot, *hashHex, len(cbor))
		os.Exit(0)
	case err == nil:
		fmt.Println("absent: nil cbor")
		os.Exit(1)
	case isNotFound(err):
		fmt.Printf("absent: %v\n", err)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "lookup error: %v\n", err)
		os.Exit(2)
	}
}

// isNotFound treats both "not found" and "history expired" errors as the
// locally-absent case. Expired history means the bp value has been replaced
// with a small marker so the bark proxy can resolve the block remotely; from
// the perspective of "is the real CBOR stored locally?" the answer is no,
// same as a hard delete.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") ||
		strings.Contains(msg, "errkeynotfound") ||
		strings.Contains(msg, "history expired")
}

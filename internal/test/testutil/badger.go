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

package testutil

const (
	// TestBadgerValueLogFileSize and TestBadgerMemTableSize are the file
	// sizes a test's badger blob store should ask for, well below the
	// production defaults (1 GiB and 128 MiB).
	//
	// badger sizes both files up front when it opens an on-disk store: the
	// value log is truncated to ValueLogFileSize and the memtable WAL to
	// MemTableSize. On Linux and macOS those are sparse, so the space is
	// only charged as it is written and the production sizes cost a test
	// nothing. On Windows the space is really reserved the moment the
	// store opens -- and badger maps the value log at twice
	// ValueLogFileSize so the last entry always fits (badger/v4
	// value.go:536), making that 2 GiB per store at the default rather
	// than 1 -- so enough concurrent stores fill a CI runner's disk.
	// badger then fails every open with "There is not enough space on the
	// disk" out of valueLog.open, and the affected t.TempDir cleanup fails
	// behind it because the mapping is still held.
	//
	// A test writes kilobytes to a few megabytes, so it asks for files it
	// can actually fill. Anything larger only reserves space.
	TestBadgerValueLogFileSize = 16 * 1024 * 1024
	TestBadgerMemTableSize     = 8 * 1024 * 1024
)

// BadgerBlobConfig returns the badger blob provider config a test should
// use, sized by TestBadgerValueLogFileSize and TestBadgerMemTableSize. Pass
// it as the provider config to plugin.Resolve wherever a test opens an
// on-disk badger store; dbtest.NewDatabase applies it for you.
func BadgerBlobConfig() map[string]any {
	return map[string]any{
		"valueLogFileSize": TestBadgerValueLogFileSize,
		"memTableSize":     TestBadgerMemTableSize,
	}
}

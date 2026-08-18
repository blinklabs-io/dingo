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

package immutable

import (
	"bytes"
	"io"
	"os"
)

// entryReader is what the chunk and index readers need of a file: read, seek,
// size, close.
//
// It is an interface rather than *os.File so a verified database can hand the
// readers the bytes it checked instead of a descriptor they read again. A
// descriptor is a reference to an inode, and a digest taken through one
// describes the inode's contents at the moment it was hashed, not at the
// moment the parser gets there — anyone who can write to the file can change
// what it holds in between, and same-inode writes are visible through a
// descriptor that has merely been rewound. Bytes already read are the parser's
// own; nothing can reach them. See ImmutableDb.openEntry.
type entryReader interface {
	io.ReadSeeker
	io.Closer
	// Size is the entry's length, the way Stat().Size() is a file's.
	Size() (int64, error)
}

// fileEntry reads straight from the filesystem, for a database opened without
// digests — an ordinary local ImmutableDB, where the files are the node's own
// and there is nothing to verify them against. Reads go to the descriptor as
// they always did, so a chunk is never held in memory.
type fileEntry struct {
	file *os.File
}

func (f fileEntry) Read(p []byte) (int, error) { return f.file.Read(p) }

func (f fileEntry) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f fileEntry) Close() error { return f.file.Close() }

func (f fileEntry) Size() (int64, error) {
	stat, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// bytesEntry serves an entry that was read and verified in one go, so the
// bytes the digest covered are the bytes the parser walks.
//
// This holds one entry in memory at a time, bounded by the certified size of
// the file rather than by its actual size — see readVerifiedEntry, which
// establishes the one before allocating for the other.
type bytesEntry struct {
	reader *bytes.Reader
}

func newBytesEntry(data []byte) *bytesEntry {
	return &bytesEntry{reader: bytes.NewReader(data)}
}

func (b *bytesEntry) Read(p []byte) (int, error) { return b.reader.Read(p) }

func (b *bytesEntry) Seek(offset int64, whence int) (int64, error) {
	return b.reader.Seek(offset, whence)
}

// Close releases nothing: the descriptor was closed when the bytes were read,
// and what remains is memory the garbage collector owns.
func (b *bytesEntry) Close() error { return nil }

func (b *bytesEntry) Size() (int64, error) { return b.reader.Size(), nil }

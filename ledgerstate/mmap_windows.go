//go:build windows

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

package ledgerstate

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func mmapReadOnly(path string) ([]byte, func(), error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	return mmapFile(file)
}

// mmapFile maps an already-open file read-only.
//
// Taking the file rather than a path is what lets a caller map a file it
// resolved through a directory handle: the bytes then come from the file it
// opened, not from whatever occupies that name by the time the mapping runs.
//
// The caller keeps ownership of file and may close it as soon as this returns —
// the mapping keeps the pages alive independently of the descriptor.
func mmapFile(file *os.File) ([]byte, func(), error) {
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, nil, errors.New("empty file")
	}
	maxInt := int64(int(^uint(0) >> 1))
	if size > maxInt {
		return nil, nil, fmt.Errorf(
			"file too large to map into memory: %d bytes",
			size,
		)
	}
	length := int(size) //nolint:gosec // size <= maxInt checked above

	mapping, err := windows.CreateFileMapping(
		windows.Handle(file.Fd()),
		nil,
		windows.PAGE_READONLY,
		0,
		0,
		nil,
	)
	if err != nil {
		return nil, nil, err
	}
	defer windows.CloseHandle(mapping) //nolint:errcheck

	addr, err := windows.MapViewOfFile(
		mapping,
		windows.FILE_MAP_READ,
		0,
		0,
		uintptr(length),
	)
	if err != nil {
		return nil, nil, err
	}
	data := unsafe.Slice( //nolint:gosec // Windows mmap view as []byte
		(*byte)(unsafe.Pointer(addr)),
		length,
	)
	if data == nil {
		_ = windows.UnmapViewOfFile(addr)
		return nil, nil, errors.New("mmap returned nil data")
	}

	return data, func() {
		_ = windows.UnmapViewOfFile(addr)
	}, nil
}

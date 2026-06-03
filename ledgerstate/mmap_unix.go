//go:build !windows

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

	"golang.org/x/sys/unix"
)

func mmapReadOnly(path string) ([]byte, func(), error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()
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
	data, err := unix.Mmap(
		int(file.Fd()),
		0,
		int(size),
		unix.PROT_READ,
		unix.MAP_PRIVATE,
	)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, errors.New("mmap returned nil data")
	}
	mappedFile := file
	file = nil
	if err := mappedFile.Close(); err != nil {
		_ = unix.Munmap(data)
		return nil, nil, err
	}
	return data, func() {
		_ = unix.Munmap(data)
	}, nil
}

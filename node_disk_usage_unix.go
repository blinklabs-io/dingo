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

//go:build !windows

package dingo

import (
	"io/fs"
	"syscall"
)

// allocatedBytes returns the disk a file occupies: its allocated 512-byte
// blocks. A preallocated or sparse file's length says nothing about it.
func allocatedBytes(info fs.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Blocks) * 512 // #nosec G115 -- a block count is not negative
	}
	return uint64(info.Size()) // #nosec G115 -- file size
}

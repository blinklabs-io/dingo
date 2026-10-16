//go:build freebsd

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

package node

import (
	"errors"
	"math"

	"golang.org/x/sys/unix"
)

func availableDiskBytes(path string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 {
		return 0, errors.New("filesystem reported an invalid block size")
	}
	if stat.Bavail < 0 {
		return 0, errors.New("filesystem reported a negative available block count")
	}
	availableBlocks := uint64(stat.Bavail)
	if availableBlocks > math.MaxUint64/uint64(stat.Bsize) {
		return math.MaxUint64, nil
	}
	return availableBlocks * uint64(stat.Bsize), nil
}

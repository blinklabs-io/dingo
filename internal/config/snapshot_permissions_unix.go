//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func restrictSnapshotDirectory(dir *os.File) error {
	info, err := dir.Stat()
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0o200 == 0 {
		return errors.New("directory is not writable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("directory owner is unavailable")
	}
	if uid := int64(stat.Uid); uid != int64(os.Geteuid()) && uid != 0 {
		return fmt.Errorf("directory is owned by UID %d", stat.Uid)
	}
	return dir.Chmod(0o700)
}

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

package securefs

import (
	"os"
	"slices"
)

// UnsafelyWritableAncestor reports whether a directory with this mode and
// group lets another user replace its entries. A sticky directory is safe. So
// is the volume root of a Kubernetes pod with securityContext.fsGroup: the
// kubelet makes it group-owned by the fsGroup GID, group-writable and setgid,
// and adds that GID to the process's groups. Such a directory is accepted only
// while its group is the process's effective or a supplementary GID and it is
// not writable by other.
func UnsafelyWritableAncestor(mode os.FileMode, gid uint32) bool {
	return unsafelyWritableAncestor(mode, gid, os.Getegid, os.Getgroups)
}

func unsafelyWritableAncestor(
	mode os.FileMode,
	gid uint32,
	getegid func() int,
	getgroups func() ([]int, error),
) bool {
	perm := mode.Perm()
	if perm&0o022 == 0 || mode&os.ModeSticky != 0 {
		return false
	}
	if perm&0o002 != 0 || mode&os.ModeSetgid == 0 {
		return true
	}
	return !processHasGroup(int64(gid), getegid, getgroups)
}

func processHasGroup(
	gid int64,
	getegid func() int,
	getgroups func() ([]int, error),
) bool {
	if gid == int64(getegid()) {
		return true
	}
	groups, err := getgroups()
	if err != nil {
		return false
	}
	return slices.ContainsFunc(groups, func(g int) bool {
		return int64(g) == gid
	})
}

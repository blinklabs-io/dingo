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
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnsafelyWritableAncestor(t *testing.T) {
	t.Parallel()
	const (
		egid         = 1000
		supplemental = 2000
		foreign      = 3000
	)
	getegid := func() int { return egid }
	groups := func() ([]int, error) { return []int{egid, supplemental}, nil }
	failingGroups := func() ([]int, error) {
		return nil, errors.New("getgroups failed")
	}
	setgid := os.ModeSetgid
	for _, tc := range []struct {
		name      string
		mode      os.FileMode
		gid       uint32
		getgroups func() ([]int, error)
		want      bool
	}{
		{name: "private", mode: 0o755, gid: foreign, want: false},
		{name: "sticky", mode: os.ModeSticky | 0o777, gid: foreign, want: false},
		{name: "fsGroup egid", mode: setgid | 0o770, gid: egid, want: false},
		{
			name: "fsGroup supplementary", mode: setgid | 0o770,
			gid: supplemental, want: false,
		},
		{name: "foreign group", mode: setgid | 0o770, gid: foreign, want: true},
		{name: "world writable", mode: setgid | 0o777, gid: egid, want: true},
		{name: "no setgid", mode: 0o770, gid: egid, want: true},
		{
			name: "getgroups error", mode: setgid | 0o770, gid: supplemental,
			getgroups: failingGroups, want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			getgroups := groups
			if tc.getgroups != nil {
				getgroups = tc.getgroups
			}
			require.Equal(
				t, tc.want,
				unsafelyWritableAncestor(tc.mode, tc.gid, getegid, getgroups),
			)
		})
	}
}

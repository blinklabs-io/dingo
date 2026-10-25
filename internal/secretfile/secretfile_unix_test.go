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

//go:build unix

package secretfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/keystore"
)

type ownerFileInfo struct {
	os.FileInfo
	uid uint32
}

func (i ownerFileInfo) Sys() any {
	return &syscall.Stat_t{Uid: i.uid}
}

func TestReadRejectsInsecurePermissions(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("s3cret"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Read(path)
	if !errors.Is(err, keystore.ErrInsecureFileMode) {
		t.Fatalf("Read(insecure file) error = %v", err)
	}
}

func TestReadRejectsFIFOWithoutBlocking(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := Read(path)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, keystore.ErrNotRegularFile) {
			t.Fatalf("Read(fifo) error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked opening a FIFO with no writer")
	}
}

func TestReadRejectsDevice(t *testing.T) {
	t.Parallel()
	_, err := Read("/dev/null")
	if !errors.Is(err, keystore.ErrNotRegularFile) {
		t.Fatalf("Read(device) error = %v", err)
	}
}

func TestReadRejectsWritableParentDirectory(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	path := filepath.Join(parent, "secret")
	if err := os.WriteFile(path, []byte("s3cret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o777); err != nil {
		t.Fatal(err)
	}
	_, err := Read(path)
	if !errors.Is(err, keystore.ErrInsecureFileMode) {
		t.Fatalf("Read(file in writable directory) error = %v", err)
	}
}

func TestReadAcceptsStickyWritableParentDirectory(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	path := filepath.Join(parent, "secret")
	if err := os.WriteFile(path, []byte("s3cret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o777|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != "s3cret" {
		t.Fatalf("Read = %q, want %q", got, "s3cret")
	}
}

func TestRequireTrustedOwner(t *testing.T) {
	t.Parallel()
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := requireTrustedOwner(
		"secret",
		ownerFileInfo{FileInfo: info, uid: 0},
	); err != nil {
		t.Fatalf("root owner: %v", err)
	}
	// #nosec G115 -- test UID is bounded.
	foreignUID := uint32(os.Geteuid() + 1)
	err = requireTrustedOwner(
		"secret",
		ownerFileInfo{FileInfo: info, uid: foreignUID},
	)
	if !errors.Is(err, keystore.ErrInsecureFileOwner) {
		t.Fatalf("foreign owner error = %v", err)
	}
}

// A value file may be shared with its group, as an orchestrator delivers it
// (a Kubernetes secret volume with mode 0440 and an fsGroup), but never with
// other users. The mode is set after writing so the umask cannot change it.
func TestReadPermissionModes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		mode   os.FileMode
		accept bool
	}{
		{mode: 0o400, accept: true},
		{mode: 0o600, accept: true},
		{mode: 0o440, accept: true},
		{mode: 0o640, accept: true},
		{mode: 0o444},
		{mode: 0o644},
		{mode: 0o604},
		{mode: 0o602},
		{mode: 0o601},
		{mode: 0o666},
	} {
		t.Run(tc.mode.String(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if err := os.Chmod(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "secret")
			if err := os.WriteFile(path, []byte("s3cret\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, tc.mode); err != nil {
				t.Fatal(err)
			}
			got, err := Read(path)
			if tc.accept {
				if err != nil {
					t.Fatalf("Read(mode %04o) error = %v", tc.mode, err)
				}
				if got != "s3cret" {
					t.Fatalf("Read(mode %04o) = %q", tc.mode, got)
				}
				return
			}
			if !errors.Is(err, keystore.ErrInsecureFileMode) {
				t.Fatalf("Read(mode %04o) error = %v", tc.mode, err)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Fatalf("Read(mode %04o) error leaks contents: %v", tc.mode, err)
			}
			if strings.Contains(err.Error(), "key file") {
				t.Fatalf("Read(mode %04o) error names a key file: %v", tc.mode, err)
			}
		})
	}
}

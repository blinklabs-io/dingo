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
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/keystore"
)

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

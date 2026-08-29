// Copyright 2025 Blink Labs Software
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

//go:build dingo_extra_plugins

package gcs

import (
	"errors"
	"testing"
)

// fakeCloser stands in for the real *storage.Writer, which needs a live GCS
// bucket (or credentials) to construct at all. joinCloseErr is tested against
// this controllable io.Closer instead of standing up real cloud storage.
type fakeCloser struct {
	err error
}

func (f *fakeCloser) Close() error {
	return f.err
}

// When the write already failed and the writer also fails to close, both
// errors matter: the close failure can be the only signal that the partial
// upload was never aborted cleanly. Neither may be dropped in favor of the
// other.
func TestJoinCloseErrJoinsOnCloseFailure(t *testing.T) {
	writeErr := errors.New("write failed")
	closeErr := errors.New("upload aborted")

	got := joinCloseErr(writeErr, &fakeCloser{err: closeErr})

	if !errors.Is(got, writeErr) {
		t.Fatalf("expected joined error to wrap the write error: %v", got)
	}
	if !errors.Is(got, closeErr) {
		t.Fatalf("expected joined error to wrap the close error: %v", got)
	}
}

// A clean close must not manufacture a joined error out of nothing -- the
// caller's original write error should come back unwrapped.
func TestJoinCloseErrReturnsOriginalOnCloseSuccess(t *testing.T) {
	writeErr := errors.New("write failed")

	got := joinCloseErr(writeErr, &fakeCloser{err: nil})

	if got != writeErr {
		t.Fatalf("expected original error identity, got: %v", got)
	}
}

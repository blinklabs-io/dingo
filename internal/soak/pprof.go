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

package soak

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Profiles captured by Snapshot. goroutine uses debug=1 so the file is
// readable text grouped by stack; heap is the binary profile for go tool pprof.
var profiles = []struct{ name, path string }{
	{"goroutine", "/debug/pprof/goroutine?debug=1"},
	{"heap", "/debug/pprof/heap"},
}

// Snapshot saves the node's goroutine and heap profiles from the pprof
// listener at debugURL (scheme and host, no path) into dir, named by at.
func Snapshot(
	ctx context.Context,
	client *http.Client,
	debugURL string,
	dir string,
	at time.Time,
) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	stamp := at.UTC().Format("20060102T150405Z")
	for _, p := range profiles {
		body, err := get(ctx, client, strings.TrimRight(debugURL, "/")+p.path)
		if err != nil {
			return err
		}
		err = writeProfile(filepath.Join(dir, fmt.Sprintf("%s-%s.pprof", p.name, stamp)), body)
		body.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func writeProfile(path string, src io.Reader) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, src); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

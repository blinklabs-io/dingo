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

package database

import (
	"context"
	"fmt"
	"io"

	"github.com/blinklabs-io/dingo/database/immutable"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
)

// LockImmutable blocks promotion and operator truncation of the ImmutableDB
// until the returned function is called, so the chunk files and the recorded
// tip cannot change underneath a snapshot or a restore. It returns once the
// lock is held or ctx is done, and is a no-op without an ImmutableDB.
//
// Take it before pausing commits: promotion holds the same lock while it waits
// on a blob commit, so the opposite order can deadlock.
func (d *Database) LockImmutable(ctx context.Context) (func(), error) {
	rt := d.immutable
	if rt == nil {
		return func() {}, nil
	}
	acquired := make(chan struct{})
	abandoned := make(chan struct{})
	go func() {
		rt.mu.Lock()
		select {
		case acquired <- struct{}{}:
		case <-abandoned:
			rt.mu.Unlock()
		}
	}()
	select {
	case <-acquired:
		return rt.mu.Unlock, nil
	case <-ctx.Done():
		close(abandoned)
		return nil, ctx.Err()
	}
}

// ImmutableLayout is the ImmutableDB as a snapshot must record it.
type ImmutableLayout struct {
	// State is the recorded tip and geometry.
	State ImmutableState
	// Dir is the chunk directory.
	Dir string
	// Files lists every chunk, primary and secondary file with the length to
	// capture.
	Files []immutable.File
}

// ImmutableFilesLocal reports whether the provider keeps its durable files in
// ImmutableDir, so a snapshot may link them. A provider that publishes chunks
// elsewhere only stages its writable tail there.
func (d *Database) ImmutableFilesLocal() bool {
	if d.immutable == nil {
		return false
	}
	_, remote := d.immutable.svc.(immplugin.ChunkPublisher)
	return !remote
}

// ReadImmutableFile streams one ImmutableDB file through its storage provider.
func (d *Database) ReadImmutableFile(
	ctx context.Context,
	name string,
	dst io.Writer,
) (int64, error) {
	if d.immutable == nil {
		return 0, ErrImmutableNotEnabled
	}
	return d.immutable.svc.ReadFile(ctx, name, dst)
}

// ImmutableLayout reads the recorded state and makes the chunk files durable
// so their lengths describe a complete prefix. The caller holds LockImmutable
// and the commit barrier. ok is false when no block has been promoted, in
// which case nothing needs capturing beyond the blob state.
func (d *Database) ImmutableLayout() (layout ImmutableLayout, ok bool, err error) {
	rt := d.immutable
	if rt == nil {
		return ImmutableLayout{}, false, nil
	}
	st := rt.snapshot()
	if !st.HasTip {
		return ImmutableLayout{}, false, nil
	}
	store, err := d.openImmutableStore()
	if err != nil {
		return ImmutableLayout{}, false, err
	}
	if err := store.Sync(); err != nil {
		return ImmutableLayout{}, false, err
	}
	files, err := rt.svc.ListFiles()
	if err != nil {
		return ImmutableLayout{}, false, fmt.Errorf(
			"list immutable files: %w",
			err,
		)
	}
	return ImmutableLayout{State: st, Dir: rt.dir, Files: files}, true, nil
}

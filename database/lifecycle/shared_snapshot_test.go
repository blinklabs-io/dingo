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

package lifecycle_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

// Tests that only need a valid snapshot to feed a restore the code under test
// rejects or inspects used to each open a database and take a snapshot, which
// costs several seconds under the race detector. One snapshot of a database
// holding block 1 is taken per test binary and every caller gets its own copy,
// because some of them rewrite the manifest.
var oneBlockSnapshot struct {
	once sync.Once
	dir  string
	err  error
}

// twoBlockCloudSnapshot is a snapshot of a two-block database, signed with
// testTrustKey and uploaded once to a fake cloud directory. Each caller gets
// its own copy of the uploaded objects because tests tamper with them.
var twoBlockCloudSnapshot struct {
	once     sync.Once
	root     string
	manifest lifecycle.Manifest
	err      error
}

func init() {
	lifecycle.RegisterSharedFixtureCleanup(func() {
		if oneBlockSnapshot.dir != "" {
			_ = os.RemoveAll(oneBlockSnapshot.dir)
		}
		if twoBlockCloudSnapshot.root != "" {
			_ = os.RemoveAll(twoBlockCloudSnapshot.root)
		}
	})
}

func buildTwoBlockCloudSnapshot(t *testing.T) {
	t.Helper()
	root, err := os.MkdirTemp("", "dingo-lifecycle-cloud-")
	twoBlockCloudSnapshot.root = root
	if err != nil {
		twoBlockCloudSnapshot.err = err
		return
	}
	src := newTestDB(t)
	for _, b := range []*struct {
		id   uint64
		fill byte
	}{{1, 0x01}, {2, 0x02}} {
		if err := src.BlockCreate(testBlock(b.id, b.fill), nil); err != nil {
			twoBlockCloudSnapshot.err = err
			return
		}
	}
	setFakeCloudBackingDir(t, root)
	m, err := lifecycle.SnapshotToCloud(
		context.Background(), testDestinationRegistry, src,
		snapshotTestDir(t, "snap"), lifecycle.TriggerManual, "test",
		"badger", "sqlite", "faketest://bucket/prefix", "", "",
		lifecycle.WithManifestKey(testTrustKey),
	)
	twoBlockCloudSnapshot.manifest, twoBlockCloudSnapshot.err = m, err
}

// copyTwoBlockCloudSnapshot places a private copy of the shared uploaded
// objects in a fresh fake cloud backing directory and returns the snapshot
// URI, the object directory and the manifest.
func copyTwoBlockCloudSnapshot(
	t *testing.T,
) (string, string, lifecycle.Manifest) {
	t.Helper()
	twoBlockCloudSnapshot.once.Do(func() {
		// A subtest, so the fake cloud fixture lock taken while building is
		// released before this test takes it again for its own copy.
		t.Run("build shared cloud snapshot", buildTwoBlockCloudSnapshot)
	})
	require.NoError(t, twoBlockCloudSnapshot.err)
	backing := t.TempDir()
	setFakeCloudBackingDir(t, backing)
	objects := filepath.Join(backing, "prefix", "snap")
	copyFlatDir(
		t, filepath.Join(twoBlockCloudSnapshot.root, "prefix", "snap"), objects,
	)
	return "faketest://bucket/prefix/snap", objects, twoBlockCloudSnapshot.manifest
}

// copyOneBlockSnapshot returns a private copy of the shared snapshot, named
// "snap1" in a fresh owner-only parent as snapshotTestDir would place it.
func copyOneBlockSnapshot(t *testing.T) string {
	t.Helper()
	oneBlockSnapshot.once.Do(func() {
		oneBlockSnapshot.dir, oneBlockSnapshot.err = buildOneBlockSnapshot(t)
	})
	require.NoError(t, oneBlockSnapshot.err)
	dst := snapshotTestDir(t, "snap1")
	src := filepath.Join(oneBlockSnapshot.dir, "snap1")
	require.NoError(t, filepath.WalkDir(src, func(
		path string, d fs.DirEntry, err error,
	) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, info.Mode().Perm())
	}))
	return dst
}

func buildOneBlockSnapshot(t *testing.T) (string, error) {
	root, err := os.MkdirTemp("", "dingo-lifecycle-snapshot-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return root, err
	}
	// The source database lives in the first caller's temp dir; the snapshot
	// is a self-contained copy, so it can be dropped once Snapshot returns.
	src := newTestDB(t)
	if err := src.BlockCreate(testBlock(1, 0x01), nil); err != nil {
		return root, err
	}
	_, err = lifecycle.Snapshot(
		context.Background(),
		src,
		filepath.Join(root, "snap1"),
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	return root, err
}

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

//go:build dingo_extra_plugins

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	// Registered as "gcs" (not "gs") to match the scheme
	// database/plugin/blob/gcs already uses for its dataDir URIs
	// ("gcs://<bucket>"), keeping the convention consistent across the
	// codebase.
	RegisterCloudDestinationScheme("gcs", newGCSDestination)
}

// gcsDestination uploads/downloads a snapshot directory's files as flat GCS
// objects under bucket/prefix. Auth comes entirely from Application Default
// Credentials — same convention as database/plugin/blob/gcs, no explicit
// service-account config here.
type gcsDestination struct {
	bucket *storage.BucketHandle
	prefix string
}

func newGCSDestination(uri *url.URL) (CloudDestination, error) {
	bucketName := uri.Host
	if bucketName == "" {
		return nil, fmt.Errorf("gcs cloud destination %q: missing bucket", uri.String())
	}
	prefix := strings.Trim(uri.Path, "/")

	ctx := context.Background()
	client, err := storage.NewGRPCClient(ctx, option.WithScopes(storage.ScopeReadWrite))
	if err != nil {
		return nil, fmt.Errorf("gcs cloud destination: create storage client: %w", err)
	}
	return &gcsDestination{
		bucket: client.Bucket(bucketName),
		prefix: prefix,
	}, nil
}

func (d *gcsDestination) objectKey(fileName string) string {
	if d.prefix == "" {
		return fileName
	}
	return path.Join(d.prefix, fileName)
}

// UploadDir uploads every regular file directly inside localDir (not
// recursive — Snapshot's output directory is flat) to the destination.
func (d *gcsDestination) UploadDir(ctx context.Context, localDir string) error {
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return fmt.Errorf("read snapshot directory %q: %w", localDir, err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		localPath := filepath.Join(localDir, entry.Name())
		f, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("open %q for upload: %w", localPath, err)
		}
		key := d.objectKey(entry.Name())
		w := d.bucket.Object(key).NewWriter(ctx)
		_, copyErr := io.Copy(w, f)
		closeWErr := w.Close()
		closeFErr := f.Close()
		if copyErr != nil {
			return fmt.Errorf("upload %q to gcs object %q: %w", localPath, key, copyErr)
		}
		if closeWErr != nil {
			return fmt.Errorf("close gcs object %q after upload: %w", key, closeWErr)
		}
		if closeFErr != nil {
			return fmt.Errorf("close %q after upload: %w", localPath, closeFErr)
		}
	}
	return nil
}

// DownloadDir downloads every object under the destination's prefix into
// localDir. Keys containing a further path separator are skipped — a
// snapshot directory's contents are flat, so any such key wasn't written by
// UploadDir.
func (d *gcsDestination) DownloadDir(ctx context.Context, localDir string) error {
	query := &storage.Query{}
	if d.prefix != "" {
		query.Prefix = d.prefix + "/"
	}
	it := d.bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("list gcs objects under %q: %w", d.prefix, err)
		}
		fileName := attrs.Name
		if d.prefix != "" {
			fileName = strings.TrimPrefix(fileName, d.prefix+"/")
		}
		if fileName == "" || strings.Contains(fileName, "/") {
			continue
		}
		localPath := filepath.Join(localDir, fileName)
		f, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("create %q for download: %w", localPath, err)
		}
		r, err := d.bucket.Object(attrs.Name).NewReader(ctx)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("open gcs object %q for download: %w", attrs.Name, err)
		}
		_, copyErr := io.Copy(f, r)
		closeRErr := r.Close()
		closeFErr := f.Close()
		if copyErr != nil {
			return fmt.Errorf("download gcs object %q: %w", attrs.Name, copyErr)
		}
		if closeRErr != nil {
			return fmt.Errorf("close gcs object %q reader: %w", attrs.Name, closeRErr)
		}
		if closeFErr != nil {
			return fmt.Errorf("close %q after download: %w", localPath, closeFErr)
		}
	}
	return nil
}

// ListSnapshots implements SnapshotLister: it lists the "sub-directories"
// one level under this destination's prefix (each one a snapshot ID,
// matching how SnapshotToCloud uploads — see its doc comment) via GCS's
// delimiter-based listing, then fetches and parses just each one's
// manifest.json rather than downloading the whole snapshot.
func (d *gcsDestination) ListSnapshots(ctx context.Context) ([]SnapshotEntry, error) {
	listPrefix := ""
	if d.prefix != "" {
		listPrefix = d.prefix + "/"
	}
	it := d.bucket.Objects(ctx, &storage.Query{Prefix: listPrefix, Delimiter: "/"})
	var entries []SnapshotEntry
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list gcs objects under %q: %w", d.prefix, err)
		}
		// With Delimiter set, a synthetic "directory entry" (Prefix set,
		// every other field empty) represents one sub-path; a real object
		// (Name set) means something was uploaded directly at this level,
		// which the nested-per-snapshot layout never does.
		if attrs.Prefix == "" {
			continue
		}
		snapshotID := strings.TrimSuffix(strings.TrimPrefix(attrs.Prefix, listPrefix), "/")
		if snapshotID == "" {
			continue
		}
		manifest, err := d.fetchManifest(ctx, snapshotID)
		if err != nil {
			// A sub-path without a valid manifest is a snapshot still
			// being written, or one that failed partway through — skip
			// it rather than failing the whole listing, same as the
			// local lifecycle.ListSnapshots convention.
			continue
		}
		entries = append(entries, SnapshotEntry{ID: snapshotID, Manifest: manifest})
	}
	return entries, nil
}

// fetchManifest downloads and parses just the manifest.json for
// snapshotID, without downloading the rest of that snapshot's (possibly
// very large) blob/metadata backups.
func (d *gcsDestination) fetchManifest(ctx context.Context, snapshotID string) (Manifest, error) {
	key := d.objectKey(path.Join(snapshotID, ManifestFileName))
	r, err := d.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return Manifest{}, fmt.Errorf("open gcs object %q: %w", key, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return Manifest{}, fmt.Errorf("read gcs object %q: %w", key, err)
	}
	return ParseManifest(data)
}

// FetchManifest implements CloudManifestFetcher: it fetches and parses
// this destination's own manifest.json directly (at its configured
// prefix, not a further per-snapshot sub-path) — used to check whether a
// specific snapshot exists at a destination without downloading the rest
// of it.
func (d *gcsDestination) FetchManifest(ctx context.Context) (Manifest, error) {
	return d.fetchManifest(ctx, "")
}

// Delete implements CloudDeleter: it removes every object under this
// destination's own prefix (all of UploadDir's files) — used by
// DeleteSnapshot to clean up a snapshot's cloud copy. Meant to be called
// on a destination parsed from a specific snapshot's own URI (base +
// snapshot ID via JoinCloudURI), never a bare base destination shared
// across many snapshots — refuses outright on an empty prefix (bucket
// root) rather than risk deleting an entire bucket.
func (d *gcsDestination) Delete(ctx context.Context) error {
	if d.prefix == "" {
		return errors.New(
			"gcs cloud destination: refusing to delete with an empty prefix (would delete the entire bucket)",
		)
	}
	it := d.bucket.Objects(ctx, &storage.Query{Prefix: d.prefix + "/"})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("list gcs objects under %q for delete: %w", d.prefix, err)
		}
		if err := d.bucket.Object(attrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("delete gcs object %q: %w", attrs.Name, err)
		}
	}
	return nil
}

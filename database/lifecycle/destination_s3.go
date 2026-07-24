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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func init() {
	RegisterCloudDestinationScheme("s3", newS3Destination)
}

// s3Destination uploads/downloads a snapshot directory's files as flat S3
// objects under bucket/prefix. Credentials come entirely from the AWS SDK's
// default credential chain (config.LoadDefaultConfig) — same convention as
// database/plugin/blob/aws, no explicit access-key/secret config here.
type s3Destination struct {
	bucket string
	prefix string
	client *s3.Client
}

func newS3Destination(uri *url.URL) (CloudDestination, error) {
	bucket := uri.Host
	if bucket == "" {
		return nil, fmt.Errorf("s3 cloud destination %q: missing bucket", uri.String())
	}
	prefix := strings.Trim(uri.Path, "/")

	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("s3 cloud destination: load default AWS config: %w", err)
	}

	// AWS_ENDPOINT (not a standard AWS SDK env var, so config.LoadDefaultConfig
	// never sees it on its own) points at a self-hosted S3-compatible store —
	// typically MinIO — instead of real AWS S3, mirroring
	// database/plugin/blob/aws's own WithEndpoint option and the same
	// AWS_ENDPOINT convention its tests already use. Without this, a
	// snapshotCloudDestination configured with anything other than genuine
	// AWS S3 would silently try to authenticate against real AWS regardless
	// of intent.
	endpoint := os.Getenv("AWS_ENDPOINT")
	if endpoint != "" {
		awsCfg.BaseEndpoint = &endpoint
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			// Virtual-hosted-style addressing requires DNS for
			// "<bucket>.<endpoint>", which a custom endpoint can't satisfy.
			o.UsePathStyle = true
		}
	})

	return &s3Destination{
		bucket: bucket,
		prefix: prefix,
		client: client,
	}, nil
}

func (d *s3Destination) objectKey(fileName string) string {
	if d.prefix == "" {
		return fileName
	}
	return path.Join(d.prefix, fileName)
}

// UploadDir uploads every regular file directly inside localDir (not
// recursive — Snapshot's output directory is flat) to the destination.
func (d *s3Destination) UploadDir(ctx context.Context, localDir string) error {
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return fmt.Errorf("read snapshot directory %q: %w", localDir, err)
	}
	// manager.Uploader is deprecated in favor of feature/s3/transfermanager,
	// but remains fully supported; migrating is tracked as follow-up work
	// rather than done here to avoid pulling in a newer, less-established
	// API right alongside this package's initial real-cloud test coverage.
	uploader := manager.NewUploader(d.client) //nolint:staticcheck
	for _, entry := range orderEntriesManifestLast(entries) {
		if !entry.Type().IsRegular() {
			continue
		}
		localPath := filepath.Join(localDir, entry.Name())
		f, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("open %q for upload: %w", localPath, err)
		}
		key := d.objectKey(entry.Name())
		_, uploadErr := uploader.Upload(ctx, &s3.PutObjectInput{ //nolint:staticcheck
			Bucket: &d.bucket,
			Key:    &key,
			Body:   f,
		})
		closeErr := f.Close()
		if uploadErr != nil {
			return fmt.Errorf("upload %q to s3://%s/%s: %w", localPath, d.bucket, key, uploadErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close %q after upload: %w", localPath, closeErr)
		}
	}
	return nil
}

// DownloadDir downloads every object under the destination's prefix into
// localDir. Keys containing a further path separator are skipped — a
// snapshot directory's contents are flat, so any such key wasn't written by
// UploadDir.
func (d *s3Destination) DownloadDir(ctx context.Context, localDir string) error {
	downloader := manager.NewDownloader(d.client) //nolint:staticcheck // see UploadDir's note on manager.Uploader
	listInput := &s3.ListObjectsV2Input{Bucket: &d.bucket}
	if d.prefix != "" {
		p := d.prefix + "/"
		listInput.Prefix = &p
	}
	paginator := s3.NewListObjectsV2Paginator(d.client, listInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list s3://%s/%s: %w", d.bucket, d.prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			fileName := strings.TrimPrefix(*obj.Key, d.prefix+"/")
			if d.prefix == "" {
				fileName = *obj.Key
			}
			if !IsSafeCloudObjectFileName(fileName) {
				continue
			}
			localPath := filepath.Join(localDir, fileName)
			f, err := os.Create(localPath)
			if err != nil {
				return fmt.Errorf("create %q for download: %w", localPath, err)
			}
			_, downloadErr := downloader.Download(ctx, f, &s3.GetObjectInput{ //nolint:staticcheck
				Bucket: &d.bucket,
				Key:    obj.Key,
			})
			closeErr := f.Close()
			if downloadErr != nil {
				return fmt.Errorf("download s3://%s/%s: %w", d.bucket, *obj.Key, downloadErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close %q after download: %w", localPath, closeErr)
			}
		}
	}
	return nil
}

// ListSnapshots implements SnapshotLister: it lists the "sub-directories"
// one level under this destination's prefix (each one a snapshot ID,
// matching how SnapshotToCloud uploads — see its doc comment) via S3's
// delimiter-based listing, then fetches and parses just each one's
// manifest.json rather than downloading the whole snapshot.
func (d *s3Destination) ListSnapshots(ctx context.Context) ([]SnapshotEntry, error) {
	listPrefix := ""
	if d.prefix != "" {
		listPrefix = d.prefix + "/"
	}
	delimiter := "/"
	input := &s3.ListObjectsV2Input{
		Bucket:    &d.bucket,
		Delimiter: &delimiter,
	}
	if listPrefix != "" {
		input.Prefix = &listPrefix
	}
	paginator := s3.NewListObjectsV2Paginator(d.client, input)
	var entries []SnapshotEntry
	var problems []error
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", d.bucket, d.prefix, err)
		}
		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			snapshotID := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, listPrefix), "/")
			if snapshotID == "" {
				continue
			}
			manifest, err := d.fetchManifest(ctx, snapshotID)
			if err != nil {
				// A sub-path with no manifest.json object at all
				// (ErrCloudSnapshotNotFound) is a snapshot still being
				// written — skip it silently, same as the local
				// lifecycle.ListSnapshots convention. Any other
				// fetch/parse failure (corrupted manifest, checksum
				// mismatch, a real storage error) is not that expected
				// case and must not be swallowed the same way: it's
				// accumulated and returned via errors.Join alongside
				// whatever entries were found, so a caller can learn the
				// catalog is missing something instead of it silently
				// looking one snapshot smaller than it is.
				if errors.Is(err, ErrCloudSnapshotNotFound) {
					continue
				}
				problems = append(
					problems,
					fmt.Errorf("snapshot %q: %w", snapshotID, err),
				)
				continue
			}
			entries = append(entries, SnapshotEntry{ID: snapshotID, Manifest: manifest})
		}
	}
	return entries, errors.Join(problems...)
}

// fetchManifest downloads and parses just the manifest.json for
// snapshotID, without downloading the rest of that snapshot's (possibly
// very large) blob/metadata backups.
func (d *s3Destination) fetchManifest(ctx context.Context, snapshotID string) (Manifest, error) {
	key := d.objectKey(path.Join(snapshotID, ManifestFileName))
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    &key,
	})
	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return Manifest{}, fmt.Errorf(
				"get s3://%s/%s: %w: %w",
				d.bucket, key, ErrCloudSnapshotNotFound, err,
			)
		}
		return Manifest{}, fmt.Errorf("get s3://%s/%s: %w", d.bucket, key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return Manifest{}, fmt.Errorf("read s3://%s/%s: %w", d.bucket, key, err)
	}
	return ParseManifest(data)
}

// FetchManifest implements CloudManifestFetcher: it fetches and parses
// this destination's own manifest.json directly (at its configured
// prefix, not a further per-snapshot sub-path) — used to check whether a
// specific snapshot exists at a destination without downloading the rest
// of it.
func (d *s3Destination) FetchManifest(ctx context.Context) (Manifest, error) {
	return d.fetchManifest(ctx, "")
}

// Delete implements CloudDeleter: it removes every object under this
// destination's own prefix (all of UploadDir's files) — used by
// DeleteSnapshot to clean up a snapshot's cloud copy. Meant to be called
// on a destination parsed from a specific snapshot's own URI (base +
// snapshot ID via JoinCloudURI), never a bare base destination shared
// across many snapshots — refuses outright on an empty prefix (bucket
// root) rather than risk deleting an entire bucket.
func (d *s3Destination) Delete(ctx context.Context) error {
	if d.prefix == "" {
		return errors.New(
			"s3 cloud destination: refusing to delete with an empty prefix (would delete the entire bucket)",
		)
	}
	listPrefix := d.prefix + "/"
	input := &s3.ListObjectsV2Input{
		Bucket: &d.bucket,
		Prefix: &listPrefix,
	}
	paginator := s3.NewListObjectsV2Paginator(d.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list s3://%s/%s for delete: %w", d.bucket, d.prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			if _, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: &d.bucket,
				Key:    obj.Key,
			}); err != nil {
				return fmt.Errorf("delete s3://%s/%s: %w", d.bucket, *obj.Key, err)
			}
		}
	}
	return nil
}

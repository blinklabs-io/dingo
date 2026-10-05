//go:build dingo_extra_plugins

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

package mithril

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func openRemoteArtifactStore(
	ctx context.Context,
	u *url.URL,
) (ArtifactStore, error) {
	prefix := strings.Trim(path.Clean("/"+u.Path), "/")
	switch u.Scheme {
	case "s3":
		return newS3ArtifactStore(ctx, u.Host, prefix)
	case "gcs":
		return newGCSArtifactStore(ctx, u.Host, prefix)
	default:
		return nil, fmt.Errorf("unsupported artifact store scheme %q", u.Scheme)
	}
}

// remoteKey joins the store's configured prefix and an artifact key.
func remoteKey(prefix, key string) string {
	return path.Join(prefix, key)
}

// remoteDir is remoteKey as a listing prefix: "" for the bucket root,
// otherwise slash-terminated so a sibling sharing the name prefix is not
// matched.
func remoteDir(prefix, key string) string {
	full := remoteKey(prefix, strings.TrimSuffix(key, "/"))
	if full == "" || full == "." {
		return ""
	}
	return full + "/"
}

// deleteKey is the object key a DeletePrefix call names, refusing the store
// root however the store's own prefix is configured.
func deleteKey(storePrefix, prefix string) (string, error) {
	prefix = strings.TrimSuffix(prefix, "/")
	if !validKey(prefix) {
		return "", errors.New("refusing to delete the artifact store root")
	}
	return remoteKey(storePrefix, prefix), nil
}

// objectReader adapts a ranged object read to io.ReadSeekCloser. The object is
// fetched lazily from the current offset, so http.ServeContent can learn the
// size with a seek and then read just the requested range.
type objectReader struct {
	ctx  context.Context
	size int64
	open func(ctx context.Context, offset int64) (io.ReadCloser, error)
	pos  int64
	body io.ReadCloser
}

func (r *objectReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	if r.body == nil {
		body, err := r.open(r.ctx, r.pos)
		if err != nil {
			return 0, err
		}
		r.body = body
	}
	n, err := r.body.Read(p)
	r.pos += int64(n)
	return n, err
}

func (r *objectReader) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.pos + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, errors.New("invalid seek whence")
	}
	if next < 0 {
		return 0, errors.New("negative seek position")
	}
	if next != r.pos {
		r.closeBody()
		r.pos = next
	}
	return next, nil
}

func (r *objectReader) closeBody() {
	if r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
}

func (r *objectReader) Close() error {
	r.closeBody()
	return nil
}

// s3ArtifactStore keeps artifacts as objects under bucket/prefix. Credentials
// come from the AWS SDK default chain; AWS_ENDPOINT selects an S3-compatible
// store such as MinIO.
type s3ArtifactStore struct {
	client *s3.Client
	// manager.Uploader is deprecated for feature/s3/transfermanager but still
	// supported; database/lifecycle uses it the same way.
	uploader *manager.Uploader //nolint:staticcheck
	bucket   string
	prefix   string
}

func newS3ArtifactStore(
	ctx context.Context,
	bucket, prefix string,
) (*s3ArtifactStore, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	endpoint := os.Getenv("AWS_ENDPOINT")
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// Checksums are computed only where S3 requires them: S3-compatible
		// stores commonly reject the SDK's default streaming trailer.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return newS3ArtifactStoreWithClient(client, bucket, prefix), nil
}

func newS3ArtifactStoreWithClient(
	client *s3.Client,
	bucket, prefix string,
) *s3ArtifactStore {
	return &s3ArtifactStore{
		client:   client,
		uploader: manager.NewUploader(client), //nolint:staticcheck
		bucket:   bucket,
		prefix:   prefix,
	}
}

func isS3NotFound(err error) bool {
	var noKey *s3types.NoSuchKey
	var notFound *s3types.NotFound
	if errors.As(err, &noKey) || errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey")
}

func (s *s3ArtifactStore) Put(
	ctx context.Context,
	key string,
	r io.Reader,
) error {
	if !validKey(key) {
		return fmt.Errorf("invalid artifact key %q", key)
	}
	_, err := s.uploader.Upload( //nolint:staticcheck // see uploader field
		ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(remoteKey(s.prefix, key)),
			Body:   r,
		})
	if err != nil {
		return fmt.Errorf("uploading artifact %s: %w", key, err)
	}
	return nil
}

func (s *s3ArtifactStore) Open(
	ctx context.Context,
	key string,
) (io.ReadSeekCloser, error) {
	if !validKey(key) {
		return nil, fmt.Errorf("invalid artifact key %q", key)
	}
	full := remoteKey(s.prefix, key)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(full),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrArtifactNotFound, key)
		}
		return nil, fmt.Errorf("stat artifact %s: %w", key, err)
	}
	return &objectReader{
		ctx:  ctx,
		size: aws.ToInt64(head.ContentLength),
		open: func(ctx context.Context, offset int64) (io.ReadCloser, error) {
			out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    aws.String(full),
				Range:  aws.String(fmt.Sprintf("bytes=%d-", offset)),
			})
			if err != nil {
				return nil, fmt.Errorf("reading artifact %s: %w", key, err)
			}
			return out.Body, nil
		},
	}, nil
}

func (s *s3ArtifactStore) Subdirs(
	ctx context.Context,
	prefix string,
) ([]string, error) {
	listPrefix := remoteDir(s.prefix, prefix)
	var dirs []string
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(listPrefix),
		Delimiter: aws.String("/"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing artifacts: %w", err)
		}
		for _, p := range page.CommonPrefixes {
			name := strings.TrimSuffix(
				strings.TrimPrefix(aws.ToString(p.Prefix), listPrefix), "/",
			)
			dirs = append(dirs, name)
		}
	}
	return dirs, nil
}

func (s *s3ArtifactStore) DeletePrefix(
	ctx context.Context,
	prefix string,
) error {
	full, err := deleteKey(s.prefix, prefix)
	if err != nil {
		return err
	}
	// The exact key first: DeleteObject succeeds for an absent key, and a
	// metadata object removed alone must not wait on the listing below.
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(full),
	}); err != nil {
		return fmt.Errorf("deleting %s: %w", prefix, err)
	}
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(full + "/"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("listing %s: %w", prefix, err)
		}
		if len(page.Contents) == 0 {
			continue
		}
		ids := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			ids = append(ids, s3types.ObjectIdentifier{Key: obj.Key})
		}
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("deleting %s: %w", prefix, err)
		}
		if len(out.Errors) > 0 {
			return fmt.Errorf(
				"deleting %s: %s: %s", prefix,
				aws.ToString(out.Errors[0].Key),
				aws.ToString(out.Errors[0].Message),
			)
		}
	}
	return nil
}

// gcsArtifactStore keeps artifacts as objects under bucket/prefix, using
// Application Default Credentials.
type gcsArtifactStore struct {
	bucket *storage.BucketHandle
	prefix string
}

func newGCSArtifactStore(
	ctx context.Context,
	bucket, prefix string,
) (*gcsArtifactStore, error) {
	var opts []option.ClientOption
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		// An emulator serves object reads only through the JSON API; the
		// default XML reads put the escaped key in the URL path, which
		// fake-gcs-server does not route.
		opts = append(opts, storage.WithJSONReads())
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}
	return &gcsArtifactStore{bucket: client.Bucket(bucket), prefix: prefix}, nil
}

func (s *gcsArtifactStore) Put(
	ctx context.Context,
	key string,
	r io.Reader,
) error {
	if !validKey(key) {
		return fmt.Errorf("invalid artifact key %q", key)
	}
	// Cancelling the writer's context abandons the upload, so a failed copy
	// never publishes a partial object.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := s.bucket.Object(remoteKey(s.prefix, key)).NewWriter(ctx)
	if _, err := io.Copy(w, r); err != nil {
		cancel()
		_ = w.Close()
		return fmt.Errorf("uploading artifact %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("uploading artifact %s: %w", key, err)
	}
	return nil
}

func (s *gcsArtifactStore) Open(
	ctx context.Context,
	key string,
) (io.ReadSeekCloser, error) {
	if !validKey(key) {
		return nil, fmt.Errorf("invalid artifact key %q", key)
	}
	obj := s.bucket.Object(remoteKey(s.prefix, key))
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrArtifactNotFound, key)
		}
		return nil, fmt.Errorf("stat artifact %s: %w", key, err)
	}
	return &objectReader{
		ctx:  ctx,
		size: attrs.Size,
		open: func(ctx context.Context, offset int64) (io.ReadCloser, error) {
			return obj.NewRangeReader(ctx, offset, -1)
		},
	}, nil
}

func (s *gcsArtifactStore) Subdirs(
	ctx context.Context,
	prefix string,
) ([]string, error) {
	listPrefix := remoteDir(s.prefix, prefix)
	var dirs []string
	it := s.bucket.Objects(ctx, &storage.Query{
		Prefix:    listPrefix,
		Delimiter: "/",
	})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return dirs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("listing artifacts: %w", err)
		}
		// Objects have an empty Prefix; a delimiter-grouped child has an
		// empty Name.
		if attrs.Prefix != "" {
			dirs = append(dirs, strings.TrimSuffix(
				strings.TrimPrefix(attrs.Prefix, listPrefix), "/",
			))
		}
	}
}

func (s *gcsArtifactStore) DeletePrefix(
	ctx context.Context,
	prefix string,
) error {
	full, err := deleteKey(s.prefix, prefix)
	if err != nil {
		return err
	}
	if err := s.bucket.Object(full).Delete(ctx); err != nil &&
		!errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("deleting %s: %w", prefix, err)
	}
	it := s.bucket.Objects(ctx, &storage.Query{Prefix: full + "/"})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("listing %s: %w", prefix, err)
		}
		if err := s.bucket.Object(attrs.Name).Delete(ctx); err != nil &&
			!errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("deleting %s: %w", attrs.Name, err)
		}
	}
}

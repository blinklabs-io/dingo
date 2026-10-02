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
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// fakeBucket is the object table behind the S3 and GCS test servers.
type fakeBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (b *fakeBucket) put(key string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.objects == nil {
		b.objects = map[string][]byte{}
	}
	b.objects[key] = data
}

func (b *fakeBucket) get(key string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	return data, ok
}

func (b *fakeBucket) remove(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
}

// list returns the object keys under prefix and, when delimiter is set, the
// grouped child prefixes, as one sorted sequence.
func (b *fakeBucket) list(prefix, delimiter string) (keys, prefixes []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := map[string]bool{}
	for key := range b.objects {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		if delimiter != "" {
			if i := strings.Index(rest, delimiter); i >= 0 {
				child := prefix + rest[:i+len(delimiter)]
				if !seen[child] {
					seen[child] = true
					prefixes = append(prefixes, child)
				}
				continue
			}
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	slices.Sort(prefixes)
	return keys, prefixes
}

func (b *fakeBucket) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.objects)
}

// serveRange writes data honoring a "bytes=N-" request.
func serveRange(w http.ResponseWriter, r *http.Request, data []byte) {
	start := 0
	status := http.StatusOK
	if spec, ok := strings.CutPrefix(r.Header.Get("Range"), "bytes="); ok {
		from, _, _ := strings.Cut(spec, "-")
		start, _ = strconv.Atoi(from)
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf(
			"bytes %d-%d/%d", start, len(data)-1, len(data),
		))
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)-start))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data[start:])
	}
}

// s3Page is the listing page size: small, so pagination is exercised.
const fakeS3PageSize = 2

// newFakeS3 serves the path-style S3 operations the artifact store uses.
func newFakeS3(t *testing.T, bucketName string) (*fakeBucket, string) {
	t.Helper()
	bucket := &fakeBucket{}
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		key, ok := strings.CutPrefix(r.URL.Path, "/"+bucketName+"/")
		if !ok {
			key = ""
		}
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("delete"):
			var req struct {
				Objects []struct {
					Key string `xml:"Key"`
				} `xml:"Object"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			for _, o := range req.Objects {
				bucket.remove(o.Key)
			}
			_, _ = io.WriteString(w, "<DeleteResult></DeleteResult>")
		case r.Method == http.MethodGet && key == "":
			fakeS3List(w, bucket, q)
		case r.Method == http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			bucket.put(key, data)
			w.Header().Set("ETag", `"etag"`)
		case r.Method == http.MethodDelete:
			bucket.remove(key)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet || r.Method == http.MethodHead:
			data, found := bucket.get(key)
			if !found {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(
					w,
					`<Error><Code>NoSuchKey</Code></Error>`,
				)
				return
			}
			serveRange(w, r, data)
		default:
			http.Error(w, "unsupported", http.StatusNotImplemented)
		}
	}))
	t.Cleanup(srv.Close)
	return bucket, srv.URL
}

func fakeS3List(w http.ResponseWriter, bucket *fakeBucket, q url.Values) {
	keys, prefixes := bucket.list(q.Get("prefix"), q.Get("delimiter"))
	type entry struct{ name, kind string }
	var all []entry
	for _, k := range keys {
		all = append(all, entry{k, "key"})
	}
	for _, p := range prefixes {
		all = append(all, entry{p, "prefix"})
	}
	slices.SortFunc(all, func(a, b entry) int {
		return strings.Compare(a.name, b.name)
	})
	// The token is the last name already returned, as a real store's is, so
	// deleting a page's objects before fetching the next does not shift it.
	token := q.Get("continuation-token")
	start := 0
	if token != "" {
		start = len(all)
		for i, e := range all {
			if e.name > token {
				start = i
				break
			}
		}
	}
	end := min(start+fakeS3PageSize, len(all))
	var sb strings.Builder
	sb.WriteString("<ListBucketResult>")
	for _, e := range all[start:end] {
		if e.kind == "key" {
			fmt.Fprintf(&sb, "<Contents><Key>%s</Key></Contents>", e.name)
		} else {
			fmt.Fprintf(
				&sb,
				"<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>",
				e.name,
			)
		}
	}
	fmt.Fprintf(&sb, "<KeyCount>%d</KeyCount>", end-start)
	if end < len(all) {
		fmt.Fprintf(
			&sb,
			"<IsTruncated>true</IsTruncated>"+
				"<NextContinuationToken>%s</NextContinuationToken>",
			all[end-1].name,
		)
	} else {
		sb.WriteString("<IsTruncated>false</IsTruncated>")
	}
	sb.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, sb.String())
}

func newTestS3Store(
	t *testing.T,
	prefix string,
) (*s3ArtifactStore, *fakeBucket) {
	t.Helper()
	bucket, endpoint := newFakeS3(t, "artifacts")
	client := s3.NewFromConfig(aws.Config{
		Region:      "test",
		Credentials: aws.AnonymousCredentials{},
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return newS3ArtifactStoreWithClient(client, "artifacts", prefix), bucket
}

// newFakeGCS serves the JSON and XML API calls the GCS artifact store makes.
func newFakeGCS(t *testing.T, bucketName string) (*fakeBucket, string) {
	t.Helper()
	bucket := &fakeBucket{}
	objectsPath := "/storage/v1/b/" + bucketName + "/o"
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		p := r.URL.Path
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && strings.HasPrefix(p, "/upload/"):
			name, data, err := parseGCSUpload(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			bucket.put(name, data)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name": name, "bucket": bucketName,
				"size": strconv.Itoa(len(data)),
			})
		case r.Method == http.MethodGet && p == objectsPath:
			keys, prefixes := bucket.list(q.Get("prefix"), q.Get("delimiter"))
			items := []map[string]any{}
			for _, k := range keys {
				items = append(items, map[string]any{
					"name": k, "bucket": bucketName,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": items, "prefixes": prefixes,
			})
		case strings.HasPrefix(p, objectsPath+"/"):
			name, _ := url.PathUnescape(strings.TrimPrefix(p, objectsPath+"/"))
			data, found := bucket.get(name)
			switch {
			case r.Method == http.MethodDelete && found:
				bucket.remove(name)
				w.WriteHeader(http.StatusNoContent)
			case !found:
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `{"error":{"code":404}}`)
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"name": name, "bucket": bucketName,
					"size": strconv.Itoa(len(data)),
				})
			}
		case r.Method == http.MethodGet &&
			strings.HasPrefix(p, "/"+bucketName+"/"):
			data, found := bucket.get(strings.TrimPrefix(p, "/"+bucketName+"/"))
			if !found {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			serveRange(w, r, data)
		default:
			http.Error(
				w,
				"unsupported "+r.Method+" "+p,
				http.StatusNotImplemented,
			)
		}
	}))
	t.Cleanup(srv.Close)
	return bucket, srv.URL
}

// parseGCSUpload reads a multipart/related media upload: a JSON part naming
// the object followed by its bytes.
func parseGCSUpload(r *http.Request) (string, []byte, error) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return "", nil, err
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	meta, err := mr.NextPart()
	if err != nil {
		return "", nil, err
	}
	var attrs struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(meta).Decode(&attrs); err != nil {
		return "", nil, err
	}
	media, err := mr.NextPart()
	if err != nil {
		return "", nil, err
	}
	data, err := io.ReadAll(media)
	return attrs.Name, data, err
}

func newTestGCSStore(
	t *testing.T,
	prefix string,
) (*gcsArtifactStore, *fakeBucket) {
	t.Helper()
	bucket, endpoint := newFakeGCS(t, "artifacts")
	client, err := storage.NewClient(
		context.Background(),
		option.WithEndpoint(endpoint+"/storage/v1/"),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return &gcsArtifactStore{
		bucket: client.Bucket("artifacts"), prefix: prefix,
	}, bucket
}

func TestS3ArtifactStoreContract(t *testing.T) {
	t.Parallel()

	for _, prefix := range []string{"", "mithril/preprod"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			t.Parallel()
			store, bucket := newTestS3Store(t, prefix)
			requireArtifactStoreContract(t, store)
			if prefix != "" {
				// Everything lives under the configured prefix.
				for key := range bucket.objects {
					assert.True(t, strings.HasPrefix(key, prefix+"/"), key)
				}
			}
		})
	}
}

func TestGCSArtifactStoreContract(t *testing.T) {
	t.Parallel()

	for _, prefix := range []string{"", "mithril/preprod"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			t.Parallel()
			store, bucket := newTestGCSStore(t, prefix)
			requireArtifactStoreContract(t, store)
			if prefix != "" {
				for key := range bucket.objects {
					assert.True(t, strings.HasPrefix(key, prefix+"/"), key)
				}
			}
		})
	}
}

// TestRemoteStoresServeSnapshotsEndToEnd selects each remote backend, produces
// a snapshot into it, prunes to the newest, and syncs a node from the handler
// both through the proxy and through a redirect to the bucket's public URL.
func TestRemoteStoresServeSnapshotsEndToEnd(t *testing.T) {
	t.Parallel()

	backends := map[string]func(t *testing.T) (ArtifactStore, *httptest.Server){
		"s3": func(t *testing.T) (ArtifactStore, *httptest.Server) {
			store, bucket := newTestS3Store(t, "mithril")
			return store, publicBucket(t, bucket)
		},
		"gcs": func(t *testing.T) (ArtifactStore, *httptest.Server) {
			store, bucket := newTestGCSStore(t, "mithril")
			return store, publicBucket(t, bucket)
		},
	}
	for name, newStore := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, public := newStore(t)
			hashes := createSnapshots(t, store, 1, 3)
			removed, err := PruneSnapshots(context.Background(), store, 1)
			require.NoError(t, err)
			require.Equal(t, hashes[:1], removed)
			require.Equal(t, hashes[1:], snapshotHashes(t, store))

			for mode, cfg := range map[string]ServerConfig{
				"proxy":    {},
				"redirect": {RedirectBaseURL: public.URL + "/mithril"},
			} {
				t.Run(mode, func(t *testing.T) {
					cfg.Store = store
					srv := httptest.NewServer(NewServerHandler(cfg))
					t.Cleanup(srv.Close)
					result, err := Bootstrap(
						context.Background(), BootstrapConfig{
							Network:           "preprod",
							Backend:           BackendV2,
							AggregatorURL:     srv.URL,
							AllowInsecureHTTP: true,
							DownloadDir:       t.TempDir(),
							Logger:            slog.New(slog.DiscardHandler),
						},
					)
					require.NoError(t, err)
					assert.Equal(t, hashes[1], result.Snapshot.Digest)
					entries, err := os.ReadDir(result.ImmutableDir)
					require.NoError(t, err)
					assert.Len(t, entries, 9)
					_, err = os.Stat(filepath.Join(
						result.AncillaryDir, "ledger", "100", "state",
					))
					require.NoError(t, err)
				})
			}
		})
	}
}

// publicBucket serves a fake bucket's objects over plain GET at
// /<prefix>/<key>, the way a public bucket or CDN would.
func publicBucket(t *testing.T, bucket *fakeBucket) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		data, ok := bucket.get(strings.TrimPrefix(r.URL.Path, "/"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		serveRange(w, r, data)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestOpenArtifactStoreSelectsRemoteBackends covers selection by location URI.
// Not t.Parallel: it sets AWS_ENDPOINT, the credential variables and
// STORAGE_EMULATOR_HOST, which the cloud SDKs read from the process
// environment.
func TestOpenArtifactStoreSelectsRemoteBackends(t *testing.T) {
	bucket, endpoint := newFakeS3(t, "artifacts")
	t.Setenv("AWS_ENDPOINT", endpoint)
	t.Setenv("AWS_REGION", "test")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	store, err := OpenArtifactStore(
		context.Background(), "s3://artifacts/mithril/preprod/",
	)
	require.NoError(t, err)
	require.IsType(t, &s3ArtifactStore{}, store)
	require.NoError(t, store.Put(
		context.Background(), "x/y.bin", strings.NewReader("data"),
	))
	_, ok := bucket.get("mithril/preprod/x/y.bin")
	assert.True(t, ok, "object lands under the location's prefix")

	gcsBucket, gcsEndpoint := newFakeGCS(t, "artifacts")
	t.Setenv(
		"STORAGE_EMULATOR_HOST",
		strings.TrimPrefix(gcsEndpoint, "http://"),
	)
	store, err = OpenArtifactStore(
		context.Background(), "gcs://artifacts/mithril/preprod",
	)
	require.NoError(t, err)
	require.IsType(t, &gcsArtifactStore{}, store)
	require.NoError(t, store.Put(
		context.Background(), "x/y.bin", strings.NewReader("data"),
	))
	_, ok = gcsBucket.get("mithril/preprod/x/y.bin")
	assert.True(t, ok, "object lands under the location's prefix")

	_, err = OpenArtifactStore(context.Background(), "ftp://host/path")
	require.Error(t, err)
}

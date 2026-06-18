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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package offchainmetadata

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type fakeStore struct{}

func (fakeStore) EnsureOffchainMetadataPointers(
	context.Context,
	time.Time,
	types.Txn,
) (int, error) {
	return 0, nil
}

func (fakeStore) GetOffchainMetadataFetchBatch(
	context.Context,
	int,
	time.Time,
	types.Txn,
) ([]models.OffchainMetadata, error) {
	return nil, nil
}

func (fakeStore) SetOffchainMetadataFetchResult(
	context.Context,
	*models.OffchainMetadata,
	types.Txn,
) error {
	return nil
}

type batchStore struct {
	fakeStore
	batch   []models.OffchainMetadata
	results []models.OffchainMetadata
}

func (s *batchStore) GetOffchainMetadataFetchBatch(
	context.Context,
	int,
	time.Time,
	types.Txn,
) ([]models.OffchainMetadata, error) {
	return append([]models.OffchainMetadata(nil), s.batch...), nil
}

func (s *batchStore) SetOffchainMetadataFetchResult(
	_ context.Context,
	doc *models.OffchainMetadata,
	_ types.Txn,
) error {
	s.results = append(s.results, *doc)
	return nil
}

type cancelCheckStore struct {
	called bool
}

func (s *cancelCheckStore) EnsureOffchainMetadataPointers(
	context.Context,
	time.Time,
	types.Txn,
) (int, error) {
	s.called = true
	return 0, nil
}

func (s *cancelCheckStore) GetOffchainMetadataFetchBatch(
	context.Context,
	int,
	time.Time,
	types.Txn,
) ([]models.OffchainMetadata, error) {
	s.called = true
	return nil, nil
}

func (s *cancelCheckStore) SetOffchainMetadataFetchResult(
	context.Context,
	*models.OffchainMetadata,
	types.Txn,
) error {
	s.called = true
	return nil
}

func TestFetchOneStoresVerifiedContent(t *testing.T) {
	body := []byte(`{"name":"Test Pool"}`)
	hash := blake2b.Sum256(body)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		},
	))
	t.Cleanup(server.Close)

	fetcher, err := New(Config{
		Store:                 fakeStore{},
		HTTPClient:            server.Client(),
		AllowPrivateAddresses: true,
	})
	require.NoError(t, err)
	now := time.Unix(100, 0).UTC()
	fetcher.now = func() time.Time { return now }

	doc := models.OffchainMetadata{
		URL:  server.URL,
		Hash: hash[:],
	}
	fetcher.fetchOne(context.Background(), &doc)

	require.Equal(t, models.OffchainMetadataStatusFetched, doc.Status)
	require.Equal(t, body, doc.Content)
	require.Equal(t, hash[:], doc.BodyHash)
	require.Equal(t, "application/json", doc.ContentType)
	require.Equal(t, uint(1), doc.FetchAttempts)
	require.Equal(t, uint(http.StatusOK), doc.LastHTTPStatus)
	require.NotNil(t, doc.FetchedAt)
	require.True(t, doc.FetchedAt.Equal(now))
	require.Nil(t, doc.NextFetchAfter)
	require.Empty(t, doc.LastError)
}

func TestFetchOneStoresVerifiedIPFSContent(t *testing.T) {
	body := []byte(`{"abstract":"Proposal metadata"}`)
	hash := blake2b.Sum256(body)
	requestedPath := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requestedPath <- r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		},
	))
	t.Cleanup(server.Close)

	fetcher, err := New(Config{
		Store:                 fakeStore{},
		HTTPClient:            server.Client(),
		IPFSGatewayURL:        server.URL + "/ipfs/",
		AllowPrivateAddresses: true,
	})
	require.NoError(t, err)
	now := time.Unix(150, 0).UTC()
	fetcher.now = func() time.Time { return now }

	doc := models.OffchainMetadata{
		URL:  "ipfs://bafkqaaa/proposal.json",
		Hash: hash[:],
	}
	fetcher.fetchOne(context.Background(), &doc)

	select {
	case path := <-requestedPath:
		require.Equal(t, "/ipfs/bafkqaaa/proposal.json", path)
	default:
		require.Fail(t, "expected IPFS gateway request")
	}
	require.Equal(t, models.OffchainMetadataStatusFetched, doc.Status)
	require.Equal(t, body, doc.Content)
	require.Equal(t, hash[:], doc.BodyHash)
	require.Equal(t, "application/json", doc.ContentType)
	require.Equal(t, uint(1), doc.FetchAttempts)
	require.Equal(t, uint(http.StatusOK), doc.LastHTTPStatus)
	require.NotNil(t, doc.FetchedAt)
	require.True(t, doc.FetchedAt.Equal(now))
	require.Nil(t, doc.NextFetchAfter)
	require.Empty(t, doc.LastError)
}

func TestResolveFetchURLAcceptsBase64URLCID(t *testing.T) {
	cidStr := "uAVUSIGZoeq34Yr13bI_Bi46fjiAIlxSFbuIzs5AqWR0NXykl"

	resolved, err := resolveFetchURL(
		"ipfs://"+cidStr+"/metadata.json",
		"https://gateway.example/ipfs/",
		true,
	)

	require.NoError(t, err)
	require.Contains(t, cidStr, "_")
	require.Contains(t, resolved, cidStr+"/metadata.json")
}

func TestFetchOneSanitizesContentType(t *testing.T) {
	testCases := []struct {
		name         string
		responseType string
		expected     string
	}{
		{
			name:         "html is not stored verbatim",
			responseType: "text/html; charset=utf-8",
			expected:     "application/octet-stream",
		},
		{
			name:         "json parameters are dropped",
			responseType: "application/json; charset=utf-8",
			expected:     "application/json",
		},
		{
			name:         "media type is lowercased",
			responseType: "APPLICATION/JSON",
			expected:     "application/json",
		},
		{
			name:         "json-ld is allowed",
			responseType: "application/ld+json",
			expected:     "application/ld+json",
		},
		{
			name:         "plain text is allowed",
			responseType: "text/plain; charset=utf-8",
			expected:     "text/plain",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"name":"Test Pool"}`)
			hash := blake2b.Sum256(body)
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", tc.responseType)
					_, _ = w.Write(body)
				},
			))
			t.Cleanup(server.Close)

			fetcher, err := New(Config{
				Store:                 fakeStore{},
				HTTPClient:            server.Client(),
				AllowPrivateAddresses: true,
			})
			require.NoError(t, err)

			doc := models.OffchainMetadata{
				URL:  server.URL,
				Hash: hash[:],
			}
			fetcher.fetchOne(context.Background(), &doc)

			require.Equal(t, models.OffchainMetadataStatusFetched, doc.Status)
			require.Equal(t, tc.expected, doc.ContentType)
		})
	}
}

func TestFetchOneRejectsHashMismatch(t *testing.T) {
	body := []byte(`{"name":"bad"}`)
	wrongHash := blake2b.Sum256([]byte("expected"))
	actualHash := blake2b.Sum256(body)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(body)
		},
	))
	t.Cleanup(server.Close)

	fetcher, err := New(Config{
		Store:                 fakeStore{},
		HTTPClient:            server.Client(),
		AllowPrivateAddresses: true,
	})
	require.NoError(t, err)
	now := time.Unix(200, 0).UTC()
	fetcher.now = func() time.Time { return now }

	doc := models.OffchainMetadata{
		URL:  server.URL,
		Hash: wrongHash[:],
	}
	fetcher.fetchOne(context.Background(), &doc)

	require.Equal(t, models.OffchainMetadataStatusFailed, doc.Status)
	require.Contains(t, doc.LastError, "metadata hash mismatch")
	require.Empty(t, doc.Content)
	require.Equal(t, actualHash[:], doc.BodyHash)
	require.NotNil(t, doc.NextFetchAfter)
	require.True(t, doc.NextFetchAfter.After(now))
}

func TestFetchOneRejectsPrivateURLByDefault(t *testing.T) {
	body := []byte(`{"name":"local"}`)
	hash := blake2b.Sum256(body)
	fetcher, err := New(Config{
		Store: fakeStore{},
	})
	require.NoError(t, err)
	now := time.Unix(300, 0).UTC()
	fetcher.now = func() time.Time { return now }

	doc := models.OffchainMetadata{
		URL:  "http://127.0.0.1/metadata.json",
		Hash: hash[:],
	}
	fetcher.fetchOne(context.Background(), &doc)

	require.Equal(t, models.OffchainMetadataStatusFailed, doc.Status)
	require.Contains(t, doc.LastError, "not allowed")
	require.Empty(t, doc.Content)
}

func TestNewSecuresCustomHTTPClientRedirects(t *testing.T) {
	fetcher, err := New(Config{
		Store: fakeStore{},
		HTTPClient: &http.Client{
			Transport: http.DefaultTransport,
		},
	})
	require.NoError(t, err)

	req, err := http.NewRequest(
		http.MethodGet,
		"http://127.0.0.1/metadata.json",
		nil,
	)
	require.NoError(t, err)
	viaReq, err := http.NewRequest(
		http.MethodGet,
		"http://metadata.example/pool.json",
		nil,
	)
	require.NoError(t, err)

	err = fetcher.client.CheckRedirect(req, []*http.Request{viaReq})
	require.ErrorContains(t, err, "not allowed")
}

func TestNewSecuresCustomHTTPClientTransportDialers(t *testing.T) {
	customDialCalled := false
	customDial := func(context.Context, string, string) (net.Conn, error) {
		customDialCalled = true
		return nil, errors.New("custom dial should not be used")
	}
	customDialNoContext := func(string, string) (net.Conn, error) {
		customDialCalled = true
		return nil, errors.New("custom dial should not be used")
	}
	fetcher, err := New(Config{
		Store: fakeStore{},
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				Proxy:          http.ProxyFromEnvironment,
				Dial:           customDialNoContext,
				DialContext:    customDial,
				DialTLS:        customDialNoContext,
				DialTLSContext: customDial,
			},
		},
	})
	require.NoError(t, err)

	transport, ok := fetcher.client.Transport.(*http.Transport)
	require.True(t, ok)
	require.Nil(t, transport.Proxy)
	require.Nil(t, transport.Dial)
	require.Nil(t, transport.DialTLS)
	require.Nil(t, transport.DialTLSContext)
	_, err = transport.DialContext(
		context.Background(),
		"tcp",
		"127.0.0.1:80",
	)
	require.ErrorContains(t, err, "not allowed")
	require.False(t, customDialCalled)
}

func TestNewRejectsCustomRoundTripperWhenPrivateAddressesBlocked(t *testing.T) {
	_, err := New(Config{
		Store: fakeStore{},
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}, nil
			}),
		},
	})

	require.ErrorContains(t, err, "cannot enforce private-address restrictions")
}

func TestRunOnceSkipsFetchResultWhenCanceled(t *testing.T) {
	requestStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(
		func(_ http.ResponseWriter, r *http.Request) {
			close(requestStarted)
			<-r.Context().Done()
		},
	))
	t.Cleanup(server.Close)

	hash := blake2b.Sum256([]byte("metadata"))
	store := &batchStore{
		batch: []models.OffchainMetadata{
			{
				ID:   1,
				URL:  server.URL,
				Hash: hash[:],
			},
		},
	}
	fetcher, err := New(Config{
		Store:                 store,
		HTTPClient:            server.Client(),
		AllowPrivateAddresses: true,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		fetcher.runOnce(ctx)
	}()

	testutil.RequireReceive(
		t,
		requestStarted,
		time.Second,
		"off-chain metadata request should start",
	)
	cancel()
	testutil.RequireReceive(
		t,
		done,
		time.Second,
		"runOnce should return after parent context cancellation",
	)
	require.Empty(t, store.results)
}

func TestRunOnceSkipsStoreWhenCanceled(t *testing.T) {
	store := &cancelCheckStore{}
	fetcher, err := New(Config{
		Store: store,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fetcher.runOnce(ctx)

	require.False(t, store.called)
}

func TestReadLimitedRejectsOversizeBody(t *testing.T) {
	_, err := readLimited(bytes.NewReader([]byte("abcd")), 3)
	require.ErrorContains(t, err, "exceeds 3 bytes")
}

func TestResolveIPFSURLUsesGatewayIPFSPath(t *testing.T) {
	fetchURL, err := resolveFetchURL(
		"ipfs://bafkqaaa/some file.json",
		"https://gateway.example",
		false,
	)

	require.NoError(t, err)
	require.Equal(
		t,
		"https://gateway.example/ipfs/bafkqaaa/some%20file.json",
		fetchURL,
	)
}

func TestResolveIPFSURLRejectsPathTraversal(t *testing.T) {
	_, err := resolveFetchURL(
		"ipfs://bafkqaaa/../secret.json",
		"https://gateway.example/ipfs/",
		false,
	)

	require.ErrorContains(t, err, "invalid IPFS path segment")
}

func TestResolveFetchURLRejectsBroadcastIPv4(t *testing.T) {
	_, err := resolveFetchURL(
		"http://255.255.255.255/metadata.json",
		"https://gateway.example/ipfs/",
		false,
	)

	require.ErrorContains(t, err, "not allowed")
}

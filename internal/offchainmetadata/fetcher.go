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
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	cidlib "github.com/ipfs/go-cid"
	"golang.org/x/crypto/blake2b"
)

const (
	defaultInterval       = 5 * time.Minute
	defaultRequestTimeout = 15 * time.Second
	defaultBatchSize      = 25
	defaultMaxBytes       = 1 << 20
	defaultUserAgent      = "dingo-offchain-metadata/1"
	defaultIPFSGatewayURL = "https://gateway.pinata.cloud/ipfs/"
	maxRedirects          = 5
)

var errFetchCanceled = errors.New("off-chain metadata fetch canceled")

type Store interface {
	EnsureOffchainMetadataPointers(
		ctx context.Context,
		now time.Time,
		txn types.Txn,
	) (int, error)
	GetOffchainMetadataFetchBatch(
		ctx context.Context,
		limit int,
		now time.Time,
		txn types.Txn,
	) ([]models.OffchainMetadata, error)
	SetOffchainMetadataFetchResult(
		ctx context.Context,
		doc *models.OffchainMetadata,
		txn types.Txn,
	) error
}

type Config struct {
	Logger *slog.Logger
	Store  Store
	// HTTPClient customizes fetch requests. When private addresses are not
	// allowed, New clones the client and replaces unsafe transport dial hooks.
	HTTPClient            *http.Client
	Interval              time.Duration
	RequestTimeout        time.Duration
	UserAgent             string
	IPFSGatewayURL        string
	BatchSize             int
	MaxBytes              int64
	AllowPrivateAddresses bool
}

type Fetcher struct {
	logger                *slog.Logger
	store                 Store
	client                *http.Client
	interval              time.Duration
	timeout               time.Duration
	userAgent             string
	ipfsGatewayURL        string
	batchSize             int
	maxBytes              int64
	allowPrivateAddresses bool
	now                   func() time.Time
	mu                    sync.Mutex
	cancel                context.CancelFunc
	done                  chan struct{}
}

func New(cfg Config) (*Fetcher, error) {
	if cfg.Store == nil {
		return nil, errors.New("off-chain metadata store is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "offchain-metadata")
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	client, err := secureHTTPClient(
		cfg.HTTPClient,
		timeout,
		cfg.AllowPrivateAddresses,
	)
	if err != nil {
		return nil, err
	}
	userAgent := cfg.UserAgent
	if userAgent == "" {
		userAgent = defaultUserAgent
	}
	ipfsGatewayURL := cfg.IPFSGatewayURL
	if ipfsGatewayURL == "" {
		ipfsGatewayURL = defaultIPFSGatewayURL
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	return &Fetcher{
		logger:                logger,
		store:                 cfg.Store,
		client:                client,
		interval:              interval,
		timeout:               timeout,
		userAgent:             userAgent,
		ipfsGatewayURL:        ipfsGatewayURL,
		batchSize:             batchSize,
		maxBytes:              maxBytes,
		allowPrivateAddresses: cfg.AllowPrivateAddresses,
		now:                   time.Now,
	}, nil
}

func (f *Fetcher) Start(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.done != nil {
		return errors.New("off-chain metadata fetcher already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.done = make(chan struct{})
	go f.loop(runCtx)
	return nil
}

func (f *Fetcher) Stop(ctx context.Context) error {
	f.mu.Lock()
	cancel := f.cancel
	done := f.done
	f.mu.Unlock()
	if cancel == nil || done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Fetcher) loop(ctx context.Context) {
	defer close(f.done)
	f.runOnce(ctx)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.runOnce(ctx)
		}
	}
}

func (f *Fetcher) runOnce(ctx context.Context) {
	now := f.now()
	if ctx.Err() != nil {
		return
	}
	created, err := f.store.EnsureOffchainMetadataPointers(ctx, now, nil)
	if err != nil {
		f.logger.Warn("off-chain metadata pointer discovery failed", "error", err)
		return
	}
	if created > 0 {
		f.logger.Debug("off-chain metadata pointers discovered", "count", created)
	}
	if ctx.Err() != nil {
		return
	}
	batch, err := f.store.GetOffchainMetadataFetchBatch(
		ctx,
		f.batchSize,
		now,
		nil,
	)
	if err != nil {
		f.logger.Warn("off-chain metadata fetch batch failed", "error", err)
		return
	}
	for i := range batch {
		if ctx.Err() != nil {
			return
		}
		doc := batch[i]
		if err := f.fetchOne(ctx, &doc); err != nil {
			if errors.Is(err, errFetchCanceled) {
				return
			}
			f.logger.Warn(
				"off-chain metadata fetch failed without result",
				"id", doc.ID,
				"url", doc.URL,
				"error", err,
			)
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if err := f.store.SetOffchainMetadataFetchResult(ctx, &doc, nil); err != nil {
			f.logger.Warn(
				"off-chain metadata fetch result update failed",
				"id", doc.ID,
				"url", doc.URL,
				"error", err,
			)
		}
	}
}

func (f *Fetcher) fetchOne(
	ctx context.Context,
	doc *models.OffchainMetadata,
) error {
	if ctx.Err() != nil {
		return errFetchCanceled
	}
	originalAttempts := doc.FetchAttempts
	doc.FetchAttempts++
	fetchURL, err := resolveFetchURL(
		doc.URL,
		f.ipfsGatewayURL,
		f.allowPrivateAddresses,
	)
	if err != nil {
		f.markFailure(doc, 0, fmt.Errorf("unsupported URL: %w", err))
		return nil
	}
	reqCtx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fetchURL, nil)
	if err != nil {
		f.markFailure(doc, 0, err)
		return nil
	}
	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept", "application/json, text/plain;q=0.8, */*;q=0.1")
	resp, err := f.client.Do(req) //nolint:gosec // URL is ledger-provided but validated and fetched through the enforced client policy.
	if err != nil {
		if ctx.Err() != nil {
			doc.FetchAttempts = originalAttempts
			return errFetchCanceled
		}
		f.markFailure(doc, 0, err)
		return nil
	}
	if resp == nil {
		f.markFailure(doc, 0, errors.New("metadata fetch returned nil response"))
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		f.markFailure(
			doc,
			uint(resp.StatusCode),
			fmt.Errorf("unexpected HTTP status %d", resp.StatusCode),
		)
		return nil
	}
	body, err := readLimited(resp.Body, f.maxBytes)
	if err != nil {
		if ctx.Err() != nil {
			doc.FetchAttempts = originalAttempts
			return errFetchCanceled
		}
		f.markFailure(doc, uint(resp.StatusCode), err)
		return nil
	}
	sum := blake2b.Sum256(body)
	doc.BodyHash = append(doc.BodyHash[:0], sum[:]...)
	if !bytes.Equal(sum[:], doc.Hash) {
		f.markFailure(
			doc,
			uint(resp.StatusCode),
			errors.New("metadata hash mismatch"),
		)
		return nil
	}
	now := f.now()
	doc.Content = body
	doc.ContentType = sanitizeContentType(resp.Header.Get("Content-Type"))
	doc.LastError = ""
	doc.LastHTTPStatus = uint(resp.StatusCode)
	doc.Status = models.OffchainMetadataStatusFetched
	doc.FetchedAt = &now
	doc.NextFetchAfter = nil
	return nil
}

func (f *Fetcher) markFailure(
	doc *models.OffchainMetadata,
	status uint,
	err error,
) {
	now := f.now()
	next := now.Add(retryDelay(doc.FetchAttempts))
	doc.Status = models.OffchainMetadataStatusFailed
	doc.LastHTTPStatus = status
	doc.LastError = truncate(err.Error(), 1024)
	doc.NextFetchAfter = &next
}

func readLimited(r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errors.New("max response bytes must be positive")
	}
	body, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxBytes)
	}
	return body, nil
}

func retryDelay(attempts uint) time.Duration {
	if attempts == 0 {
		attempts = 1
	}
	exp := min(attempts-1, 6)
	delay := 15 * time.Minute * time.Duration(1<<exp)
	if delay > 24*time.Hour || delay <= 0 {
		return 24 * time.Hour
	}
	return delay
}

// allowedContentTypes are the media types persisted verbatim from fetch
// responses. The Content-Type header is attacker-controlled (unlike the
// body, it is not bound by the on-chain hash), so anything outside this
// list is stored as application/octet-stream to keep a future serving
// endpoint from echoing types like text/html under the API origin.
var allowedContentTypes = map[string]struct{}{
	"application/json":    {},
	"application/ld+json": {},
	"text/plain":          {},
}

func sanitizeContentType(header string) string {
	mediaType, _, err := mime.ParseMediaType(header)
	if err != nil {
		return "application/octet-stream"
	}
	if _, ok := allowedContentTypes[mediaType]; !ok {
		return "application/octet-stream"
	}
	return mediaType
}

func truncate(s string, maxLen int) string {
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func newHTTPClient(timeout time.Duration, allowPrivate bool) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		Transport:     newRestrictedTransport(timeout, allowPrivate),
		CheckRedirect: restrictedRedirectPolicy(nil, allowPrivate),
	}
}

func secureHTTPClient(
	client *http.Client,
	timeout time.Duration,
	allowPrivate bool,
) (*http.Client, error) {
	if client == nil {
		return newHTTPClient(timeout, allowPrivate), nil
	}
	secured := *client
	secured.CheckRedirect = restrictedRedirectPolicy(
		client.CheckRedirect,
		allowPrivate,
	)
	if allowPrivate {
		return &secured, nil
	}
	transport, err := secureHTTPTransport(client.Transport, timeout)
	if err != nil {
		return nil, err
	}
	secured.Transport = transport
	return &secured, nil
}

func secureHTTPTransport(
	base http.RoundTripper,
	timeout time.Duration,
) (http.RoundTripper, error) {
	if base == nil {
		return newRestrictedTransport(timeout, false), nil
	}
	transport, ok := base.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf(
			"off-chain metadata HTTP transport %T cannot enforce private-address restrictions",
			base,
		)
	}
	secured := transport.Clone()
	applyRestrictedTransport(secured, timeout, false)
	return secured, nil
}

func newRestrictedTransport(
	timeout time.Duration,
	allowPrivate bool,
) *http.Transport {
	transport := &http.Transport{}
	applyRestrictedTransport(transport, timeout, allowPrivate)
	return transport
}

func applyRestrictedTransport(
	transport *http.Transport,
	timeout time.Duration,
	allowPrivate bool,
) {
	dialer := &restrictedDialer{
		dialer:       &net.Dialer{Timeout: timeout},
		allowPrivate: allowPrivate,
	}
	transport.Proxy = nil
	transport.DialContext = dialer.DialContext
	transport.Dial = nil    //nolint:staticcheck // deprecated hook is cleared so it cannot bypass the restricted dialer
	transport.DialTLS = nil //nolint:staticcheck // deprecated hook is cleared so it cannot bypass the restricted dialer
	transport.DialTLSContext = nil
	if transport.TLSHandshakeTimeout <= 0 {
		transport.TLSHandshakeTimeout = timeout
	}
	if transport.ResponseHeaderTimeout <= 0 {
		transport.ResponseHeaderTimeout = timeout
	}
}

func restrictedRedirectPolicy(
	next func(*http.Request, []*http.Request) error,
	allowPrivate bool,
) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errors.New("too many redirects")
		}
		if err := validateURL(req.URL.String(), allowPrivate); err != nil {
			return err
		}
		if next != nil {
			return next(req, via)
		}
		return nil
	}
}

func validateURL(raw string, allowPrivate bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return fmt.Errorf("scheme %q is not supported", u.Scheme)
	}
	if u.User != nil {
		return errors.New("userinfo is not allowed")
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("host is required")
	}
	if !allowPrivate && isBlockedHost(host) {
		return fmt.Errorf("host %q is not allowed", host)
	}
	if ip := net.ParseIP(host); ip != nil && !allowPrivate && isBlockedIP(ip) {
		return fmt.Errorf("IP %s is not allowed", ip)
	}
	return nil
}

func resolveFetchURL(
	raw string,
	ipfsGatewayURL string,
	allowPrivate bool,
) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		if err := validateURL(raw, allowPrivate); err != nil {
			return "", err
		}
		return raw, nil
	case "ipfs":
		return resolveIPFSFetchURL(u, ipfsGatewayURL, allowPrivate)
	default:
		return "", fmt.Errorf("scheme %q is not supported", u.Scheme)
	}
}

func resolveIPFSFetchURL(
	u *url.URL,
	gatewayRaw string,
	allowPrivate bool,
) (string, error) {
	if u.User != nil {
		return "", errors.New("userinfo is not allowed")
	}
	if u.RawQuery != "" {
		return "", errors.New("query is not allowed in IPFS URL")
	}
	if u.Fragment != "" {
		return "", errors.New("fragment is not allowed in IPFS URL")
	}
	cid, pathSegments, err := parseIPFSPath(u)
	if err != nil {
		return "", err
	}
	gateway, err := url.Parse(gatewayRaw)
	if err != nil {
		return "", fmt.Errorf("parse IPFS gateway URL: %w", err)
	}
	if err := validateURL(gatewayRaw, allowPrivate); err != nil {
		return "", fmt.Errorf("IPFS gateway URL: %w", err)
	}
	if gateway.RawQuery != "" {
		return "", errors.New("IPFS gateway query is not allowed")
	}
	if gateway.Fragment != "" {
		return "", errors.New("IPFS gateway fragment is not allowed")
	}
	basePath := strings.TrimRight(gateway.Path, "/")
	if basePath == "" {
		basePath = "/ipfs"
	}
	segments := append([]string{cid}, pathSegments...)
	gateway.Path = basePath + "/" + strings.Join(segments, "/")
	return gateway.String(), nil
}

func parseIPFSPath(u *url.URL) (string, []string, error) {
	var rawPath string
	switch {
	case u.Opaque != "":
		rawPath = u.Opaque
	case u.Host != "" && !strings.EqualFold(u.Host, "ipfs"):
		rawPath = strings.TrimRight(u.Host, "/")
		if u.Path != "" {
			rawPath += "/" + strings.TrimLeft(u.Path, "/")
		}
	default:
		rawPath = strings.TrimLeft(u.Path, "/")
	}
	parts, err := cleanIPFSPathSegments(rawPath)
	if err != nil {
		return "", nil, err
	}
	if len(parts) > 0 && strings.EqualFold(parts[0], "ipfs") {
		parts = parts[1:]
	}
	if len(parts) == 0 {
		return "", nil, errors.New("IPFS CID is required")
	}
	cid := parts[0]
	if err := validateIPFSCID(cid); err != nil {
		return "", nil, err
	}
	return cid, parts[1:], nil
}

func cleanIPFSPathSegments(rawPath string) ([]string, error) {
	rawPath = strings.Trim(rawPath, "/")
	if rawPath == "" {
		return nil, nil
	}
	rawParts := strings.Split(rawPath, "/")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		if part == "" {
			return nil, errors.New("empty IPFS path segment")
		}
		if part == "." || part == ".." {
			return nil, fmt.Errorf("invalid IPFS path segment %q", part)
		}
		for _, r := range part {
			if r < 0x20 || r == 0x7f {
				return nil, errors.New("IPFS path contains control character")
			}
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func validateIPFSCID(cidStr string) error {
	if cidStr == "" {
		return errors.New("IPFS CID is required")
	}
	if len(cidStr) > 256 {
		return errors.New("IPFS CID is too long")
	}
	if _, err := cidlib.Decode(cidStr); err != nil {
		return fmt.Errorf("invalid IPFS CID: %w", err)
	}
	return nil
}

type restrictedDialer struct {
	dialer       *net.Dialer
	allowPrivate bool
}

func (d *restrictedDialer) DialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	if d.allowPrivate {
		return d.dialer.DialContext(ctx, network, address)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if isBlockedHost(host) {
		return nil, fmt.Errorf("host %q is not allowed", host)
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses resolved for host %q", host)
	}
	for _, addr := range addrs {
		if isBlockedIP(addr.IP) {
			return nil, fmt.Errorf("resolved IP %s is not allowed", addr.IP)
		}
	}
	var lastErr error
	for _, addr := range addrs {
		target := net.JoinHostPort(addr.IP.String(), port)
		conn, err := d.dialer.DialContext(ctx, network, target)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("dial failed")
	}
	return nil, lastErr
}

func isBlockedHost(host string) bool {
	h := strings.TrimSuffix(strings.ToLower(host), ".")
	return h == "localhost" || strings.HasSuffix(h, ".localhost")
}

func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip.IsUnspecified() ||
		ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		isSpecialUseIPv4(ip)
}

func isSpecialUseIPv4(ip net.IP) bool {
	v4 := ip.To4()
	if v4 == nil {
		return false
	}
	// RFC 6598 carrier-grade NAT and documentation ranges are not routable
	// public internet targets and should not be fetched from ledger URLs.
	return v4[0] == 100 && v4[1]&0xc0 == 64 ||
		v4[0] == 192 && v4[1] == 0 && v4[2] == 0 ||
		v4[0] == 192 && v4[1] == 0 && v4[2] == 2 ||
		v4[0] == 198 && (v4[1] == 18 || v4[1] == 19) ||
		v4[0] == 198 && v4[1] == 51 && v4[2] == 100 ||
		v4[0] == 203 && v4[1] == 0 && v4[2] == 113 ||
		v4[0] >= 240 ||
		v4[0] == 255 && v4[1] == 255 &&
			v4[2] == 255 && v4[3] == 255
}

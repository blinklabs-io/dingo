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

package node

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/internal/httpuseragent"
	"github.com/blinklabs-io/dingo/internal/netguard"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
)

const (
	// remoteImmutableCacheDir is the download cache for a remote ImmutableDB,
	// under the database path so it goes away with the database it feeds.
	remoteImmutableCacheDir = "immutable-download"
	// remoteImmutablePrefetch bounds the chunks downloaded ahead of the one
	// being copied, and with it the chunks held in the cache.
	remoteImmutablePrefetch = 4
	// remoteImmutableAttempts bounds the tries per file. A retry resumes the
	// partial file; a load that still fails resumes it on the next run.
	remoteImmutableAttempts = 3
	// remoteImmutableFileTimeout bounds one request, so a stalled connection
	// becomes a retry instead of a hang.
	remoteImmutableFileTimeout = 10 * time.Minute
	// Leave one maximum block-fetch message of headroom while staging. The
	// aggregate import and cache bounds are derived from the requested chunk
	// range and the format limits below.
	remoteImmutableStagingOverhead = blockfetch.StreamingMaxPendingMessageBytes
)

// errRemoteChunkNotPublished reports that the remote root serves no .chunk
// file for a chunk number, which ends the contiguous published history.
var errRemoteChunkNotPublished = errors.New("remote chunk not published")

func isExpectedRemotePrefetchCleanupError(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !isExpectedRemotePrefetchCleanupError(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isExpectedRemotePrefetchCleanupError(wrapped.Unwrap())
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, errRemoteChunkNotPublished)
}

var errRemoteImmutableFileTooLarge = errors.New(
	"remote ImmutableDB file exceeds its format limit",
)

var (
	errRemoteImmutableImportBudget = errors.New(
		"remote ImmutableDB import byte budget exhausted",
	)
	errRemoteImmutableCacheBudget = errors.New(
		"remote ImmutableDB temporary disk budget exhausted",
	)
	errRemoteImmutableFreeSpace = errors.New(
		"remote ImmutableDB requires more free disk space",
	)
)

const (
	remotePrimaryIndexVersionBytes = 1
	remotePrimaryIndexOffsetBytes  = 4
	remoteSecondaryIndexEntryBytes = 56
)

type remoteImmutableLimits struct {
	primary    int64
	secondary  int64
	chunk      int64
	imported   int64
	cache      int64
	minFree    int64
	concurrent int
	chunkSlots uint64
}

func (l remoteImmutableLimits) withImportRange(
	first uint64,
	tipSlot uint64,
) (remoteImmutableLimits, error) {
	if l.chunkSlots == 0 || l.concurrent <= 0 {
		return remoteImmutableLimits{}, errors.New(
			"invalid remote ImmutableDB import range limits",
		)
	}
	last := tipSlot / l.chunkSlots
	last = max(last, first)
	return l.withChunkCount(last - first + 1)
}

func (l remoteImmutableLimits) withChunkCount(
	requested uint64,
) (remoteImmutableLimits, error) {
	if l.concurrent <= 0 || l.concurrent > remoteImmutablePrefetch ||
		l.primary <= 0 || l.secondary <= 0 || l.chunk <= 0 {
		return remoteImmutableLimits{}, errors.New(
			"invalid remote ImmutableDB chunk limits",
		)
	}
	concurrent := uint64(l.concurrent) //nolint:gosec // positive and bounded above
	if requested > math.MaxUint64-concurrent {
		return remoteImmutableLimits{}, errors.New(
			"remote ImmutableDB import range overflows uint64",
		)
	}
	fetched := requested + concurrent
	if l.primary > math.MaxInt64-l.secondary ||
		l.primary+l.secondary > math.MaxInt64-l.chunk {
		return remoteImmutableLimits{}, errors.New(
			"remote ImmutableDB triad limit overflows int64",
		)
	}
	triad := l.primary + l.secondary + l.chunk
	if fetched > uint64(math.MaxInt64)/uint64(remoteImmutableAttempts) ||
		fetched*uint64(remoteImmutableAttempts) >
			uint64(math.MaxInt64/triad) { //nolint:gosec // triad is positive above
		return remoteImmutableLimits{}, errors.New(
			"remote ImmutableDB aggregate import limit overflows int64",
		)
	}
	activeChunks := int64(l.concurrent + 1)
	if triad > math.MaxInt64/activeChunks {
		return remoteImmutableLimits{}, errors.New(
			"remote ImmutableDB aggregate cache limit overflows int64",
		)
	}
	l.imported = int64(fetched) * remoteImmutableAttempts * triad //nolint:gosec
	l.cache = activeChunks * triad
	l.minFree = remoteImmutableStagingOverhead
	return l, nil
}

type remoteImmutableBudget struct {
	mu       sync.Mutex
	total    int64
	active   int64
	reserved int64
	files    map[string]int64
	limits   remoteImmutableLimits
	freeDisk func(string) (uint64, error)
}

func newRemoteImmutableBudget(
	cacheDir string,
	limits remoteImmutableLimits,
) (*remoteImmutableBudget, error) {
	if limits.imported <= 0 || limits.cache <= 0 || limits.minFree < 0 ||
		limits.concurrent <= 0 || limits.concurrent > remoteImmutablePrefetch {
		return nil, errors.New("invalid remote ImmutableDB aggregate limits")
	}
	b := &remoteImmutableBudget{
		files:    make(map[string]int64),
		limits:   limits,
		freeDisk: availableDiskBytes,
	}
	err := filepath.WalkDir(cacheDir, func(
		path string,
		entry os.DirEntry,
		err error,
	) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() < 0 || b.active > limits.cache-info.Size() {
			return errRemoteImmutableCacheBudget
		}
		b.files[path] = info.Size()
		b.active += info.Size()
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("measuring remote ImmutableDB cache: %w", err)
	}
	return b, nil
}

func (b *remoteImmutableBudget) reserveWrite(path string, size int) error {
	if size <= 0 {
		return nil
	}
	n := int64(size)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.total > b.limits.imported-n {
		return fmt.Errorf(
			"%w: maximum %d bytes", errRemoteImmutableImportBudget,
			b.limits.imported,
		)
	}
	if b.active > b.limits.cache-n {
		return fmt.Errorf(
			"%w: maximum %d bytes", errRemoteImmutableCacheBudget,
			b.limits.cache,
		)
	}
	free, err := b.freeDisk(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("checking remote ImmutableDB free disk space: %w", err)
	}
	required := uint64(n)               //nolint:gosec // n is positive above
	minFree := uint64(b.limits.minFree) //nolint:gosec // minFree is validated
	reserved := uint64(b.reserved)      //nolint:gosec // reservations are positive
	if minFree > math.MaxUint64-required ||
		reserved > math.MaxUint64-minFree-required ||
		free < minFree+required+reserved {
		return fmt.Errorf(
			"%w: %d bytes available; this write, outstanding writes, and the free-space reserve do not fit",
			errRemoteImmutableFreeSpace, free,
		)
	}
	b.total += n
	b.active += n
	b.reserved += n
	b.files[path] += n
	return nil
}

func (b *remoteImmutableBudget) completeWrite(
	path string,
	reserved int64,
	written int64,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reserved -= reserved
	if written >= reserved {
		return
	}
	size := min(reserved-written, b.files[path])
	b.files[path] -= size
	b.active -= size
	if b.files[path] == 0 {
		delete(b.files, path)
	}
}

func (b *remoteImmutableBudget) release(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active -= b.files[path]
	delete(b.files, path)
}

func (b *remoteImmutableBudget) rename(oldPath, newPath string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	size := b.files[oldPath]
	delete(b.files, oldPath)
	b.active -= b.files[newPath]
	b.files[newPath] = size
}

type remoteImmutableBudgetWriter struct {
	file   *os.File
	path   string
	budget *remoteImmutableBudget
}

func (w remoteImmutableBudgetWriter) Write(data []byte) (int, error) {
	if err := w.budget.reserveWrite(w.path, len(data)); err != nil {
		return 0, err
	}
	n, err := w.file.Write(data)
	w.budget.completeWrite(w.path, int64(len(data)), int64(n))
	return n, err
}

func remoteImmutableLimitsForSlots(
	regularSlots uint64,
	canContainEBB bool,
) (remoteImmutableLimits, error) {
	if regularSlots == 0 {
		return remoteImmutableLimits{}, errors.New(
			"remote ImmutableDB requires a positive chunk slot count",
		)
	}
	// Ouroboros Consensus uses a uniform chunk sized from the initial era's
	// epoch. The primary index has one offset per regular slot, optionally one
	// EBB slot, and a final sentinel. Each offset is a uint32 after the version.
	slots := regularSlots
	if canContainEBB {
		if slots == math.MaxUint64 {
			return remoteImmutableLimits{}, errors.New(
				"remote ImmutableDB chunk slot count overflows uint64",
			)
		}
		slots++
	}
	if slots > (math.MaxInt64-remotePrimaryIndexVersionBytes)/
		remotePrimaryIndexOffsetBytes-1 {
		return remoteImmutableLimits{}, errors.New(
			"remote ImmutableDB primary index limit overflows int64",
		)
	}
	if slots > math.MaxInt64/blockfetch.StreamingMaxPendingMessageBytes {
		return remoteImmutableLimits{}, errors.New(
			"remote ImmutableDB chunk file limit overflows int64",
		)
	}
	return remoteImmutableLimits{
		primary: remotePrimaryIndexVersionBytes +
			int64(slots+1)*remotePrimaryIndexOffsetBytes, //nolint:gosec
		// Every secondary entry names one block or EBB. The block-fetch wire
		// budget is an upper bound on the encoded block stored in the chunk.
		secondary:  int64(slots) * remoteSecondaryIndexEntryBytes,             //nolint:gosec
		chunk:      int64(slots) * blockfetch.StreamingMaxPendingMessageBytes, //nolint:gosec
		concurrent: remoteImmutablePrefetch,
		chunkSlots: regularSlots,
	}, nil
}

// remoteImmutableRetryDelay is a variable so tests do not wait it out.
var remoteImmutableRetryDelay = time.Second

// classifyRemoteImmutableSource distinguishes a local directory from an
// authenticated remote root.
func classifyRemoteImmutableSource(source string) (bool, error) {
	u, err := url.Parse(source)
	if err != nil {
		if colon := strings.IndexByte(source, ':'); colon > 0 &&
			(strings.EqualFold(source[:colon], "http") ||
				strings.EqualFold(source[:colon], "https")) {
			return false, errors.New("invalid remote ImmutableDB source URL")
		}
		return false, nil
	}
	if !strings.EqualFold(u.Scheme, "http") &&
		!strings.EqualFold(u.Scheme, "https") {
		return false, nil
	}
	if u.Host == "" {
		return false, errors.New("remote ImmutableDB source requires a host")
	}
	if strings.EqualFold(u.Scheme, "http") && !isLoopbackRemoteURL(u) {
		return false, errors.New("remote ImmutableDB source requires HTTPS")
	}
	return true, nil
}

func canonicalRemoteImmutableRoot(source string) (string, error) {
	u, err := url.Parse(strings.TrimRight(source, "/"))
	if err != nil {
		return "", errors.New("invalid remote ImmutableDB source URL")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	return u.String(), nil
}

func remoteImmutableSourceCache(cacheDir, source string) (string, error) {
	canonical, err := canonicalRemoteImmutableRoot(source)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(canonical))
	return filepath.Join(cacheDir, hex.EncodeToString(digest[:16])), nil
}

func redactRemoteImmutableURL(value string) string {
	u, err := url.Parse(value)
	if err != nil {
		return "remote ImmutableDB URL"
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

// remoteImmutableCredentialToken matches whitespace and double-quote
// delimited words. Transport causes quote URLs that net/http could not parse
// (relative, scheme-relative, non-http, or containing apostrophes), so a word
// is treated as URL text by the characters that carry credentials rather than
// by its scheme.
var remoteImmutableCredentialToken = regexp.MustCompile(`[^\s"]+`)

const (
	remoteImmutableRedactedURL    = "remote ImmutableDB URL"
	remoteImmutableRedactedSecret = "[redacted]"
	remoteImmutableMinSecretLen   = 3
)

// remoteImmutableRequestSecrets returns the credential values carried by the
// request URL, so a cause that echoes them outside URL syntax is still
// scrubbed.
func remoteImmutableRequestSecrets(requestURL string) []string {
	u, err := url.Parse(requestURL)
	if err != nil {
		return nil
	}
	var secrets []string
	add := func(v string) {
		if len(v) >= remoteImmutableMinSecretLen {
			secrets = append(secrets, v)
		}
	}
	if u.User != nil {
		add(u.User.Username())
		if pw, ok := u.User.Password(); ok {
			add(pw)
		}
	}
	add(u.RawQuery)
	add(u.Fragment)
	add(u.RawFragment)
	for _, values := range u.Query() {
		for _, v := range values {
			add(v)
		}
	}
	return secrets
}

func scrubRemoteImmutableText(text string, secrets []string) string {
	for _, secret := range secrets {
		text = strings.ReplaceAll(text, secret, remoteImmutableRedactedSecret)
	}
	// net/http quotes an unparseable Location with %q. A double quote or
	// space inside that literal would split it into several words and leave
	// the credential in a word without ?, # or @, so a quoted literal is
	// judged and replaced whole.
	var b strings.Builder
	for {
		i := strings.IndexByte(text, '"')
		if i < 0 {
			b.WriteString(scrubRemoteImmutableWords(text))
			return b.String()
		}
		b.WriteString(scrubRemoteImmutableWords(text[:i]))
		text = text[i:]
		quoted, err := strconv.QuotedPrefix(text)
		if err != nil {
			b.WriteByte('"')
			text = text[1:]
			continue
		}
		if value, err := strconv.Unquote(quoted); err == nil &&
			strings.ContainsAny(value, "?#@") {
			b.WriteString(`"` + remoteImmutableRedactedURL + `"`)
		} else {
			b.WriteString(scrubRemoteImmutableWords(quoted))
		}
		text = text[len(quoted):]
	}
}

func scrubRemoteImmutableWords(text string) string {
	return remoteImmutableCredentialToken.ReplaceAllStringFunc(
		text,
		func(word string) string {
			if !strings.ContainsAny(word, "?#@") {
				return word
			}
			trimmed := strings.TrimRight(word, ":,.;)'")
			return remoteImmutableRedactedURL + word[len(trimmed):]
		},
	)
}

// redactedRemoteImmutableError keeps the original error reachable through
// Unwrap for errors.Is and errors.As while its message carries no URL
// userinfo, query or fragment.
type redactedRemoteImmutableError struct {
	msg     string
	err     error
	secrets []string
}

func (e *redactedRemoteImmutableError) Error() string { return e.msg }

func (e *redactedRemoteImmutableError) Unwrap() error { return e.err }

// As stops errors.As from handing out the original *url.Error, whose URL and
// message can carry credentials. Other targets traverse the original chain.
func (e *redactedRemoteImmutableError) As(target any) bool {
	urlErrTarget, ok := target.(**url.Error)
	if !ok {
		return false
	}
	var urlErr *url.Error
	if !errors.As(e.err, &urlErr) {
		return false
	}
	redacted := *urlErr
	redacted.URL = redactRemoteImmutableURL(urlErr.URL)
	redacted.Err = errors.New(
		scrubRemoteImmutableText(fmt.Sprint(urlErr.Err), e.secrets),
	)
	*urlErrTarget = &redacted
	return true
}

func remoteImmutableRequestError(action, requestURL string, err error) error {
	secrets := remoteImmutableRequestSecrets(requestURL)
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		secrets = append(secrets, remoteImmutableRequestSecrets(urlErr.URL)...)
		err = urlErr.Err
	}
	msg := fmt.Sprintf(
		"%s %s: %s", action, redactRemoteImmutableURL(requestURL),
		scrubRemoteImmutableText(err.Error(), secrets),
	)
	return &redactedRemoteImmutableError{msg: msg, err: err, secrets: secrets}
}

func isLoopbackRemoteURL(u *url.URL) bool {
	host := u.Hostname()
	ip := net.ParseIP(host)
	return strings.EqualFold(host, "localhost") || ip != nil && ip.IsLoopback()
}

type remoteImmutableNetworkPolicy struct {
	allowLoopback bool
	dial          netguard.DialContextFunc
	lookup        netguard.LookupIPAddrFunc
}

func newRemoteImmutableHTTPClient(rootURL string) (*http.Client, error) {
	u, err := url.Parse(rootURL)
	if err != nil {
		return nil, errors.New("invalid remote ImmutableDB source URL")
	}
	policy := remoteImmutableNetworkPolicy{
		allowLoopback: strings.EqualFold(u.Scheme, "http") &&
			isLoopbackRemoteURL(u),
		dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		lookup: net.DefaultResolver.LookupIPAddr,
	}
	if err := policy.validateURL(u); err != nil {
		return nil, err
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("remote ImmutableDB HTTP transport is unavailable")
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.DialContext = policy.dialContext
	transport.Dial = nil    //nolint:staticcheck
	transport.DialTLS = nil //nolint:staticcheck
	transport.DialTLSContext = nil
	transport.MaxConnsPerHost = remoteImmutablePrefetch
	return &http.Client{
		Timeout:       remoteImmutableFileTimeout,
		Transport:     transport,
		CheckRedirect: policy.checkRedirect,
	}, nil
}

func (p remoteImmutableNetworkPolicy) validateURL(u *url.URL) error {
	if u == nil || u.Hostname() == "" {
		return errors.New("remote ImmutableDB destination requires a host")
	}
	if u.User != nil {
		return errors.New("remote ImmutableDB destination must not contain credentials")
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
	case "http":
		if !p.allowLoopback || !isLoopbackRemoteURL(u) {
			return errors.New("remote ImmutableDB destination requires HTTPS")
		}
	default:
		return errors.New("remote ImmutableDB destination requires HTTP or HTTPS")
	}
	host := u.Hostname()
	if netguard.IsBlockedHost(host) {
		if p.allowLoopback && isLoopbackRemoteURL(u) {
			return nil
		}
		return fmt.Errorf(
			"remote ImmutableDB host %q is not allowed: %w",
			host, netguard.ErrBlockedDestination,
		)
	}
	if ip := net.ParseIP(host); ip != nil && netguard.IsBlockedIP(ip) {
		if p.allowLoopback && ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf(
			"remote ImmutableDB IP %s is not allowed: %w",
			ip, netguard.ErrBlockedDestination,
		)
	}
	return nil
}

func (p remoteImmutableNetworkPolicy) checkRedirect(
	req *http.Request,
	via []*http.Request,
) error {
	if len(via) >= 10 {
		return errors.New("remote ImmutableDB stopped after 10 redirects")
	}
	return p.validateURL(req.URL)
}

func (p remoteImmutableNetworkPolicy) dialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	loopback := strings.EqualFold(host, "localhost")
	if ip := net.ParseIP(host); ip != nil {
		loopback = ip.IsLoopback()
	}
	if !p.allowLoopback || !loopback {
		return netguard.DialContext(ctx, network, address, p.dial, p.lookup)
	}
	addrs, err := p.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("%w for host %q", netguard.ErrNoAddresses, host)
	}
	for _, addr := range addrs {
		if addr.IP == nil || !addr.IP.IsLoopback() {
			return nil, fmt.Errorf(
				"resolved IP %s is not allowed: %w",
				addr.IP, netguard.ErrBlockedDestination,
			)
		}
	}
	var lastErr error
	for _, addr := range addrs {
		conn, dialErr := p.dial(
			ctx, network, net.JoinHostPort(addr.IP.String(), port),
		)
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	return nil, lastErr
}

// remoteImmutableTip is the tip.json document a Genesis Sync Accelerator
// style root publishes beside its chunk files.
type remoteImmutableTip struct {
	Slot uint64 `json:"slot"`
	Hash string `json:"hash"`
}

// copyBlocksRemote loads blocks from an HTTPS or loopback HTTP ImmutableDB
// root. Chunk triads download into staging up to remoteImmutablePrefetch
// ahead, in any order, and move into the ready directory strictly in chunk
// order, so copyBlocksDirect only ever reads a contiguous prefix. Copying
// stops when the chain reaches the remote tip or the root publishes no next
// chunk.
// Copied chunks below the one holding the chain tip are evicted, and that
// chunk stays, so a later run resumes from it.
func copyBlocksRemote(
	ctx context.Context,
	logger *slog.Logger,
	rootURL string,
	cacheDir string,
	limits remoteImmutableLimits,
	c *chain.Chain,
	replayBatches chan<- []gledger.Block,
) (int, uint64, error) {
	remote, err := classifyRemoteImmutableSource(rootURL)
	if err != nil {
		return 0, 0, err
	}
	if !remote {
		return 0, 0, errors.New("remote ImmutableDB source requires HTTP or HTTPS")
	}
	rootURL, err = canonicalRemoteImmutableRoot(rootURL)
	if err != nil {
		return 0, 0, fmt.Errorf("canonicalizing remote ImmutableDB source: %w", err)
	}
	client, err := newRemoteImmutableHTTPClient(rootURL)
	if err != nil {
		return 0, 0, fmt.Errorf("validating remote ImmutableDB source: %w", err)
	}
	tip, err := fetchRemoteImmutableTipWithClient(ctx, rootURL, client)
	if err != nil {
		return 0, 0, err
	}
	sourceCache, err := remoteImmutableSourceCache(cacheDir, rootURL)
	if err != nil {
		return 0, 0, fmt.Errorf("selecting remote ImmutableDB cache: %w", err)
	}
	stagingDir := filepath.Join(sourceCache, "staging")
	readyDir := filepath.Join(sourceCache, "ready")
	first := uint64(0)
	if chainTip := c.Tip(); chainTip.Point.Slot == 0 &&
		len(chainTip.Point.Hash) == 0 {
		// Nothing was copied yet, so whatever is ready belongs to another
		// load and would not be contiguous with chunk 0.
		if err := os.RemoveAll(readyDir); err != nil {
			return 0, 0, fmt.Errorf("clearing remote chunk cache: %w", err)
		}
	} else if lowest, ok, err := lowestChunk(readyDir); err != nil {
		return 0, 0, err
	} else if ok {
		first = lowest
	}
	if limits.imported == 0 {
		limits, err = limits.withImportRange(first, tip.Slot)
		if err != nil {
			return 0, 0, err
		}
	}
	defer func() {
		_ = os.RemoveAll(stagingDir)
	}()
	for _, dir := range []string{stagingDir, readyDir} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return 0, 0, fmt.Errorf("creating remote chunk cache: %w", err)
		}
	}
	budget, err := newRemoteImmutableBudget(sourceCache, limits)
	if err != nil {
		return 0, 0, err
	}

	fetchCtx, cancelFetch := context.WithCancel(ctx)
	pending := make([]chan error, 0, remoteImmutablePrefetch)
	defer func() {
		cancelFetch()
		for _, done := range pending {
			if err := <-done; err != nil &&
				!isExpectedRemotePrefetchCleanupError(err) {
				logger.Warn("remote prefetch cleanup failed", "error", err)
			}
		}
	}()
	next := first
	fetchNext := func() {
		done := make(chan error, 1)
		go func(chunk uint64) {
			done <- fetchRemoteChunkWithClient(
				fetchCtx, client, budget, rootURL, stagingDir, chunk, limits,
			)
		}(next)
		pending = append(pending, done)
		next++
	}
	for range limits.concurrent {
		fetchNext()
	}

	var blocksCopied int
	for chunk := first; ; chunk++ {
		err := <-pending[0]
		pending = pending[1:]
		if errors.Is(err, errRemoteChunkNotPublished) {
			if chunk == 0 {
				return 0, tip.Slot, fmt.Errorf(
					"remote ImmutableDB %s: chunk 0: %w",
					redactRemoteImmutableURL(rootURL), err,
				)
			}
			logger.Info(
				"remote ImmutableDB publishes no further chunks",
				"chunk", chunk,
				"chain_tip_slot", c.Tip().Point.Slot,
				"remote_tip_slot", tip.Slot,
			)
			break
		}
		if err != nil {
			return blocksCopied, tip.Slot, err
		}
		fetchNext()
		if err := moveRemoteChunk(
			stagingDir, readyDir, chunk, budget,
		); err != nil {
			return blocksCopied, tip.Slot, err
		}
		copied, _, err := copyBlocksDirect(
			ctx, logger, readyDir, c, replayBatches,
		)
		blocksCopied += copied
		if err != nil {
			return blocksCopied, tip.Slot, fmt.Errorf(
				"chunk %s: %w", immutable.ChunkName(chunk), err,
			)
		}
		if err := evictRemoteChunksBefore(readyDir, chunk, budget); err != nil {
			return blocksCopied, tip.Slot, err
		}
		chainTip := c.Tip().Point
		logger.Info(
			"loaded remote ImmutableDB chunk",
			"chunk", chunk,
			"chain_tip_slot", chainTip.Slot,
			"remote_tip_slot", tip.Slot,
		)
		if chainTip.Slot >= tip.Slot {
			if chainTip.Slot == tip.Slot &&
				!strings.EqualFold(
					hex.EncodeToString(chainTip.Hash),
					tip.Hash,
				) {
				return blocksCopied, tip.Slot, fmt.Errorf(
					"remote ImmutableDB tip mismatch at slot %d: "+
						"tip.json hash %s, loaded block %x",
					tip.Slot, tip.Hash, chainTip.Hash,
				)
			}
			break
		}
	}
	return blocksCopied, tip.Slot, nil
}

func fetchRemoteImmutableTipWithClient(
	ctx context.Context,
	rootURL string,
	client *http.Client,
) (remoteImmutableTip, error) {
	var tip remoteImmutableTip
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, rootURL+"/tip.json", nil,
	)
	if err != nil {
		return tip, fmt.Errorf(
			"creating remote ImmutableDB tip request for %s",
			redactRemoteImmutableURL(rootURL+"/tip.json"),
		)
	}
	httpuseragent.Set(req)
	resp, err := client.Do(req)
	if err != nil {
		return tip, remoteImmutableRequestError(
			"fetching remote ImmutableDB tip", rootURL+"/tip.json", err,
		)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return tip, fmt.Errorf(
			"fetching remote ImmutableDB tip: %s", resp.Status,
		)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).
		Decode(&tip); err != nil {
		return tip, fmt.Errorf("decoding remote ImmutableDB tip: %w", err)
	}
	return tip, nil
}

// fetchRemoteChunk downloads one chunk triad into dir. A file already present
// under its final name is complete, because it is only renamed into place
// after a full download.
func fetchRemoteChunkWithClient(
	ctx context.Context,
	client *http.Client,
	budget *remoteImmutableBudget,
	rootURL string,
	dir string,
	chunk uint64,
	limits remoteImmutableLimits,
) error {
	name := immutable.ChunkName(chunk)
	primaryName := name + ".primary"
	if err := fetchRemoteFileWithClient(
		ctx, client, budget, rootURL, dir, primaryName, limits.primary, 0,
	); err != nil {
		if errors.Is(err, errRemoteChunkNotPublished) {
			published, probeErr := remoteImmutableFilePublishedWithClient(
				ctx, client, rootURL+"/"+name+".chunk",
			)
			if probeErr != nil {
				return probeErr
			}
			if !published {
				return err
			}
			return fmt.Errorf("remote chunk %s is missing .primary", name)
		}
		return err
	}
	secondarySize, err := remoteSecondarySize(
		filepath.Join(dir, primaryName), limits.primary,
	)
	if err != nil {
		return fmt.Errorf("remote chunk %s primary index: %w", name, err)
	}
	secondaryName := name + ".secondary"
	if err := fetchRemoteFileWithClient(
		ctx, client, budget, rootURL, dir,
		secondaryName, secondarySize, secondarySize,
	); err != nil {
		if errors.Is(err, errRemoteChunkNotPublished) {
			return fmt.Errorf("remote chunk %s is missing .secondary", name)
		}
		return err
	}
	if err := fetchRemoteFileWithClient(
		ctx, client, budget, rootURL, dir, name+".chunk", limits.chunk, 0,
	); err != nil {
		return err
	}
	immDB, err := immutable.New(dir)
	if err != nil {
		return fmt.Errorf("opening remote chunk %s: %w", name, err)
	}
	if err := immDB.ValidateChunkBlockSpans(name); err != nil {
		return fmt.Errorf("remote chunk %s block offsets: %w", name, err)
	}
	return nil
}

func remoteImmutableFilePublishedWithClient(
	ctx context.Context,
	client *http.Client,
	fileURL string,
) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return false, fmt.Errorf(
			"creating remote ImmutableDB probe request for %s",
			redactRemoteImmutableURL(fileURL),
		)
	}
	httpuseragent.Set(req)
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		return false, remoteImmutableRequestError("probing", fileURL, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf(
			"probing %s: %s", redactRemoteImmutableURL(fileURL), resp.Status,
		)
	}
}

func remoteSecondarySize(primaryPath string, primaryLimit int64) (int64, error) {
	data, err := os.ReadFile(primaryPath) // #nosec G304 -- internal cache path
	if err != nil {
		return 0, err
	}
	if int64(len(data)) > primaryLimit {
		return 0, errRemoteImmutableFileTooLarge
	}
	if len(data) < remotePrimaryIndexVersionBytes+remotePrimaryIndexOffsetBytes ||
		(len(data)-remotePrimaryIndexVersionBytes)%remotePrimaryIndexOffsetBytes != 0 {
		return 0, errors.New("invalid primary index length")
	}
	if data[0] != 1 {
		return 0, fmt.Errorf("unsupported primary index version %d", data[0])
	}
	offset := binary.BigEndian.Uint32(data[len(data)-remotePrimaryIndexOffsetBytes:])
	if offset%remoteSecondaryIndexEntryBytes != 0 {
		return 0, errors.New("unaligned final secondary index offset")
	}
	maxSecondary := int64((len(data)-remotePrimaryIndexVersionBytes)/
		remotePrimaryIndexOffsetBytes) * remoteSecondaryIndexEntryBytes
	if int64(offset) > maxSecondary {
		return 0, errors.New("secondary index exceeds primary slot count")
	}
	return int64(offset), nil
}

func fetchRemoteFileWithClient(
	ctx context.Context,
	client *http.Client,
	budget *remoteImmutableBudget,
	rootURL string,
	dir string,
	filename string,
	maxBytes int64,
	exactBytes int64,
) error {
	target := filepath.Join(dir, filename)
	if info, err := os.Stat(target); err == nil {
		return validateRemoteFileSize(info.Size(), maxBytes, exactBytes)
	}
	var err error
	for attempt := range remoteImmutableAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			case <-time.After(remoteImmutableRetryDelay):
			}
		}
		err = fetchRemoteFileOnceWithClient(
			ctx, client, budget, rootURL+"/"+filename,
			target+".part", maxBytes,
		)
		if err == nil {
			info, statErr := os.Stat(target + ".part")
			if statErr != nil {
				return statErr
			}
			if sizeErr := validateRemoteFileSize(
				info.Size(), maxBytes, exactBytes,
			); sizeErr != nil {
				return sizeErr
			}
			if renameErr := os.Rename(target+".part", target); renameErr != nil {
				return renameErr
			}
			budget.rename(target+".part", target)
			return nil
		}
		if errors.Is(err, errRemoteChunkNotPublished) ||
			errors.Is(err, errRemoteImmutableFileTooLarge) ||
			errors.Is(err, errRemoteImmutableImportBudget) ||
			errors.Is(err, errRemoteImmutableCacheBudget) ||
			errors.Is(err, errRemoteImmutableFreeSpace) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

func validateRemoteFileSize(size, maxBytes, exactBytes int64) error {
	if size < 0 || size > maxBytes {
		return fmt.Errorf(
			"%w: got %d bytes, maximum %d",
			errRemoteImmutableFileTooLarge, size, maxBytes,
		)
	}
	if exactBytes > 0 && size != exactBytes {
		return fmt.Errorf(
			"remote ImmutableDB file has %d bytes, expected %d",
			size, exactBytes,
		)
	}
	return nil
}

func fetchRemoteFileOnceWithClient(
	ctx context.Context,
	client *http.Client,
	budget *remoteImmutableBudget,
	fileURL string,
	partPath string,
	maxBytes int64,
) error {
	var offset int64
	if info, err := os.Stat(partPath); err == nil {
		offset = info.Size()
	}
	if err := validateRemoteFileSize(offset, maxBytes, 0); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return fmt.Errorf(
			"creating remote ImmutableDB request for %s",
			redactRemoteImmutableURL(fileURL),
		)
	}
	httpuseragent.Set(req)
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	resp, err := client.Do(req)
	if err != nil {
		return remoteImmutableRequestError("fetching", fileURL, err)
	}
	defer resp.Body.Close()
	flags := os.O_CREATE | os.O_WRONLY
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf(
			"%s: %w", redactRemoteImmutableURL(fileURL),
			errRemoteChunkNotPublished,
		)
	case resp.StatusCode == http.StatusPartialContent && offset > 0 &&
		strings.HasPrefix(
			resp.Header.Get("Content-Range"),
			"bytes "+strconv.FormatInt(offset, 10)+"-",
		):
		flags |= os.O_APPEND
	case resp.StatusCode == http.StatusOK:
		flags |= os.O_TRUNC
	default:
		return fmt.Errorf(
			"fetching %s: %s", redactRemoteImmutableURL(fileURL), resp.Status,
		)
	}
	remaining := maxBytes
	if flags&os.O_APPEND != 0 {
		remaining -= offset
	}
	if resp.ContentLength > remaining {
		return fmt.Errorf(
			"%w: response declares %d bytes with %d remaining",
			errRemoteImmutableFileTooLarge, resp.ContentLength, remaining,
		)
	}
	file, err := os.OpenFile(
		partPath,
		flags,
		0o640,
	) //nolint:gosec // cache path built from a chunk number
	if err != nil {
		return err
	}
	if flags&os.O_TRUNC != 0 && offset > 0 {
		budget.release(partPath)
	}
	written, copyErr := io.CopyBuffer(
		remoteImmutableBudgetWriter{
			file: file, path: partPath, budget: budget,
		},
		io.LimitReader(resp.Body, remaining+1),
		make([]byte, 1<<20),
	)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return errors.Join(copyErr, closeErr)
	}
	if written > remaining {
		budget.release(partPath)
		_ = os.Remove(partPath)
		return fmt.Errorf(
			"%w: response exceeded %d remaining bytes",
			errRemoteImmutableFileTooLarge, remaining,
		)
	}
	return nil
}

// moveRemoteChunk moves a downloaded triad into the ready directory, .chunk
// last: the reader lists chunks by their .chunk file, so a crash part way
// through never exposes a chunk without its indexes.
func moveRemoteChunk(
	stagingDir string,
	readyDir string,
	chunk uint64,
	budget *remoteImmutableBudget,
) error {
	name := immutable.ChunkName(chunk)
	for _, ext := range []string{".primary", ".secondary", ".chunk"} {
		oldPath := filepath.Join(stagingDir, name+ext)
		newPath := filepath.Join(readyDir, name+ext)
		if err := os.Rename(oldPath, newPath); err != nil {
			return fmt.Errorf("moving remote chunk %s: %w", name, err)
		}
		budget.rename(oldPath, newPath)
	}
	return nil
}

// evictRemoteChunksBefore removes the copied chunks below chunk once chunk
// itself holds a block. An empty chunk evicts nothing, so the ready set keeps
// the chunk that holds the chain tip, which the next copy starts from.
func evictRemoteChunksBefore(
	readyDir string,
	chunk uint64,
	budget *remoteImmutableBudget,
) error {
	imm, err := immutable.New(readyDir)
	if err != nil {
		return err
	}
	if _, hasBlocks, err := imm.LastSlotInChunk(chunk); err != nil ||
		!hasBlocks {
		return err
	}
	entries, err := os.ReadDir(readyDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		number, ok := chunkNumber(entry.Name())
		if ok && number < chunk {
			path := filepath.Join(readyDir, entry.Name())
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("evicting remote chunk: %w", err)
			}
			budget.release(path)
		}
	}
	return nil
}

func lowestChunk(readyDir string) (uint64, bool, error) {
	entries, err := os.ReadDir(readyDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var (
		lowest uint64
		found  bool
	)
	for _, entry := range entries {
		number, ok := chunkNumber(entry.Name())
		if ok && filepath.Ext(entry.Name()) == ".chunk" &&
			(!found || number < lowest) {
			lowest, found = number, true
		}
	}
	return lowest, found, nil
}

// chunkNumber parses the chunk number of a triad file name.
func chunkNumber(filename string) (uint64, bool) {
	base, _, ok := strings.Cut(filename, ".")
	if !ok {
		return 0, false
	}
	number, err := strconv.ParseUint(base, 10, 64)
	return number, err == nil
}

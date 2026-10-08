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
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
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
)

// errRemoteChunkNotPublished reports that the remote root serves no .chunk
// file for a chunk number, which ends the contiguous published history.
var errRemoteChunkNotPublished = errors.New("remote chunk not published")

var errRemoteImmutableFileTooLarge = errors.New(
	"remote ImmutableDB file exceeds its format limit",
)

const (
	remotePrimaryIndexVersionBytes = 1
	remotePrimaryIndexOffsetBytes  = 4
	remoteSecondaryIndexEntryBytes = 56
)

type remoteImmutableLimits struct {
	primary int64
	chunk   int64
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
		chunk: int64(slots) * blockfetch.StreamingMaxPendingMessageBytes, //nolint:gosec
	}, nil
}

var remoteImmutableClient = &http.Client{
	Timeout:       remoteImmutableFileTimeout,
	CheckRedirect: checkRemoteImmutableRedirect,
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
	return u.String()
}

func remoteImmutableRequestError(action, requestURL string, err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		err = urlErr.Err
	}
	return fmt.Errorf(
		"%s %s: %w", action, redactRemoteImmutableURL(requestURL), err,
	)
}

func isLoopbackRemoteURL(u *url.URL) bool {
	host := u.Hostname()
	ip := net.ParseIP(host)
	return strings.EqualFold(host, "localhost") || ip != nil && ip.IsLoopback()
}

func checkRemoteImmutableRedirect(
	req *http.Request,
	via []*http.Request,
) error {
	if len(via) >= 10 {
		return errors.New("remote ImmutableDB stopped after 10 redirects")
	}
	if strings.EqualFold(req.URL.Scheme, "https") {
		return nil
	}
	if strings.EqualFold(req.URL.Scheme, "http") && len(via) > 0 {
		previous := via[len(via)-1]
		if strings.EqualFold(previous.URL.Scheme, "http") &&
			isLoopbackRemoteURL(previous.URL) && isLoopbackRemoteURL(req.URL) {
			return nil
		}
	}
	return errors.New("remote ImmutableDB redirect requires HTTPS")
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
	tip, err := fetchRemoteImmutableTip(ctx, rootURL)
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
	for _, dir := range []string{stagingDir, readyDir} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return 0, 0, fmt.Errorf("creating remote chunk cache: %w", err)
		}
	}

	fetchCtx, cancelFetch := context.WithCancel(ctx)
	pending := make([]chan error, 0, remoteImmutablePrefetch)
	defer func() {
		cancelFetch()
		for _, done := range pending {
			<-done
		}
	}()
	next := first
	fetchNext := func() {
		done := make(chan error, 1)
		go func(chunk uint64) {
			done <- fetchRemoteChunk(
				fetchCtx, rootURL, stagingDir, chunk, limits,
			)
		}(next)
		pending = append(pending, done)
		next++
	}
	for range remoteImmutablePrefetch {
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
		if err := moveRemoteChunk(stagingDir, readyDir, chunk); err != nil {
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
		if err := evictRemoteChunksBefore(readyDir, chunk); err != nil {
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

func fetchRemoteImmutableTip(
	ctx context.Context,
	rootURL string,
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
	resp, err := remoteImmutableClient.Do(req)
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
func fetchRemoteChunk(
	ctx context.Context,
	rootURL string,
	dir string,
	chunk uint64,
	limits remoteImmutableLimits,
) error {
	name := immutable.ChunkName(chunk)
	primaryName := name + ".primary"
	if err := fetchRemoteFile(
		ctx, rootURL, dir, primaryName, limits.primary, 0,
	); err != nil {
		if errors.Is(err, errRemoteChunkNotPublished) {
			published, probeErr := remoteImmutableFilePublished(
				ctx, rootURL+"/"+name+".chunk",
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
	if err := fetchRemoteFile(
		ctx, rootURL, dir, secondaryName, secondarySize, secondarySize,
	); err != nil {
		if errors.Is(err, errRemoteChunkNotPublished) {
			return fmt.Errorf("remote chunk %s is missing .secondary", name)
		}
		return err
	}
	if err := fetchRemoteFile(
		ctx, rootURL, dir, name+".chunk", limits.chunk, 0,
	); err != nil {
		return err
	}
	return nil
}

func remoteImmutableFilePublished(ctx context.Context, fileURL string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return false, fmt.Errorf(
			"creating remote ImmutableDB probe request for %s",
			redactRemoteImmutableURL(fileURL),
		)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := remoteImmutableClient.Do(req)
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

func fetchRemoteFile(
	ctx context.Context,
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
		err = fetchRemoteFileOnce(
			ctx, rootURL+"/"+filename, target+".part", maxBytes,
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
			return os.Rename(target+".part", target)
		}
		if errors.Is(err, errRemoteChunkNotPublished) ||
			errors.Is(err, errRemoteImmutableFileTooLarge) || ctx.Err() != nil {
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

// fetchRemoteFileOnce appends to a partial file left by an interrupted
// attempt when the server honours the range, and restarts it otherwise.
func fetchRemoteFileOnce(
	ctx context.Context,
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
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	resp, err := remoteImmutableClient.Do(req)
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
	written, copyErr := io.Copy(file, io.LimitReader(resp.Body, remaining+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return errors.Join(copyErr, closeErr)
	}
	if written > remaining {
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
func moveRemoteChunk(stagingDir, readyDir string, chunk uint64) error {
	name := immutable.ChunkName(chunk)
	for _, ext := range []string{".primary", ".secondary", ".chunk"} {
		if err := os.Rename(
			filepath.Join(stagingDir, name+ext),
			filepath.Join(readyDir, name+ext),
		); err != nil {
			return fmt.Errorf("moving remote chunk %s: %w", name, err)
		}
	}
	return nil
}

// evictRemoteChunksBefore removes the copied chunks below chunk once chunk
// itself holds a block. An empty chunk evicts nothing, so the ready set keeps
// the chunk that holds the chain tip, which the next copy starts from.
func evictRemoteChunksBefore(readyDir string, chunk uint64) error {
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
			if err := os.Remove(
				filepath.Join(readyDir, entry.Name()),
			); err != nil {
				return fmt.Errorf("evicting remote chunk: %w", err)
			}
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

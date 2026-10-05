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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
			return false, fmt.Errorf("invalid remote ImmutableDB source: %w", err)
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
	rootURL = strings.TrimRight(rootURL, "/")
	tip, err := fetchRemoteImmutableTip(ctx, rootURL)
	if err != nil {
		return 0, 0, err
	}
	stagingDir := filepath.Join(cacheDir, "staging")
	readyDir := filepath.Join(cacheDir, "ready")
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
			done <- fetchRemoteChunk(fetchCtx, rootURL, stagingDir, chunk)
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
					"remote ImmutableDB %s: chunk 0: %w", rootURL, err,
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
		return tip, err
	}
	resp, err := remoteImmutableClient.Do(req)
	if err != nil {
		return tip, fmt.Errorf("fetching remote ImmutableDB tip: %w", err)
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
) error {
	name := immutable.ChunkName(chunk)
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		err := fetchRemoteFile(ctx, rootURL, dir, name+ext)
		if errors.Is(err, errRemoteChunkNotPublished) && ext != ".chunk" {
			return fmt.Errorf("remote chunk %s is missing %s", name, ext)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func fetchRemoteFile(
	ctx context.Context,
	rootURL string,
	dir string,
	filename string,
) error {
	target := filepath.Join(dir, filename)
	if _, err := os.Stat(target); err == nil {
		return nil
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
		err = fetchRemoteFileOnce(ctx, rootURL+"/"+filename, target+".part")
		if err == nil {
			return os.Rename(target+".part", target)
		}
		if errors.Is(err, errRemoteChunkNotPublished) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

// fetchRemoteFileOnce appends to a partial file left by an interrupted
// attempt when the server honours the range, and restarts it otherwise.
func fetchRemoteFileOnce(ctx context.Context, fileURL, partPath string) error {
	var offset int64
	if info, err := os.Stat(partPath); err == nil {
		offset = info.Size()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return err
	}
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	resp, err := remoteImmutableClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetching %s: %w", fileURL, err)
	}
	defer resp.Body.Close()
	flags := os.O_CREATE | os.O_WRONLY
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%s: %w", fileURL, errRemoteChunkNotPublished)
	case resp.StatusCode == http.StatusPartialContent && offset > 0 &&
		strings.HasPrefix(
			resp.Header.Get("Content-Range"),
			"bytes "+strconv.FormatInt(offset, 10)+"-",
		):
		flags |= os.O_APPEND
	case resp.StatusCode == http.StatusOK:
		flags |= os.O_TRUNC
	default:
		return fmt.Errorf("fetching %s: %s", fileURL, resp.Status)
	}
	file, err := os.OpenFile(
		partPath,
		flags,
		0o640,
	) //nolint:gosec // cache path built from a chunk number
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, resp.Body)
	return errors.Join(copyErr, file.Close())
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

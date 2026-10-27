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
	"io"
	"net"
	"net/http"
	"net/url"
)

// ErrSnapshotRestoreUnavailable is wrapped by a RunSnapshotBackfill error
// caused by a source the restore reads from not answering usefully: the
// aggregator or a download location unreachable, timing out, rate limiting,
// failing with a server error, or answering 404 for anything but the pinned
// artifact. It says nothing about the database, so a caller retrying an
// interrupted restore should back off and retry rather than count it
// against the restore.
var ErrSnapshotRestoreUnavailable = errors.New(
	"mithril: snapshot restore source unavailable",
)

// errPinnedArtifactNotServed is wrapped by selectVerifiedArtifact's error
// when the aggregator answered 404 for the pinned artifact itself.
var errPinnedArtifactNotServed = errors.New(
	"the aggregator does not serve the pinned artifact",
)

// isSourceUnavailable reports whether err comes from a remote source not
// answering usefully rather than from what it answered.
func isSourceUnavailable(err error) bool {
	if err == nil || errors.Is(err, errPinnedArtifactNotServed) {
		return false
	}
	if status, ok := errors.AsType[*aggregatorStatusError](err); ok {
		code := status.code
		return code == http.StatusNotFound ||
			code == http.StatusRequestTimeout ||
			code == http.StatusTooManyRequests ||
			code >= http.StatusInternalServerError
	}
	if archive, ok := errors.AsType[*ImmutableArchiveError](err); ok {
		return archiveSourcesUnavailable(archive)
	}
	if errors.Is(err, errDownloadTransient) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if _, ok := errors.AsType[*url.Error](err); ok {
		return true
	}
	_, ok := errors.AsType[net.Error](err)
	return ok
}

// archiveSourcesUnavailable reports whether every remote location tried for
// an immutable trio failed to deliver it, rather than delivering bytes that
// disagree with the certified digests.
func archiveSourcesUnavailable(e *ImmutableArchiveError) bool {
	remote := 0
	for _, attempt := range e.Attempts {
		if attempt.Location == 0 {
			continue
		}
		remote++
		if attempt.Mismatch() != nil || !isSourceUnavailable(attempt.Err) {
			return false
		}
	}
	return remote > 0
}

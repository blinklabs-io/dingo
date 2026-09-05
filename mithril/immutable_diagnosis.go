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
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// DigestMismatchError reports a file whose bytes do not match the digest the
// artifact's certified digest list carries for it.
//
// It is a diagnosis, never a relaxation: every path that produces one still
// refuses the bytes. It exists so the refusal can name the file, both digests
// and the source that served them, which is what an operator needs to tell a
// bad replica apart from a bad local cache and to report a mis-published
// archive.
type DigestMismatchError struct {
	// FileName is the immutable file the digest list names ("05471.chunk").
	FileName string
	// Expected is the certified digest; Observed is what the bytes hashed to.
	Expected string
	Observed string
}

func (e *DigestMismatchError) Error() string {
	return fmt.Sprintf(
		"digest mismatch for %s: computed %s, expected %s",
		e.FileName,
		e.Observed,
		e.Expected,
	)
}

// immutableSourceCache is the source label used for an already-extracted trio
// reused from the download cache, as opposed to a location URI.
const immutableSourceCache = "local extraction cache"

// ImmutableArchiveAttempt records one source tried for an immutable trio.
type ImmutableArchiveAttempt struct {
	// Source is the local cache label or the redacted location URI.
	Source string
	// Location is the 1-based index into the artifact's immutable location
	// list, or 0 for the local cache.
	Location int
	// Err is why the attempt was rejected.
	Err error
}

// Mismatch returns the digest mismatch this attempt was rejected for, or nil
// when it failed for another reason (a download or extraction failure).
func (a ImmutableArchiveAttempt) Mismatch() *DigestMismatchError {
	var mismatch *DigestMismatchError
	if errors.As(a.Err, &mismatch) {
		return mismatch
	}
	return nil
}

// ImmutableArchiveError reports that no source produced an immutable trio
// matching the artifact's certified digest list, with the per-source evidence
// needed to compare replicas.
//
// Fail-closed is the point: the digest list is verified against the artifact's
// certificate merkle root before any archive is fetched, so bytes that
// disagree with it are refused whatever their source. This type only makes the
// refusal actionable.
type ImmutableArchiveError struct {
	ArtifactHash        string
	Epoch               uint64
	ImmutableFileNumber uint64
	// Attempts is every source tried, in order, including the local cache
	// when a cached trio was present and rejected.
	Attempts []ImmutableArchiveAttempt
	// Locations is how many published locations the artifact carried.
	Locations int
}

// sameBytesEverywhere reports whether every remote location was rejected for a
// digest mismatch on the same file with the same observed digest: the artifact
// is published with bytes that disagree with its own certified digest list,
// rather than one replica being stale or corrupt.
func (e *ImmutableArchiveError) sameBytesEverywhere() (*DigestMismatchError, bool) {
	var first *DigestMismatchError
	remote := 0
	for _, attempt := range e.Attempts {
		if attempt.Location == 0 {
			continue
		}
		remote++
		mismatch := attempt.Mismatch()
		if mismatch == nil {
			return nil, false
		}
		if first == nil {
			first = mismatch
			continue
		}
		if mismatch.FileName != first.FileName ||
			mismatch.Observed != first.Observed {
			return nil, false
		}
	}
	if first == nil || remote < 2 || remote != e.Locations {
		return nil, false
	}
	return first, true
}

func (e *ImmutableArchiveError) Error() string {
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"immutable archive %05d of Mithril artifact %s "+
			"(epoch %d, immutable file number %d) matched no location",
		e.ImmutableFileNumber,
		e.ArtifactHash,
		e.Epoch,
		e.ImmutableFileNumber,
	)
	if mismatch, ok := e.sameBytesEverywhere(); ok {
		fmt.Fprintf(
			&b,
			": all %d published locations served identical bytes for %s "+
				"(sha256 %s) that the certified digest list does not cover "+
				"(it expects %s), so the published archive disagrees with "+
				"its own certificate rather than one replica being stale; "+
				"report the artifact hash and immutable file number to the "+
				"aggregator operator",
			e.Locations,
			mismatch.FileName,
			mismatch.Observed,
			mismatch.Expected,
		)
	}
	for _, attempt := range e.Attempts {
		if attempt.Location == 0 {
			fmt.Fprintf(&b, "; cache (%s): %v", attempt.Source, attempt.Err)
			continue
		}
		fmt.Fprintf(
			&b,
			"; location %d/%d (%s): %v",
			attempt.Location,
			e.Locations,
			attempt.Source,
			attempt.Err,
		)
	}
	return b.String()
}

// Unwrap exposes the last attempt's cause so errors.Is/errors.As still reach
// the underlying download, extraction or mismatch error.
func (e *ImmutableArchiveError) Unwrap() error {
	if len(e.Attempts) == 0 {
		return nil
	}
	return e.Attempts[len(e.Attempts)-1].Err
}

// redactLocationURI reduces an archive URI to scheme, host and path.
//
// Cloud-storage locations are routinely pre-signed, carrying credentials and a
// signature in the query string, and a userinfo component carries them
// outright. Neither belongs in an error an operator is asked to paste into a
// bug report, while scheme+host+path is exactly what identifies the replica.
// A URI that will not parse is reported as its scheme-and-host prefix only, or
// as "unparsable location" when it has none.
func redactLocationURI(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "unparsable location"
	}
	redacted := url.URL{
		Scheme: parsed.Scheme,
		Host:   parsed.Host,
		Path:   parsed.Path,
	}
	return redacted.String()
}

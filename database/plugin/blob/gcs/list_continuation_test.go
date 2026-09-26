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

package gcs

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
)

// TestStreamIteratorDefersListing pins that neither Rewind nor Seek contacts
// GCS. NewIterator rewinds the iterator it returns and every caller in
// database/ seeks straight afterwards, so a rewind that listed eagerly fetched
// a page from the head of the prefix and threw it away. database's block
// iterator reopens the iterator every batch of block keys, so it paid one such
// page per batch.
//
// A nil bucket is the proof: reaching GCS through it panics, so returning from
// Rewind and Seek is evidence that no listing was opened.
func TestStreamIteratorDefersListing(t *testing.T) {
	store := &BlobStoreGCS{}
	it := &gcsStreamIterator{
		store:  store,
		prefix: []byte(types.BlockBlobKeyPrefix),
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Rewind opened a listing: %v", r)
			}
		}()
		it.Rewind()
	}()
	if it.iter != nil {
		t.Fatal("Rewind opened a listing")
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Seek opened a listing: %v", r)
			}
		}()
		it.Seek([]byte("bp-somewhere"))
	}()
	if it.iter != nil {
		t.Fatal("Seek opened a listing")
	}
	if it.err != nil {
		t.Fatalf("Seek: %v", it.err)
	}
}

// TestStreamIteratorCloseStopsListing pins that a closed iterator stays closed.
// The listing is deferred to the first read, so Valid, Err, and Next after
// Close must not open one. A nil bucket panics if they do.
func TestStreamIteratorCloseStopsListing(t *testing.T) {
	store := &BlobStoreGCS{}
	it := &gcsStreamIterator{
		store:  store,
		prefix: []byte(types.BlockBlobKeyPrefix),
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Rewind opened a listing: %v", r)
			}
		}()
		it.Rewind()
	}()
	it.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("reading a closed iterator opened a listing: %v", r)
		}
	}()
	if it.Valid() {
		t.Fatal("closed iterator reports a valid position")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("closed iterator: %v", err)
	}
	it.Next()
}

func TestStreamIteratorSeekAfterCloseRearmsListing(t *testing.T) {
	it := &gcsStreamIterator{}
	it.Close()
	it.Seek([]byte("bp-resume"))
	if it.closed || it.started || string(it.pendingStart) != "bp-resume" {
		t.Fatalf("Seek after Close did not rearm listing: %+v", it)
	}
}

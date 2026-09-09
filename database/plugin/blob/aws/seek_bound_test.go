// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//go:build dingo_extra_plugins

package aws

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeS3List serves just enough ListObjectsV2 to drive s3StreamIterator, and
// records the start-after parameter of every request it answers.
type fakeS3List struct {
	mu         sync.Mutex
	keys       []string // full object keys, sorted
	startAfter []string // one entry per ListObjectsV2 request
	returned   int      // total keys returned across all requests
}

func (f *fakeS3List) handler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		after = tok
	}
	f.mu.Lock()
	f.startAfter = append(f.startAfter, q.Get("start-after"))
	f.mu.Unlock()

	var out []string
	for _, k := range f.keys {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if after != "" && k <= after {
			continue
		}
		out = append(out, k)
	}
	const page = 1000
	truncated := false
	if len(out) > page {
		out = out[:page]
		truncated = true
	}
	f.mu.Lock()
	f.returned += len(out)
	f.mu.Unlock()

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, "<IsTruncated>%t</IsTruncated>", truncated)
	for _, k := range out {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>1</Size></Contents>", k)
	}
	if truncated && len(out) > 0 {
		fmt.Fprintf(
			&b,
			"<NextContinuationToken>%s</NextContinuationToken>",
			out[len(out)-1],
		)
	}
	b.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(b.String()))
}

// TestS3SeekBoundsListServerSide pins that seeking a forward iterator asks the
// server to start near the seek key instead of listing the whole prefix and
// discarding keys client-side.
//
// bark's ArchiveService is unauthenticated, and height-only block references
// drive a binary search whose every probe seeks into the full "bi" prefix. With
// no server-side bound each probe lists the prefix from the start, so one
// anonymous request costs order N keys per probe.
func TestS3SeekBoundsListServerSide(t *testing.T) {
	const total = 20000
	fake := &fakeS3List{}
	for i := range total {
		key := fmt.Sprintf("bi%06d", i)
		fake.keys = append(fake.keys, hex.EncodeToString([]byte(key)))
	}
	sort.Strings(fake.keys)

	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	store, err := NewWithOptions(
		WithBucket("test-bucket"),
		WithEndpoint(srv.URL),
		WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// Start() would need a real bucket; the iterator only needs the client.
	store.client = s3.New(s3.Options{
		BaseEndpoint: aws.String(srv.URL),
		Region:       "us-east-1",
		UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(
			"test", "test", "",
		),
	})

	seek := []byte(fmt.Sprintf("bi%06d", total/2))
	it := &s3StreamIterator{store: store, prefix: []byte("bi")}
	it.reset(seek)
	if it.err != nil {
		t.Fatalf("seek: %v", it.err)
	}
	if !it.valid {
		t.Fatal("seek landed on no key")
	}
	if got := it.key; got != string(seek) {
		t.Fatalf("seek key = %q, want %q", got, string(seek))
	}

	fake.mu.Lock()
	reqs := append([]string(nil), fake.startAfter...)
	returned := fake.returned
	fake.mu.Unlock()

	t.Logf(
		"seek to mid-point of %d keys: %d ListObjectsV2 request(s), "+
			"%d key(s) returned",
		total,
		len(reqs),
		returned,
	)

	bounded := 0
	for _, s := range reqs {
		if s != "" {
			bounded++
		}
	}
	if bounded == 0 {
		t.Fatalf(
			"seek to the mid-point issued %d ListObjectsV2 request(s) and "+
				"returned %d key(s), none carrying start-after: the whole "+
				"prefix is listed and discarded client-side",
			len(reqs),
			returned,
		)
	}
	// The bound is a strict prefix of the seek key, so a handful of keys just
	// below it can still come back -- but not half the bucket.
	if returned > total/10 {
		t.Fatalf(
			"seek returned %d of %d keys; the server-side bound is not "+
				"restricting the listing",
			returned,
			total,
		)
	}
}

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

// Package fakecloud is an in-memory object store that answers the subset of the
// S3 REST API and the GCS JSON API that Dingo's cloud blob stores and snapshot
// destinations use. It is a test double: one bucket map serves both providers
// through an http.RoundTripper, so an SDK client built with it as its transport
// never leaves the process.
//
// Hooks let a test observe the store between requests and make a request fail
// after it took effect, which is how a cloud commit's partial-visibility and
// uncertain-outcome behavior is driven deterministically.
package fakecloud

import (
	"bufio"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrInjected is a ready-made error for hooks to return.
var ErrInjected = errors.New("fakecloud: injected failure")

// Op describes one request that reaches the store.
type Op struct {
	// Method is the HTTP method as sent by the SDK.
	Method string
	// Bucket and Key identify the object; Key is empty for list requests.
	Bucket string
	Key    string
	// Mutates reports whether the request writes or deletes an object.
	Mutates bool
}

// Hooks intercept requests. A nil hook is skipped. Hooks run without the store
// lock held, so they may read the store or issue further requests.
type Hooks struct {
	// Before runs ahead of the request. A non-nil error fails the request
	// without applying it.
	Before func(Op) error
	// After runs once a mutation has been applied and before the response is
	// sent. A non-nil error answers 500 although the mutation took effect.
	After func(Op) error
}

// Store holds the buckets.
type Store struct {
	mu      sync.Mutex
	buckets map[string]map[string][]byte
	hooks   Hooks
	// requests counts requests per "METHOD kind" for tests that bound work.
	requests map[string]int
	// listed counts the keys returned by list requests.
	listed int
}

// New returns an empty store.
func New() *Store {
	return &Store{
		buckets:  map[string]map[string][]byte{},
		requests: map[string]int{},
	}
}

// SetHooks installs hooks. Call it before issuing requests concurrently.
func (s *Store) SetHooks(h Hooks) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks = h
}

// Put stores an object directly, bypassing hooks.
func (s *Store) Put(bucket, key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putLocked(bucket, key, data)
}

// Get returns a copy of an object and whether it exists.
func (s *Store) Get(bucket, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.buckets[bucket][key]
	return bytes.Clone(data), ok
}

// Delete removes an object directly, bypassing hooks.
func (s *Store) Delete(bucket, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.buckets[bucket], key)
}

// Keys returns the sorted keys of bucket that start with prefix.
func (s *Store) Keys(bucket, prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keysLocked(bucket, prefix)
}

// Requests returns how many requests of the given kind were served. Kinds are
// "GET object", "HEAD object", "PUT object", "DELETE object" and "LIST".
func (s *Store) Requests(kind string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[kind]
}

// Listed returns the total number of keys list requests have returned.
func (s *Store) Listed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listed
}

func (s *Store) putLocked(bucket, key string, data []byte) {
	if s.buckets[bucket] == nil {
		s.buckets[bucket] = map[string][]byte{}
	}
	s.buckets[bucket][key] = bytes.Clone(data)
}

func (s *Store) keysLocked(bucket, prefix string) []string {
	var keys []string
	for k := range s.buckets[bucket] {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// RoundTrip implements http.RoundTripper. Requests to the GCS API host
// (storage.googleapis.com) are answered as GCS; every other host is answered as
// path-style S3.
func (s *Store) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := &recorder{header: http.Header{}, code: http.StatusOK}
	if req.URL.Hostname() == "storage.googleapis.com" {
		s.serveGCS(rec, req)
	} else {
		s.serveS3(rec, req)
	}
	if req.Body != nil {
		_ = req.Body.Close()
	}
	return &http.Response{
		StatusCode: rec.code,
		Status: fmt.Sprintf(
			"%d %s",
			rec.code,
			http.StatusText(rec.code),
		),
		Header:        rec.header,
		Body:          io.NopCloser(bytes.NewReader(rec.body.Bytes())),
		ContentLength: int64(rec.body.Len()),
		Request:       req,
	}, nil
}

type recorder struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (r *recorder) status(code int) { r.code = code }

func (r *recorder) write(b []byte) { r.body.Write(b) }

func (s *Store) count(kind string) {
	s.mu.Lock()
	s.requests[kind]++
	s.mu.Unlock()
}

// before and after run the hooks; they report whether the request must fail.
func (s *Store) before(op Op) error {
	s.mu.Lock()
	h := s.hooks.Before
	s.mu.Unlock()
	if h == nil {
		return nil
	}
	return h(op)
}

func (s *Store) after(op Op) error {
	s.mu.Lock()
	h := s.hooks.After
	s.mu.Unlock()
	if h == nil {
		return nil
	}
	return h(op)
}

// apply runs a mutation between the hooks and reports the injected failure, if
// any. do runs only when Before allows it.
func (s *Store) apply(op Op, do func()) error {
	if err := s.before(op); err != nil {
		return err
	}
	do()
	return s.after(op)
}

func internalError(rec *recorder, err error) {
	rec.status(http.StatusInternalServerError)
	rec.write([]byte(err.Error()))
}

// ---- S3 ----

type s3ListResult struct {
	XMLName               xml.Name     `xml:"ListBucketResult"`
	Xmlns                 string       `xml:"xmlns,attr"`
	Name                  string       `xml:"Name"`
	KeyCount              int          `xml:"KeyCount"`
	IsTruncated           bool         `xml:"IsTruncated"`
	NextContinuationToken string       `xml:"NextContinuationToken,omitempty"`
	Contents              []s3ListItem `xml:"Contents"`
}

type s3ListItem struct {
	Key  string `xml:"Key"`
	Size int    `xml:"Size"`
}

func s3Error(rec *recorder, code int, name string) {
	rec.status(code)
	rec.header.Set("Content-Type", "application/xml")
	rec.write(fmt.Appendf(
		nil,
		`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`,
		name,
		name,
	))
}

func (s *Store) serveS3(rec *recorder, req *http.Request) {
	path := strings.TrimPrefix(req.URL.EscapedPath(), "/")
	bucket, rawKey, _ := strings.Cut(path, "/")
	bucket, _ = url.PathUnescape(bucket)
	key, _ := url.PathUnescape(rawKey)
	q := req.URL.Query()

	if key == "" {
		switch {
		case req.Method == http.MethodGet:
			s.listS3(rec, bucket, q)
		case req.Method == http.MethodPost && q.Has("delete"):
			s.deleteManyS3(rec, req, bucket)
		default:
			s3Error(rec, http.StatusBadRequest, "InvalidRequest")
		}
		return
	}

	switch req.Method {
	case http.MethodGet, http.MethodHead:
		s.count(req.Method + " object")
		op := Op{Method: req.Method, Bucket: bucket, Key: key}
		if err := s.before(op); err != nil {
			internalError(rec, err)
			return
		}
		data, ok := s.Get(bucket, key)
		if !ok {
			if req.Method == http.MethodHead {
				rec.status(http.StatusNotFound)
				return
			}
			s3Error(rec, http.StatusNotFound, "NoSuchKey")
			return
		}
		rec.header.Set("Content-Length", strconv.Itoa(len(data)))
		rec.header.Set("ETag", `"fake"`)
		if req.Method == http.MethodGet {
			rec.write(data)
		}
	case http.MethodPut:
		s.count("PUT object")
		body, err := readS3Body(req)
		if err != nil {
			internalError(rec, err)
			return
		}
		op := Op{Method: req.Method, Bucket: bucket, Key: key, Mutates: true}
		if err := s.apply(op, func() { s.Put(bucket, key, body) }); err != nil {
			internalError(rec, err)
			return
		}
		rec.header.Set("ETag", `"fake"`)
	case http.MethodDelete:
		s.count("DELETE object")
		op := Op{Method: req.Method, Bucket: bucket, Key: key, Mutates: true}
		if err := s.apply(op, func() { s.Delete(bucket, key) }); err != nil {
			internalError(rec, err)
			return
		}
		rec.status(http.StatusNoContent)
	default:
		s3Error(rec, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

// readS3Body returns the request payload, decoding the aws-chunked framing the
// SDK uses when it sends a checksum trailer.
func readS3Body(req *http.Request) ([]byte, error) {
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(req.Header.Get("Content-Encoding"), "aws-chunked") &&
		req.Header.Get("X-Amz-Decoded-Content-Length") == "" {
		return raw, nil
	}
	var out bytes.Buffer
	br := bufio.NewReader(bytes.NewReader(raw))
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("fakecloud: aws-chunked header: %w", err)
		}
		sizeText, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		size, err := strconv.ParseInt(sizeText, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("fakecloud: aws-chunked size: %w", err)
		}
		if size == 0 {
			return out.Bytes(), nil
		}
		if _, err := io.CopyN(&out, br, size); err != nil {
			return nil, err
		}
		if _, err := br.Discard(2); err != nil {
			return nil, err
		}
	}
}

func (s *Store) listS3(rec *recorder, bucket string, q url.Values) {
	s.count("LIST")
	if err := s.before(Op{Method: http.MethodGet, Bucket: bucket}); err != nil {
		internalError(rec, err)
		return
	}
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		after = tok
	}
	limit := 1000
	if n, err := strconv.Atoi(q.Get("max-keys")); err == nil && n > 0 &&
		n < limit {
		limit = n
	}
	page := make([]string, 0, limit)
	truncated := false
	for _, k := range s.Keys(bucket, q.Get("prefix")) {
		if after != "" && k <= after {
			continue
		}
		if len(page) == limit {
			truncated = true
			break
		}
		page = append(page, k)
	}
	res := s3ListResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		KeyCount:    len(page),
		IsTruncated: truncated,
	}
	if truncated {
		res.NextContinuationToken = page[len(page)-1]
	}
	s.mu.Lock()
	s.listed += len(page)
	for _, k := range page {
		res.Contents = append(res.Contents, s3ListItem{
			Key: k, Size: len(s.buckets[bucket][k]),
		})
	}
	s.mu.Unlock()
	body, _ := xml.Marshal(res)
	rec.header.Set("Content-Type", "application/xml")
	rec.write([]byte(xml.Header))
	rec.write(body)
}

func (s *Store) deleteManyS3(rec *recorder, req *http.Request, bucket string) {
	var in struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		s3Error(rec, http.StatusBadRequest, "MalformedXML")
		return
	}
	var out strings.Builder
	out.WriteString(
		`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
	)
	for _, o := range in.Objects {
		s.count("DELETE object")
		op := Op{
			Method:  http.MethodDelete,
			Bucket:  bucket,
			Key:     o.Key,
			Mutates: true,
		}
		if err := s.apply(op, func() { s.Delete(bucket, o.Key) }); err != nil {
			internalError(rec, err)
			return
		}
		fmt.Fprintf(&out, "<Deleted><Key>%s</Key></Deleted>", o.Key)
	}
	out.WriteString("</DeleteResult>")
	rec.header.Set("Content-Type", "application/xml")
	rec.write([]byte(out.String()))
}

// ---- GCS ----

type gcsObject struct {
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Bucket     string `json:"bucket"`
	Size       string `json:"size"`
	Generation string `json:"generation"`
}

func gcsResource(bucket, key string, size int) gcsObject {
	return gcsObject{
		Kind:       "storage#object",
		Name:       key,
		Bucket:     bucket,
		Size:       strconv.Itoa(size),
		Generation: "1",
	}
}

func gcsError(rec *recorder, code int, msg string) {
	rec.status(code)
	rec.header.Set("Content-Type", "application/json")
	body, err := json.Marshal(
		gcsErrorBody{Error: gcsErrorDetail{Code: code, Message: msg}},
	)
	if err != nil {
		panic(err)
	}
	rec.write(body)
}

type gcsErrorBody struct {
	Error gcsErrorDetail `json:"error"`
}

type gcsErrorDetail struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type gcsList struct {
	Kind          string      `json:"kind"`
	Items         []gcsObject `json:"items"`
	NextPageToken string      `json:"nextPageToken"`
}

func gcsJSON[T any](rec *recorder, v T) {
	rec.header.Set("Content-Type", "application/json")
	body, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	rec.write(body)
}

func (s *Store) serveGCS(rec *recorder, req *http.Request) {
	path := req.URL.EscapedPath()
	q := req.URL.Query()
	switch {
	case strings.HasPrefix(path, "/upload/storage/v1/b/"):
		s.uploadGCS(rec, req, strings.TrimPrefix(path, "/upload/storage/v1/b/"))
	case strings.HasPrefix(path, "/storage/v1/b/"):
		bucket, rest, _ := strings.Cut(
			strings.TrimPrefix(path, "/storage/v1/b/"), "/o",
		)
		bucket, _ = url.PathUnescape(bucket)
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			s.listGCS(rec, bucket, q)
			return
		}
		key, _ := url.PathUnescape(rest)
		s.objectGCS(rec, req, bucket, key, q)
	default:
		// Media download: /<bucket>/<object>.
		bucket, rawKey, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
		bucket, _ = url.PathUnescape(bucket)
		key, _ := url.PathUnescape(rawKey)
		s.downloadGCS(rec, bucket, key)
	}
}

func (s *Store) objectGCS(
	rec *recorder, req *http.Request, bucket, key string, q url.Values,
) {
	switch req.Method {
	case http.MethodGet:
		s.count("HEAD object")
		if err := s.before(Op{Method: http.MethodHead, Bucket: bucket, Key: key}); err != nil {
			internalError(rec, err)
			return
		}
		data, ok := s.Get(bucket, key)
		if !ok {
			gcsError(rec, http.StatusNotFound, "No such object")
			return
		}
		if q.Get("alt") == "media" {
			rec.write(data)
			return
		}
		gcsJSON(rec, gcsResource(bucket, key, len(data)))
	case http.MethodDelete:
		s.count("DELETE object")
		op := Op{Method: req.Method, Bucket: bucket, Key: key, Mutates: true}
		if _, ok := s.Get(bucket, key); !ok {
			gcsError(rec, http.StatusNotFound, "No such object")
			return
		}
		if err := s.apply(op, func() { s.Delete(bucket, key) }); err != nil {
			internalError(rec, err)
			return
		}
		rec.status(http.StatusNoContent)
	default:
		gcsError(rec, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Store) downloadGCS(rec *recorder, bucket, key string) {
	s.count("GET object")
	if err := s.before(Op{Method: http.MethodGet, Bucket: bucket, Key: key}); err != nil {
		internalError(rec, err)
		return
	}
	data, ok := s.Get(bucket, key)
	if !ok {
		gcsError(rec, http.StatusNotFound, "No such object")
		return
	}
	rec.header.Set("Content-Length", strconv.Itoa(len(data)))
	rec.header.Set("X-Goog-Generation", "1")
	rec.write(data)
}

func (s *Store) listGCS(rec *recorder, bucket string, q url.Values) {
	s.count("LIST")
	if err := s.before(Op{Method: http.MethodGet, Bucket: bucket}); err != nil {
		internalError(rec, err)
		return
	}
	start := q.Get("pageToken")
	if off := q.Get("startOffset"); off != "" && off > start {
		start = off
	}
	limit := 1000
	if n, err := strconv.Atoi(q.Get("maxResults")); err == nil && n > 0 &&
		n < limit {
		limit = n
	}
	var items []gcsObject
	next := ""
	s.mu.Lock()
	for _, k := range s.keysLocked(bucket, q.Get("prefix")) {
		if start != "" && k < start {
			continue
		}
		if len(items) == limit {
			next = k
			break
		}
		items = append(items, gcsResource(bucket, k, len(s.buckets[bucket][k])))
	}
	s.listed += len(items)
	s.mu.Unlock()
	gcsJSON(
		rec,
		gcsList{Kind: "storage#objects", Items: items, NextPageToken: next},
	)
}

func (s *Store) uploadGCS(rec *recorder, req *http.Request, rest string) {
	bucket, _, _ := strings.Cut(rest, "/")
	bucket, _ = url.PathUnescape(bucket)
	q := req.URL.Query()
	var name string
	var data []byte
	switch q.Get("uploadType") {
	case "multipart":
		mt, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if err != nil || !strings.HasPrefix(mt, "multipart/") {
			gcsError(rec, http.StatusBadRequest, "not multipart")
			return
		}
		mr := multipart.NewReader(req.Body, params["boundary"])
		meta, err := mr.NextPart()
		if err != nil {
			gcsError(rec, http.StatusBadRequest, "no metadata part")
			return
		}
		var m struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(meta).Decode(&m); err != nil {
			gcsError(rec, http.StatusBadRequest, "bad metadata")
			return
		}
		name = m.Name
		media, err := mr.NextPart()
		if err != nil {
			gcsError(rec, http.StatusBadRequest, "no media part")
			return
		}
		if data, err = io.ReadAll(media); err != nil {
			internalError(rec, err)
			return
		}
	case "media":
		name = q.Get("name")
		var err error
		if data, err = io.ReadAll(req.Body); err != nil {
			internalError(rec, err)
			return
		}
	default:
		gcsError(rec, http.StatusBadRequest, "unsupported uploadType")
		return
	}
	s.count("PUT object")
	op := Op{Method: http.MethodPut, Bucket: bucket, Key: name, Mutates: true}
	if err := s.apply(op, func() { s.Put(bucket, name, data) }); err != nil {
		internalError(rec, err)
		return
	}
	gcsJSON(rec, gcsResource(bucket, name, len(data)))
}

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

package mcp

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	maxMCPSessions          = 128
	maxMCPSessionsPerClient = 8
	mcpSessionIdleTimeout   = 5 * time.Minute
	mcpSessionMaxLifetime   = time.Hour
	mcpSessionIDHeader      = "Mcp-Session-Id"
)

type sessionAdmission struct {
	mu           sync.Mutex
	closed       bool
	maxSessions  int
	maxPerClient int
	idleTimeout  time.Duration
	maxLifetime  time.Duration
	leases       map[*sessionLease]struct{}
	byID         map[string]*sessionLease
	byClient     map[string]int
}

type sessionLease struct {
	admission *sessionAdmission
	client    string
	created   time.Time
	lastSeen  time.Time
	id        string
	close     func()
	timer     *time.Timer
	released  bool
}

func newSessionAdmission() *sessionAdmission {
	return &sessionAdmission{
		maxSessions:  maxMCPSessions,
		maxPerClient: maxMCPSessionsPerClient,
		idleTimeout:  mcpSessionIdleTimeout,
		maxLifetime:  mcpSessionMaxLifetime,
		leases:       make(map[*sessionLease]struct{}),
		byID:         make(map[string]*sessionLease),
		byClient:     make(map[string]int),
	}
}

func (a *sessionAdmission) acquire(
	client string,
	closeSession func(),
) (*sessionLease, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || len(a.leases) >= a.maxSessions ||
		a.byClient[client] >= a.maxPerClient {
		return nil, false
	}
	now := time.Now()
	lease := &sessionLease{
		admission: a,
		client:    client,
		created:   now,
		lastSeen:  now,
		close:     closeSession,
	}
	a.leases[lease] = struct{}{}
	a.byClient[client]++
	lease.timer = time.AfterFunc(a.nextExpiryLocked(lease, now), lease.expire)
	return lease, true
}

func (a *sessionAdmission) nextExpiryLocked(
	lease *sessionLease,
	now time.Time,
) time.Duration {
	idleRemaining := lease.lastSeen.Add(a.idleTimeout).Sub(now)
	lifetimeRemaining := lease.created.Add(a.maxLifetime).Sub(now)
	remaining := min(idleRemaining, lifetimeRemaining)
	return max(remaining, 0)
}

func (lease *sessionLease) bind(id string, closeSession func()) bool {
	a := lease.admission
	a.mu.Lock()
	defer a.mu.Unlock()
	if lease.released || a.closed || id == "" {
		return false
	}
	if existing := a.byID[id]; existing != nil && existing != lease {
		return false
	}
	lease.id = id
	lease.close = closeSession
	a.byID[id] = lease
	return true
}

func (a *sessionAdmission) touch(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	lease := a.byID[id]
	if lease == nil || lease.released {
		return
	}
	now := time.Now()
	if !now.Before(lease.created.Add(a.maxLifetime)) {
		return
	}
	lease.lastSeen = now
	lease.timer.Reset(a.nextExpiryLocked(lease, now))
}

func (lease *sessionLease) bound() bool {
	a := lease.admission
	a.mu.Lock()
	defer a.mu.Unlock()
	return !lease.released && lease.id != ""
}

func (a *sessionAdmission) releaseID(id string) {
	a.mu.Lock()
	lease := a.byID[id]
	if lease != nil {
		a.releaseLocked(lease)
	}
	a.mu.Unlock()
}

func (lease *sessionLease) release() {
	a := lease.admission
	a.mu.Lock()
	a.releaseLocked(lease)
	a.mu.Unlock()
}

func (lease *sessionLease) expire() {
	a := lease.admission
	a.mu.Lock()
	if lease.released {
		a.mu.Unlock()
		return
	}
	now := time.Now()
	remaining := a.nextExpiryLocked(lease, now)
	if remaining > 0 {
		lease.timer.Reset(remaining)
		a.mu.Unlock()
		return
	}
	closeSession := lease.close
	a.releaseLocked(lease)
	a.mu.Unlock()
	if closeSession != nil {
		closeSession()
	}
}

func (a *sessionAdmission) releaseLocked(lease *sessionLease) {
	if lease == nil || lease.released {
		return
	}
	lease.released = true
	if lease.timer != nil {
		lease.timer.Stop()
	}
	delete(a.leases, lease)
	if lease.id != "" && a.byID[lease.id] == lease {
		delete(a.byID, lease.id)
	}
	a.byClient[lease.client]--
	if a.byClient[lease.client] == 0 {
		delete(a.byClient, lease.client)
	}
}

func (a *sessionAdmission) Close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	closeSessions := make([]func(), 0, len(a.leases))
	for lease := range a.leases {
		if lease.close != nil {
			closeSessions = append(closeSessions, lease.close)
		}
		a.releaseLocked(lease)
	}
	a.mu.Unlock()
	for _, closeSession := range closeSessions {
		closeSession()
	}
}

type admittedStreamableHandler struct {
	next      http.Handler
	admission *sessionAdmission
}

func (h *admittedStreamableHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	sessionID := r.Header.Get(mcpSessionIDHeader)
	if sessionID != "" {
		key := "streamable:" + sessionID
		h.admission.touch(key)
		h.next.ServeHTTP(w, r)
		if r.Method == http.MethodDelete {
			h.admission.releaseID(key)
		}
		return
	}
	if r.Method != http.MethodPost {
		h.next.ServeHTTP(w, r)
		return
	}

	lease, ok := h.admission.acquire(requestClientKey(r), nil)
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many MCP sessions", http.StatusTooManyRequests)
		return
	}
	wrapped := &sessionIDResponseWriter{
		ResponseWriter: w,
		onSessionID: func(id string) { //nolint:contextcheck // Session cleanup must outlive the request that created it.
			key := "streamable:" + id
			if !lease.bind(key, func() {
				closeStreamableSession(h.next, id)
			}) {
				closeStreamableSession(h.next, id)
			}
		},
	}
	h.next.ServeHTTP(wrapped, r)
	if !lease.bound() {
		lease.release()
	}
}

func closeStreamableSession(handler http.Handler, sessionID string) {
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodDelete,
		"http://mcp.internal/mcp",
		nil,
	)
	if err != nil {
		return
	}
	req.Header.Set(mcpSessionIDHeader, sessionID)
	handler.ServeHTTP(newDiscardResponseWriter(), req)
}

type admittedSSEHandler struct {
	next      http.Handler
	admission *sessionAdmission
}

func (h *admittedSSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if sessionID := r.URL.Query().Get("sessionid"); sessionID != "" {
			h.admission.touch("sse:" + sessionID)
		}
		h.next.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodGet {
		h.next.ServeHTTP(w, r)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	lease, ok := h.admission.acquire(requestClientKey(r), cancel)
	if !ok {
		cancel()
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many MCP sessions", http.StatusTooManyRequests)
		return
	}
	wrapped := &sseSessionResponseWriter{
		ResponseWriter: w,
		onSessionID: func(id string) {
			if !lease.bind("sse:"+id, cancel) {
				cancel()
			}
		},
	}
	h.next.ServeHTTP(wrapped, r.WithContext(ctx))
	lease.release()
	cancel()
}

func requestClientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

type sessionIDResponseWriter struct {
	http.ResponseWriter
	onSessionID func(string)
	once        sync.Once
}

func (w *sessionIDResponseWriter) WriteHeader(statusCode int) {
	w.capture()
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *sessionIDResponseWriter) Write(data []byte) (int, error) {
	w.capture()
	return w.ResponseWriter.Write(data)
}

func (w *sessionIDResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *sessionIDResponseWriter) capture() {
	sessionID := w.Header().Get(mcpSessionIDHeader)
	if sessionID != "" {
		w.once.Do(func() { w.onSessionID(sessionID) })
	}
}

type sseSessionResponseWriter struct {
	http.ResponseWriter
	onSessionID func(string)
	once        sync.Once
	prefix      []byte
	done        bool
}

func (w *sseSessionResponseWriter) Write(data []byte) (int, error) {
	if !w.done && len(w.prefix) < 4096 {
		remaining := 4096 - len(w.prefix)
		w.prefix = append(w.prefix, data[:min(len(data), remaining)]...)
		if sessionID := sseSessionID(w.prefix); sessionID != "" {
			w.once.Do(func() { w.onSessionID(sessionID) })
			w.prefix = nil
			w.done = true
		}
	}
	return w.ResponseWriter.Write(data)
}

func (w *sseSessionResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func sseSessionID(data []byte) string {
	const prefix = "data: "
	start := bytes.Index(data, []byte(prefix))
	if start < 0 {
		return ""
	}
	start += len(prefix)
	end := bytes.IndexByte(data[start:], '\n')
	if end < 0 {
		return ""
	}
	endpoint, err := url.Parse(strings.TrimSpace(string(data[start : start+end])))
	if err != nil {
		return ""
	}
	return endpoint.Query().Get("sessionid")
}

type discardResponseWriter struct {
	header http.Header
}

func newDiscardResponseWriter() *discardResponseWriter {
	return &discardResponseWriter{header: make(http.Header)}
}

func (w *discardResponseWriter) Header() http.Header {
	return w.header
}

func (*discardResponseWriter) Write(data []byte) (int, error) {
	return len(data), nil
}

func (*discardResponseWriter) WriteHeader(int) {}

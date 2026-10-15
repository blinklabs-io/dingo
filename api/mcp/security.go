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
	"container/list"
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	clientLimiterMaxEntries = 4096
	clientLimiterIdleTTL    = 30 * time.Minute
)

type clientLimiterEntry struct {
	key      string
	limiter  *rate.Limiter
	lastSeen time.Time
	element  *list.Element
}

// clientLimiter tracks a bounded set of rate limiters for client IP addresses.
type clientLimiter struct {
	mu         sync.Mutex
	entries    map[string]*clientLimiterEntry
	order      list.List
	rate       rate.Limit
	burst      int
	maxEntries int
	idleTTL    time.Duration
	timer      *time.Timer
}

func newClientLimiter(r rate.Limit, burst int) *clientLimiter {
	return newClientLimiterWithBounds(
		r,
		burst,
		clientLimiterMaxEntries,
		clientLimiterIdleTTL,
	)
}

func newClientLimiterWithBounds(
	r rate.Limit,
	burst int,
	maxEntries int,
	idleTTL time.Duration,
) *clientLimiter {
	return &clientLimiter{
		entries:    make(map[string]*clientLimiterEntry),
		rate:       r,
		burst:      burst,
		maxEntries: maxEntries,
		idleTTL:    idleTTL,
	}
}

func (cl *clientLimiter) getLimiter(ip string) *rate.Limiter {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	now := time.Now()
	cl.evictExpiredLocked(now)
	if entry, ok := cl.entries[ip]; ok {
		entry.lastSeen = now
		cl.order.MoveToBack(entry.element)
		cl.scheduleExpiryLocked(now)
		return entry.limiter
	}

	if len(cl.entries) >= cl.maxEntries {
		cl.removeOldestLocked()
	}
	entry := &clientLimiterEntry{
		key:      ip,
		limiter:  rate.NewLimiter(cl.rate, cl.burst),
		lastSeen: now,
	}
	entry.element = cl.order.PushBack(entry)
	cl.entries[ip] = entry
	cl.scheduleExpiryLocked(now)
	return entry.limiter
}

func (cl *clientLimiter) removeOldestLocked() {
	oldest := cl.order.Front()
	if oldest == nil {
		return
	}
	entry, ok := oldest.Value.(*clientLimiterEntry)
	if ok && entry != nil {
		delete(cl.entries, entry.key)
	}
	cl.order.Remove(oldest)
}

func (cl *clientLimiter) evictExpiredLocked(now time.Time) {
	for {
		oldest := cl.order.Front()
		if oldest == nil {
			return
		}
		entry, ok := oldest.Value.(*clientLimiterEntry)
		if !ok || entry == nil {
			cl.removeOldestLocked()
			continue
		}
		if now.Before(entry.lastSeen.Add(cl.idleTTL)) {
			return
		}
		cl.removeOldestLocked()
	}
}

func (cl *clientLimiter) scheduleExpiryLocked(now time.Time) {
	if cl.timer != nil {
		return
	}
	for cl.order.Len() > 0 {
		oldest, ok := cl.order.Front().Value.(*clientLimiterEntry)
		if !ok || oldest == nil {
			cl.removeOldestLocked()
			continue
		}
		delay := oldest.lastSeen.Add(cl.idleTTL).Sub(now)
		if delay < 0 {
			delay = 0
		}
		cl.timer = time.AfterFunc(delay, cl.expireIdle)
		return
	}
}

func (cl *clientLimiter) expireIdle() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.timer = nil
	now := time.Now()
	cl.evictExpiredLocked(now)
	cl.scheduleExpiryLocked(now)
}

// SecurityMiddleware wraps an HTTP handler with optional authentication, rate limiting, and CORS.
func SecurityMiddleware(
	authToken string,
	reqPerSec float64,
	burst int,
	allowedOrigins []string,
	next http.Handler,
) http.Handler {
	var limiter *clientLimiter
	if reqPerSec > 0 {
		if burst <= 0 {
			burst = 10
		}
		limiter = newClientLimiter(rate.Limit(reqPerSec), burst)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		origins, present := r.Header["Origin"]
		if present {
			if len(origins) != 1 ||
				!trustedOrigin(origins[0], r, allowedOrigins) {
				http.Error(w, "untrusted Origin", http.StatusForbidden)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origins[0])
			w.Header().
				Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().
				Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, Accept, Mcp-Session-Id, Mcp-Protocol-Version, Last-Event-ID")
			w.Header().Set("Access-Control-Expose-Headers", "Mcp-Session-Id")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Health endpoint bypasses auth and rate limiting
		if r.URL.Path == "/healthz" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		// Rate limiting check
		if limiter != nil {
			clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				clientIP = r.RemoteAddr
			}

			if !limiter.getLimiter(clientIP).Allow() {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "too many requests: rate limit exceeded",
				})
				return
			}
		}

		// Authentication check
		if authToken != "" {
			var token string
			authHeader := r.Header.Get("Authorization")
			if after, ok := strings.CutPrefix(authHeader, "Bearer "); ok {
				token = after
			} else if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
				token = apiKey
			}

			// Constant time comparison to prevent timing attacks
			if subtle.ConstantTimeCompare(
				[]byte(token),
				[]byte(authToken),
			) != 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "unauthorized: missing or invalid authentication token",
				})
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func trustedOrigin(origin string, r *http.Request, allowed []string) bool {
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Host == "" ||
		u.User != nil ||
		u.Path != "" ||
		u.RawQuery != "" ||
		u.ForceQuery ||
		u.Fragment != "" ||
		u.String() != origin {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	// Request Host alone is not a trust anchor on LAN-bound listeners.
	loopback := strings.EqualFold(host, "localhost") ||
		(ip != nil && ip.IsLoopback())
	if loopback && origin == scheme+"://"+r.Host {
		return true
	}
	for _, candidate := range allowed {
		// The shared API wildcard must never expose local MCP to arbitrary websites.
		if candidate != "*" && candidate == origin {
			return true
		}
	}
	return false
}

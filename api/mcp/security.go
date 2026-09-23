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
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// clientLimiter tracks rate limiters for client IP addresses.
type clientLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	lastSeen map[string]time.Time
	rate     rate.Limit
	burst    int
}

func newClientLimiter(r rate.Limit, burst int) *clientLimiter {
	cl := &clientLimiter{
		limiters: make(map[string]*rate.Limiter),
		lastSeen: make(map[string]time.Time),
		rate:     r,
		burst:    burst,
	}

	// Periodic cleanup of stale client limiters
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			cl.mu.Lock()
			threshold := time.Now().Add(-30 * time.Minute)
			for ip, last := range cl.lastSeen {
				if last.Before(threshold) {
					delete(cl.limiters, ip)
					delete(cl.lastSeen, ip)
				}
			}
			cl.mu.Unlock()
		}
	}()

	return cl
}

func (cl *clientLimiter) getLimiter(ip string) *rate.Limiter {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	limiter, exists := cl.limiters[ip]
	if !exists {
		limiter = rate.NewLimiter(cl.rate, cl.burst)
		cl.limiters[ip] = limiter
	}
	cl.lastSeen[ip] = time.Now()
	return limiter
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
		// Handle CORS headers
		origin := r.Header.Get("Origin")
		allowOrigin := "*"
		if len(allowedOrigins) > 0 {
			allowOrigin = ""
			for _, allowed := range allowedOrigins {
				if allowed == "*" || allowed == origin {
					allowOrigin = origin
					break
				}
			}
		}
		if allowOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().
				Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, Accept, Mcp-Session-Id")
			w.Header().Set("Access-Control-Expose-Headers", "Mcp-Session-Id")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Health endpoint bypasses auth and rate limiting
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		// Authentication check
		if authToken != "" {
			var token string
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				token = strings.TrimPrefix(authHeader, "Bearer ")
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

		next.ServeHTTP(w, r)
	})
}

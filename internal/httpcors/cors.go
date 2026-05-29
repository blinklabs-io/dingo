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

package httpcors

import (
	"net/http"
	"slices"
	"strings"
)

const (
	allowMethods = "GET, HEAD, POST, OPTIONS"
	allowHeaders = "Accept, Authorization, Content-Type, " +
		"Connect-Protocol-Version, Connect-Timeout-Ms, Grpc-Timeout, " +
		"X-Blockfrost-Project-Id, X-Grpc-Web, X-Requested-With"
	exposeHeaders = "Grpc-Message, Grpc-Status, Grpc-Status-Details-Bin"
)

type Config struct {
	AllowedOrigins []string
}

// Handler wraps next with CORS response headers. An empty AllowedOrigins
// list disables CORS.
func Handler(next http.Handler, cfg Config) http.Handler {
	allowedOrigins := normalizeOrigins(cfg.AllowedOrigins)
	if len(allowedOrigins) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		h := w.Header()
		if origin != "" {
			h.Add("Vary", "Origin")
			h.Add("Vary", "Access-Control-Request-Method")
			h.Add("Vary", "Access-Control-Request-Headers")
		}
		allowedOrigin, ok := allowedOrigin(origin, allowedOrigins)
		if ok {
			h.Set("Access-Control-Allow-Origin", allowedOrigin)
			h.Set("Access-Control-Allow-Methods", allowMethods)
			h.Set("Access-Control-Allow-Headers", allowHeaders)
			if requestedHeaders := r.Header.Get(
				"Access-Control-Request-Headers",
			); requestedHeaders != "" {
				h.Set("Access-Control-Allow-Headers", requestedHeaders)
			}
			h.Set("Access-Control-Expose-Headers", exposeHeaders)
		}
		if r.Method == http.MethodOptions &&
			origin != "" &&
			r.Header.Get("Access-Control-Request-Method") != "" {
			if !ok {
				http.Error(w, "CORS origin not allowed", http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func normalizeOrigins(origins []string) []string {
	ret := make([]string, 0, len(origins))
	for _, origin := range origins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		ret = append(ret, origin)
	}
	return ret
}

func allowedOrigin(origin string, allowedOrigins []string) (string, bool) {
	if origin == "" {
		return "", false
	}
	if slices.Contains(allowedOrigins, "*") {
		return "*", true
	}
	if slices.Contains(allowedOrigins, origin) {
		return origin, true
	}
	return "", false
}

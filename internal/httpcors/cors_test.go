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
	"net/http/httptest"
	"testing"
)

func TestHandlerWildcardOrigin(t *testing.T) {
	handler := Handler(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		Config{AllowedOrigins: []string{"*"}},
	)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://mywallet.io")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("unexpected allow origin: %q", got)
	}
	if got := rec.Code; got != http.StatusOK {
		t.Fatalf("unexpected status: %d", got)
	}
}

func TestHandlerSpecificOriginPreflight(t *testing.T) {
	handler := Handler(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		Config{AllowedOrigins: []string{"https://mywallet.io"}},
	)
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://mywallet.io")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "x-dapp-session")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if got := rec.Code; got != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://mywallet.io" {
		t.Fatalf("unexpected allow origin: %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "x-dapp-session" {
		t.Fatalf("unexpected allow headers: %q", got)
	}
}

func TestHandlerRejectsDisallowedPreflight(t *testing.T) {
	handler := Handler(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		Config{AllowedOrigins: []string{"https://mywallet.io"}},
	)
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://other.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if got := rec.Code; got != http.StatusForbidden {
		t.Fatalf("unexpected status: %d", got)
	}
	vary := rec.Header().Values("Vary")
	if len(vary) != 3 {
		t.Fatalf("unexpected Vary headers: %v", vary)
	}
}

func TestHandlerEmptyConfigDisablesCORS(t *testing.T) {
	handler := Handler(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		Config{},
	)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://mywallet.io")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unexpected allow origin: %q", got)
	}
}

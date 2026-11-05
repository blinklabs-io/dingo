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

package blockfrost

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCORSExposesRetryAfterOnAdmissionRejection(t *testing.T) {
	t.Parallel()
	const allowedOrigin = "https://app.example"
	for _, origin := range []string{allowedOrigin, "https://other.example"} {
		t.Run(origin, func(t *testing.T) {
			b := New(BlockfrostConfig{
				CORSAllowedOrigins: []string{allowedOrigin},
			}, &mockNode{}, nil)
			b.chainSummaryAdmission = newRequestAdmission(1, 1)
			req := httptest.NewRequest(http.MethodGet, "/api/v0/network", nil)
			req.Header.Set("Origin", origin)
			release, ok := b.chainSummaryAdmission.acquire(
				t.Context(), requestClient(req),
			)
			require.True(t, ok)
			defer release()
			rec := httptest.NewRecorder()
			b.handler().ServeHTTP(rec, req)
			require.Equal(t, http.StatusTooManyRequests, rec.Code)
			require.Equal(t, "1", rec.Header().Get("Retry-After"))
			if origin != allowedOrigin {
				for name := range rec.Header() {
					require.False(t, strings.HasPrefix(name, "Access-Control-"), name)
				}
				return
			}
			require.Equal(t, allowedOrigin,
				rec.Header().Get("Access-Control-Allow-Origin"))
			exposed := false
			for _, value := range rec.Header().Values("Access-Control-Expose-Headers") {
				for name := range strings.SplitSeq(value, ",") {
					if strings.EqualFold(strings.TrimSpace(name), "Retry-After") {
						exposed = true
					}
				}
			}
			require.True(t, exposed, "browser must be able to read Retry-After")
		})
	}
}

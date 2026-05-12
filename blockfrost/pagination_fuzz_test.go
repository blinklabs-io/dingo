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
	"net/url"
	"testing"
)

func FuzzParsePagination(f *testing.F) {
	f.Add("")
	f.Add("count=0&page=0&order=ASC")
	f.Add("count=101&page=-1&order=desc")
	f.Add("count=abc&page=1&order=asc")

	f.Fuzz(func(t *testing.T, rawQuery string) {
		if len(rawQuery) > 16*1024 {
			t.Skip("query input is too large for fast fuzzing")
		}

		req := &http.Request{URL: &url.URL{RawQuery: rawQuery}}
		params, err := ParsePagination(req)
		if err != nil {
			return
		}
		if params.Count < 1 || params.Count > MaxPaginationCount {
			t.Fatalf("count = %d, want 1..%d", params.Count, MaxPaginationCount)
		}
		if params.Page < 1 {
			t.Fatalf("page = %d, want >= 1", params.Page)
		}
		if params.Order != PaginationOrderAsc && params.Order != PaginationOrderDesc {
			t.Fatalf("order = %q, want asc or desc", params.Order)
		}
	})
}

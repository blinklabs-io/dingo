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

package httpuseragent

import (
	"fmt"
	"net/http"
	"os"
	"runtime"

	"github.com/blinklabs-io/dingo/internal/version"
	"golang.org/x/net/http/httpguts"
)

// Default returns the user-agent for Dingo's outbound HTTP requests. A
// non-empty DINGO_HTTP_USER_AGENT that is a valid header value overrides it;
// otherwise it names Dingo's version and the platform on which it was built.
func Default() string {
	// An override net/http would refuse to send is ignored rather than
	// returned, since it would fail every request that carries it.
	if userAgent := os.Getenv("DINGO_HTTP_USER_AGENT"); userAgent != "" &&
		httpguts.ValidHeaderFieldValue(userAgent) {
		return userAgent
	}
	return fmt.Sprintf(
		"dingo/%s (%s/%s)",
		version.GetVersionString(),
		runtime.GOOS,
		runtime.GOARCH,
	)
}

// Set sets Default as the request's user-agent unless the request already
// carries a User-Agent header. A present but empty header is kept, because
// net/http then sends no User-Agent at all.
func Set(req *http.Request) {
	if req == nil {
		return
	}
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", Default())
	}
}

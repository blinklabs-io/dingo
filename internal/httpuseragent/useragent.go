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
)

// Default returns Dingo's default HTTP user-agent, including its version and
// the platform on which it was built.
func Default() string {
	if userAgent := os.Getenv("DINGO_HTTP_USER_AGENT"); userAgent != "" {
		return userAgent
	}
	return fmt.Sprintf(
		"dingo/%s (%s/%s)",
		version.GetVersionString(),
		runtime.GOOS,
		runtime.GOARCH,
	)
}

// Set sets Dingo's default user-agent unless the caller already supplied one.
func Set(req *http.Request) {
	if req != nil && req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", Default())
	}
}

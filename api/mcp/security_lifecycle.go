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

package mcp

import "net/http"

type managedSecurityHandler struct {
	handler http.Handler
	limiter *clientLimiter
	onClose func()
}

func (h *managedSecurityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handler.ServeHTTP(w, r)
}

func (h *managedSecurityHandler) Close() {
	if h.onClose != nil {
		h.onClose()
	}
	if h.limiter != nil {
		h.limiter.close()
	}
}

func (cl *clientLimiter) close() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.closed {
		return
	}
	cl.closed = true
	if cl.timer != nil {
		cl.timer.Stop()
		cl.timer = nil
	}
	cl.entries = make(map[string]*clientLimiterEntry)
	cl.order.Init()
}

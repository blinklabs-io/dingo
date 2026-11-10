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

package bark

import (
	"context"
	"net/http"
)

type requestClientContextKey struct{}

func withRequestClient(ctx context.Context, client string) context.Context {
	return context.WithValue(ctx, requestClientContextKey{}, client)
}

func requestClientFromContext(ctx context.Context) string {
	client, _ := ctx.Value(requestClientContextKey{}).(string)
	return client
}

func requestClientMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client := archiveCallerKey(r.RemoteAddr)
		next.ServeHTTP(w, r.WithContext(withRequestClient(r.Context(), client)))
	})
}

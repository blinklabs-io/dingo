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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

var poolListTestPaths = []string{
	"/api/v0/pools",
	"/api/v0/pools/retiring",
}

// TestHandlePoolListClientDisconnectWritesNoError covers a client that leaves
// mid-query on either route: the handler logs and answers nothing, not a 500.
func TestHandlePoolListClientDisconnectWritesNoError(t *testing.T) {
	t.Parallel()
	node := &poolListCanceledNode{mockNode: &mockNode{}}
	b := newTestBlockfrost(node)
	for _, path := range poolListTestPaths {
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(
			w, addressSummaryRequest(context.Background(), path),
		)
		require.NotEqual(t, http.StatusInternalServerError, w.Code, path)
		require.Empty(t, w.Body.String(), path)
	}
}

type poolListCanceledNode struct {
	*mockNode
}

func (poolListCanceledNode) PoolsList(
	context.Context, PaginationParams,
) ([]string, int, error) {
	return nil, 0, context.Canceled
}

func (poolListCanceledNode) PoolsRetiring(
	context.Context, PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	return nil, 0, context.Canceled
}

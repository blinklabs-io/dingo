//go:build dingo_extra_plugins

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

package gcs

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestResetBatchUsesFreshTimeoutForEveryGCSDelete(t *testing.T) {
	keys := [][]byte{[]byte("first"), []byte("second")}
	deleteCalls := 0
	err := deleteBatchIndividually(
		t.Context(),
		keys,
		10*time.Millisecond,
		func(ctx context.Context, _ []byte) error {
			deleteCalls++
			if deleteCalls == 1 {
				// Consume the first operation's complete timeout. A batch-wide
				// context would now be expired and never issue the second delete.
				<-ctx.Done()
				return types.ErrBlobKeyNotFound
			}
			require.NoError(t, ctx.Err())
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, len(keys), deleteCalls)
}

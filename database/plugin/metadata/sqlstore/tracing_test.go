// Copyright 2024 Blink Labs Software
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

package sqlstore

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	_ "modernc.org/sqlite"
)

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestSQLSpansOnlyUnderParent(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	db, err := OpenDB("sqlite", ":memory:", "sqlite", true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Empty(t, recorder.Ended())

	ctx, parent := otel.Tracer("test").Start(context.Background(), "parent")
	_, err = db.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	parent.End()

	var children int
	for _, span := range recorder.Ended() {
		if span.Parent().SpanID() == parent.SpanContext().SpanID() {
			children++
		}
	}
	require.NotZero(t, children)
}

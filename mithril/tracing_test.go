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

package mithril

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestMithrilBootstrapPhaseSpans(t *testing.T) {
	spans := testutil.RecordSpans(t)
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 2})
	result, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	byName := map[string]int{}
	var root string
	ended := spans.Ended()
	for _, span := range ended {
		byName[span.Name()]++
		if span.Name() == "mithril.bootstrap" {
			root = span.SpanContext().SpanID().String()
		}
	}
	require.Equal(t, 1, byName["mithril.bootstrap"])
	for _, phase := range []string{
		"mithril.select_artifact",
		"mithril.verify_certificate",
		"mithril.fetch_digests",
		"mithril.download_immutables",
		"mithril.download_ancillary",
	} {
		require.Equal(t, 1, byName[phase], phase)
	}
	for _, span := range ended {
		if span.Name() != "mithril.bootstrap" &&
			span.Parent().SpanID().String() == root {
			continue
		}
		if span.Name() == "mithril.bootstrap" {
			continue
		}
		t.Errorf("span %q is not a child of mithril.bootstrap", span.Name())
	}
}

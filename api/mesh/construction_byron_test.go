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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConstructionMetadataByronEraNoFeeSubstitution traces the Byron-era
// behavior of the /construction/metadata consumer of GetCurrentPParams.
//
// Byron carries no protocol-parameter CBOR, so the ledger reports nil for a
// genuine Byron prefix. The defined behavior is the retriable ErrUnavailable
// that TestConstructionMetadataUnavailable already covers; what this adds is
// the substitution prohibition. A zero-valued fee body would be worse than an
// error here, because a client that ignored the status would sign a
// transaction whose fee was computed from parameters nobody configured.
func TestConstructionMetadataByronEraNoFeeSubstitution(t *testing.T) {
	deps := newTestDeps()
	require.Nil(
		t,
		deps.ledger.pparams,
		"precondition: Byron prefix reports no current pparams",
	)
	h := newTestHandler(t, deps)

	rec := postJSON(
		t, h, "/construction/metadata", metadataRequest(),
	)

	got := requireMeshError(
		t, rec, ErrUnavailable, http.StatusServiceUnavailable,
	)
	require.True(
		t,
		got.Retriable,
		"a Byron prefix resolves once the chain reaches Shelley",
	)

	// The body must be an error document only — no metadata or fee fields a
	// lenient client could pick up.
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.NotContains(t, body, "metadata")
	assert.NotContains(t, body, "suggested_fee")
}

// TestConstructionMetadataByronEraDescribesCause pins the operator-facing
// half. "service unavailable" alone sends someone hunting a node fault during
// what is a normal stage of a from-genesis sync, so the wrapped cause has to
// name protocol-parameter availability. Mesh already does this; the test locks
// it against a future refactor that drops the wrapped error on the floor.
func TestConstructionMetadataByronEraDescribesCause(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postJSON(
		t, h, "/construction/metadata", metadataRequest(),
	)

	var got Error
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Contains(t, got.Details, "error")
	assert.Contains(
		t,
		got.Details["error"],
		"protocol parameters",
	)
}

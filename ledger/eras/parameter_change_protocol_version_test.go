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

package eras

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEraDescWiresProtocolVersionProtection is the dingo#4439 "protect
// replay, import, and backfill paths" regression. ConwayEraDesc and
// DijkstraEraDesc.{ValidateTxFunc,PParamsUpdateFunc} are the single
// implementation every caller shares -- live block application
// (ledger/delta.go), Mithril bootstrap (mithril/sync_gap.go), backfill
// (internal/node/backfill.go), and conformance replay
// (internal/test/conformance/state_manager.go) all resolve validation and
// enactment through these fields rather than calling ValidateTxConway or
// PParamsUpdateConway directly. There is no separate replay-specific
// validation or enactment path to protect; this pins that the era
// descriptors actually point at the protected functions, so a future
// refactor cannot quietly rewire one path to a stale copy while leaving
// this test's direct-call coverage green.
func TestEraDescWiresProtocolVersionProtection(t *testing.T) {
	require.Equal(
		t,
		reflect.ValueOf(ValidateTxConway).Pointer(),
		reflect.ValueOf(ConwayEraDesc.ValidateTxFunc).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(PParamsUpdateConway).Pointer(),
		reflect.ValueOf(ConwayEraDesc.PParamsUpdateFunc).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(ValidateTxDijkstra).Pointer(),
		reflect.ValueOf(DijkstraEraDesc.ValidateTxFunc).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(PParamsUpdateDijkstra).Pointer(),
		reflect.ValueOf(DijkstraEraDesc.PParamsUpdateFunc).Pointer(),
	)
}

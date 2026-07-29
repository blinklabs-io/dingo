// Copyright 2025 Blink Labs Software
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

package main

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/stretchr/testify/require"
)

// TestCheckResultErrOnPersistedOutcomeAlone guards against the bug where a
// fresh cached FAIL/ERROR (no epoch freshly (re)checked this run, so
// CheckResult is empty or nil) was reported as success. run.go now derives its
// exit-code input from koiosparity.EffectiveCheckOutcome(statuses, 0, 0)
// rather than the raw CheckResult returned by Check — this test exercises
// checkResultErr against exactly that kind of "zero fresh work, but persisted
// failure" result, for both FAIL and ERROR statuses.
func TestCheckResultErrOnPersistedOutcomeAlone(t *testing.T) {
	failOnly := koiosparity.EffectiveCheckOutcome([]koiosparity.CheckEpochStatus{
		{Epoch: 5, Status: koiosparity.StatusFail},
	}, 0, 0)
	require.Zero(t, failOnly.EpochsChecked, "no epoch was freshly checked")
	require.Error(t, checkResultErr(failOnly))

	errorOnly := koiosparity.EffectiveCheckOutcome([]koiosparity.CheckEpochStatus{
		{Epoch: 7, Status: koiosparity.StatusError},
	}, 0, 0)
	require.Error(t, checkResultErr(errorOnly))

	allPass := koiosparity.EffectiveCheckOutcome([]koiosparity.CheckEpochStatus{
		{Epoch: 1, Status: koiosparity.StatusPass},
	}, 0, 0)
	require.NoError(t, checkResultErr(allPass))
}

func TestCheckResultErrNilResult(t *testing.T) {
	require.NoError(t, checkResultErr(nil))
}

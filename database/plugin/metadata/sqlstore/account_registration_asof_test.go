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

package sqlstore

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetLatestAccountRegistrationAtOrBeforeWithoutHistory(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	row, err := store.GetLatestAccountRegistrationAtOrBefore(
		0, make([]byte, 28), 1_000, nil,
	)
	require.NoError(t, err)
	require.Nil(t, row)

	row, err = store.GetLatestAccountRegistrationAtOrBefore(0, nil, 1_000, nil)
	require.NoError(t, err)
	require.Nil(t, row)

	_, err = store.GetLatestAccountRegistrationAtOrBefore(
		0, make([]byte, 28), math.MaxUint64, nil,
	)
	require.Error(t, err, "a slot above math.MaxInt64 must be rejected")
}

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

package drepquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVotingPowerSQLPostgresPlaceholderReuse(t *testing.T) {
	credential := []byte("credential")
	offSQL, offArgs := VotingPowerSQL("postgres", 1, credential, 0)
	require.NotContains(t, offSQL, "expiration_epoch")
	require.Equal(t, []any{credential, uint8(1)}, offArgs)

	onSQL, onArgs := VotingPowerSQL("postgres", 1, credential, 5)
	require.Equal(t, 2, strings.Count(onSQL, "expiration_epoch >= $3"))
	require.NotContains(t, onSQL, "expiration_epoch >= ?")
	require.Equal(t, []any{credential, uint8(1), uint64(5)}, onArgs)
}

func TestCollectionArgsExpiryOrder(t *testing.T) {
	values := []uint64{2, 3}
	require.Equal(t, []any{values, values}, CollectionArgs(values, 0))
	require.Equal(
		t,
		[]any{uint64(5), values, uint64(5), values},
		CollectionArgs(values, 5),
	)
}

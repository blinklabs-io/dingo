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

package badger

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueThresholdChangesPreserveMixedStoredValues(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sizes := []int{512, 2048, 64 << 10, 999953, 1 << 20}
	for phase, threshold := range []int64{
		DefaultValueThreshold,
		DefaultCoreLeiosValueThreshold,
		DefaultValueThreshold,
	} {
		store, err := New(
			WithDataDir(dir), WithGc(false),
			WithValueThreshold(threshold),
			WithMemTableSize(8<<20), WithValueLogFileSize(16<<20),
		)
		require.NoError(t, err)
		func() {
			defer func() { require.NoError(t, store.Close()) }()
			for previous := range phase {
				for index, size := range sizes {
					txn := store.NewTransaction(false)
					got, err := store.Get(
						txn,
						[]byte{byte(previous), byte(index)},
					)
					require.NoError(t, txn.Rollback())
					require.NoError(t, err)
					require.True(t, bytes.Equal(
						bytes.Repeat([]byte{byte(previous + index)}, size), got,
					), "stored bytes changed after value threshold transition")
				}
			}
			if phase == 2 {
				return
			}
			for index, size := range sizes {
				txn := store.NewTransaction(true)
				err := store.Set(txn, []byte{byte(phase), byte(index)},
					bytes.Repeat([]byte{byte(phase + index)}, size))
				if err != nil {
					txn.Rollback() //nolint:errcheck
					t.Fatal(err)
				}
				require.NoError(t, txn.Commit())
			}
		}()
	}
}

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

package database

import (
	"bytes"
	"context"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestGetDatumsAlignsResultsAndReportsMissingAsNil covers the batch contract:
// the result lines up index for index with the requested hashes, and a hash
// with no stored datum is a nil entry, not an error that discards the rest.
func TestGetDatumsAlignsResultsAndReportsMissingAsNil(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	raw := []byte{0x01}
	require.NoError(t, db.SetDatum(raw, 1, nil))
	stored := lcommon.Blake2b256Hash(raw).Bytes()
	missing := bytes.Repeat([]byte{0xEE}, lcommon.Blake2b256Size)

	got, err := db.GetDatums(
		context.Background(),
		[][]byte{missing, stored},
	)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Nil(t, got[0])
	require.NotNil(t, got[1])
	require.Equal(t, raw, got[1].RawDatum)
}

// TestGetDatumsCanceledContext covers a batch started with an already
// cancelled context. It must stop and return the context error instead of
// reading.
func TestGetDatumsCanceledContext(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.GetDatums(ctx, [][]byte{
		bytes.Repeat([]byte{0x01}, lcommon.Blake2b256Size),
	})
	require.ErrorIs(t, err, context.Canceled)
}

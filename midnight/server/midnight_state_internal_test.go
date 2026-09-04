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

package server

import (
	"errors"
	"log/slog"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestEffectivePageSize verifies zero/omitted capacity requests fall back
// to a bounded default instead of being forwarded to the store as an
// unbounded scan (limit <= 0 means "no SQL LIMIT" in the store contract),
// and that oversized requests are clamped rather than honored as-is.
func TestEffectivePageSize(t *testing.T) {
	t.Parallel()
	require.Equal(t, defaultEventPageSize, effectivePageSize(0))
	require.Equal(t, 1, effectivePageSize(1))
	require.Equal(t, maxEventPageSize, effectivePageSize(maxEventPageSize))
	require.Equal(t, maxEventPageSize, effectivePageSize(maxEventPageSize+1))
}

// TestMidnightBlockNumber_Boundary verifies midnightBlockNumber accepts
// every value representable in the Midnight wire type's uint32 and rejects
// a stored value one past it, rather than silently wrapping (uint32(v) on a
// value like 1<<32 truncates to 0, corrupting the reported block number).
func TestMidnightBlockNumber_Boundary(t *testing.T) {
	t.Parallel()

	got, err := midnightBlockNumber(math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, uint32(math.MaxUint32), got)

	_, err = midnightBlockNumber(uint64(math.MaxUint32) + 1)
	require.Error(t, err)
}

// TestMidnightTimestamp_Boundary verifies midnightTimestamp accepts every
// value representable in the Midnight wire type's int64 and rejects a
// stored value one past it, rather than silently wrapping into a negative
// timestamp (int64(v) on math.MaxInt64+1 produces math.MinInt64).
func TestMidnightTimestamp_Boundary(t *testing.T) {
	t.Parallel()

	got, err := midnightTimestamp(math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), got)

	_, err = midnightTimestamp(uint64(math.MaxInt64) + 1)
	require.Error(t, err)
}

// TestCheckedUint32_Boundary verifies checkedUint32 accepts every value
// representable in uint32 and rejects a value one past it, rather than
// silently wrapping.
func TestCheckedUint32_Boundary(t *testing.T) {
	t.Parallel()

	got, err := checkedUint32(math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, uint32(math.MaxUint32), got)

	_, err = checkedUint32(uint64(math.MaxUint32) + 1)
	require.Error(t, err)
}

// TestCheckedUint64_Boundary verifies checkedUint64 accepts a non-negative
// unix-seconds timestamp and rejects a negative one, rather than silently
// wrapping it into a huge unsigned number.
func TestCheckedUint64_Boundary(t *testing.T) {
	t.Parallel()

	got, err := checkedUint64(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)

	_, err = checkedUint64(-1)
	require.Error(t, err)
}

// TestInternalError_DoesNotLeakDetail verifies internalError returns a
// stable, generic message naming only op — never the wrapped error's own
// text, which can carry driver-specific SQL text, file paths, or CBOR
// diagnostics that must stay server-side — and that it tolerates a nil
// logger (a bare &service{} in a white-box test, rather than one built via
// New, which always defaults Logger).
func TestInternalError_DoesNotLeakDetail(t *testing.T) {
	t.Parallel()

	svc := &service{}
	err := svc.internalError(
		"get technical committee datum",
		status.Error(codes.Unknown, "pq: connection refused at 10.0.0.5:5432"),
	)
	require.Equal(t, codes.Internal, status.Code(err))
	msg := status.Convert(err).Message()
	require.Equal(t, "get technical committee datum failed", msg)
	require.NotContains(t, msg, "10.0.0.5")

	svc.logger = slog.Default()
	err = svc.internalError(
		"get technical committee datum",
		errors.New("boom"),
	)
	require.Equal(t, codes.Internal, status.Code(err))
}

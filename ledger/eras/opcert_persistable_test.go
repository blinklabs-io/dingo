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

package eras_test

import (
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateOpCertPersistableCounter pins the boundary between a counter
// dingo records and one it refuses. The bound is math.MaxInt64 because
// pool_opcert_sequence.sequence and pool.latest_op_cert_sequence are signed
// engine integers that carry the monotonicity ordering as well as the value.
func TestValidateOpCertPersistableCounter(t *testing.T) {
	tests := []struct {
		name      string
		candidate uint64
		wantErr   bool
	}{
		{name: "zero", candidate: 0},
		{
			name:      "beyond uint32 is recorded",
			candidate: uint64(math.MaxUint32) + 1,
		},
		{
			name:      "at the bound",
			candidate: uint64(math.MaxInt64),
		},
		{
			name:      "one past the bound",
			candidate: uint64(math.MaxInt64) + 1,
			wantErr:   true,
		},
		{
			name:      "max uint64",
			candidate: math.MaxUint64,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := eras.ValidateOpCertPersistableCounter(tt.candidate)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			// The message has to name the bound: an operator seeing this
			// is looking at a certificate the reference accepts, and the
			// only useful thing to say is which limit is dingo's.
			assert.Contains(t, err.Error(), "9223372036854775807")
			assert.Contains(t, err.Error(), "pool_opcert_sequence")
		})
	}
}

// TestMaxPersistableOpCertCounterMatchesSignedColumn documents why the bound
// is where it is: it is the largest value a signed 64-bit SQL column holds,
// which is what checkedInt64 enforces at the metadata store.
func TestMaxPersistableOpCertCounterMatchesSignedColumn(t *testing.T) {
	assert.Equal(
		t,
		uint64(math.MaxInt64),
		eras.MaxPersistableOpCertCounter,
	)
}

// TestValidateOpCertCounterMaxUint64DoesNotWrap pins the era rule's gap
// comparison at the top of the counter domain: an unchanged counter at
// math.MaxUint64 must not be misread as gapped by a stored+1 that wrapped to
// zero. The rule is evaluated over the full unsigned range even though the
// forging and storage paths refuse to record a counter that large, because it
// is the chain rule rather than dingo's persistence bound.
func TestValidateOpCertCounterMaxUint64DoesNotWrap(t *testing.T) {
	require.NoError(
		t,
		eras.ValidateOpCertCounter(math.MaxUint64, true, math.MaxUint64, true),
	)
	require.Error(
		t,
		eras.ValidateOpCertCounter(math.MaxUint64, true, math.MaxUint64-1, true),
	)
}

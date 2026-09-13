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

package dingo

import (
	"errors"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

type forgedValidationRecorder struct {
	aggregateCalls int
	fullCalls      int
	err            error
}

func (v *forgedValidationRecorder) ValidateForgedBlock(
	gledger.Block,
	[]byte,
) error {
	v.fullCalls++
	return v.err
}

func (v *forgedValidationRecorder) ValidateBlockReferenceScripts(
	gledger.Block,
) error {
	v.aggregateCalls++
	return v.err
}

func TestForgedBlockValidatorDefaultAndFullModes(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "default"
		if full {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			failure := errors.New("aggregate reference-script budget exceeded")
			state := &forgedValidationRecorder{err: failure}
			validator := newForgedBlockValidator(state, full)
			require.NotNil(
				t,
				validator,
				"default mode must retain aggregate validation",
			)
			require.ErrorIs(
				t,
				validator.ValidateForgedBlock(&conway.ConwayBlock{}, nil),
				failure,
			)
			state.err = nil
			require.NoError(
				t,
				validator.ValidateForgedBlock(&conway.ConwayBlock{}, nil),
			)
			if full {
				require.Equal(t, 2, state.fullCalls)
				require.Zero(
					t,
					state.aggregateCalls,
					"full validation owns its aggregate check",
				)
			} else {
				require.Equal(t, 2, state.aggregateCalls)
				require.Zero(t, state.fullCalls, "default mode must not execute full validation")
			}
		})
	}
}

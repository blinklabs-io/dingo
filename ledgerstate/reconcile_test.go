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

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func b32(fill byte) []byte { return bytes.Repeat([]byte{fill}, 32) }

func TestValidateReconcileImportConfigRequiresCompleteInputs(t *testing.T) {
	t.Parallel()

	base := ImportConfig{
		State: &RawLedgerState{
			Tip:           &SnapshotTip{Slot: 1, BlockHash: b32(0x01)},
			UTxOData:      []byte{0xa0},
			CertStateData: []byte{0x83, 0xa0, 0xa0, 0xa0},
		},
	}
	require.NoError(t, validateReconcileImportConfig(base))

	withImportKey := base
	withImportKey.ImportKey = "digest:1"
	require.ErrorContains(
		t,
		validateReconcileImportConfig(withImportKey),
		"ImportKey",
	)

	missingUTxO := base
	missingUTxO.State = &RawLedgerState{
		Tip:           base.State.Tip,
		CertStateData: base.State.CertStateData,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingUTxO),
		"UTxO",
	)

	missingCertState := base
	missingCertState.State = &RawLedgerState{
		Tip:      base.State.Tip,
		UTxOData: base.State.UTxOData,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingCertState),
		"cert-state",
	)

	missingGovState := base
	missingGovState.State = &RawLedgerState{
		Tip:           base.State.Tip,
		UTxOData:      base.State.UTxOData,
		CertStateData: base.State.CertStateData,
		EraIndex:      EraConway,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingGovState),
		"Conway governance state",
	)

	withGovState := missingGovState
	withGovState.State.GovStateData = []byte{0x87}
	require.NoError(t, validateReconcileImportConfig(withGovState))

	missingUTxOHDTable := base
	missingUTxOHDTable.State = &RawLedgerState{
		Tip:           base.State.Tip,
		UTxOHD:        true,
		UTxOData:      base.State.UTxOData,
		CertStateData: base.State.CertStateData,
	}
	require.ErrorContains(
		t,
		validateReconcileImportConfig(missingUTxOHDTable),
		"UTxO-HD",
	)
}

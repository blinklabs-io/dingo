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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlstore

import (
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// feelessTransaction stands in for a transaction body whose Fee is nil, which
// is what TransactionBodyBase returns for any body that does not override it --
// the synthetic transaction carrying imported certificates among them. Fee is
// overridden here only so a single type can cover both the nil and non-nil
// cases; the write-path reproduction lives in ledgerstate.
type feelessTransaction struct {
	lcommon.TransactionBodyBase
	fee *big.Int
}

func (t *feelessTransaction) Type() int     { return 0 }
func (t *feelessTransaction) Cbor() []byte  { return nil }
func (t *feelessTransaction) IsValid() bool { return true }
func (t *feelessTransaction) Fee() *big.Int { return t.fee }
func (t *feelessTransaction) Metadata() lcommon.TransactionMetadatum {
	return nil
}

func (t *feelessTransaction) AuxiliaryData() lcommon.AuxiliaryData { return nil }

func (t *feelessTransaction) Certificates() []lcommon.Certificate { return nil }

func (t *feelessTransaction) Consumed() []lcommon.TransactionInput { return nil }

func (t *feelessTransaction) Produced() []lcommon.Utxo { return nil }

func (t *feelessTransaction) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (t *feelessTransaction) Id() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

func (t *feelessTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (t *feelessTransaction) ProtocolParameterUpdates() (uint64, map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate) {
	return 0, nil
}

func (t *feelessTransaction) Witnesses() lcommon.TransactionWitnessSet {
	return nil
}

// TestTransactionFeeTreatsNilAsZero unit-tests the accessor. It does not by
// itself prove the write path is guarded -- reverting the setTransaction call
// site leaves this green -- so the reproduction that exercises
// persistImportedCommitteeCertificates end to end lives in
// ledgerstate/imported_committee_certificates_test.go.
func TestTransactionFeeTreatsNilAsZero(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		require.Equal(
			t,
			uint64(0),
			uint64(transactionFee(&feelessTransaction{})),
		)
	})

	// A real fee is still recorded unchanged.
	require.Equal(
		t,
		uint64(174301),
		uint64(transactionFee(
			&feelessTransaction{fee: big.NewInt(174301)},
		)),
	)
}

// TestTransactionBodyBaseFeeIsNil pins the upstream behaviour this guard exists
// for. If gouroboros ever returns a zero big.Int instead, the guard becomes
// redundant rather than wrong, but the write path must never assume it.
func TestTransactionBodyBaseFeeIsNil(t *testing.T) {
	t.Parallel()

	var base lcommon.TransactionBodyBase
	require.Nil(t, base.Fee())
}

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

package txpump

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	"github.com/stretchr/testify/require"
)

func TestWorkloadSubmissionsKeepControlledChangeAddress(t *testing.T) {
	for _, workload := range []struct {
		name   string
		submit func(*Pump, *NodeClient, int) bool
	}{
		{"delegation", (*Pump).submitDelegation},
		{"governance", (*Pump).submitGovernance},
		{"plutus lock", (*Pump).submitPlutus},
	} {
		t.Run(workload.name, func(t *testing.T) {
			controlled := append([]byte{0x60}, bytes.Repeat([]byte{0x42}, 28)...)
			pump := testPump(time.Now().Add(-time.Second), time.Second)
			pump.cfg.DelegationStakeKeyHash = hex.EncodeToString(sampleStakeKeyHash)
			pump.cfg.DelegationPoolKeyHash = hex.EncodeToString(samplePoolKeyHash)
			pump.wallet.Add(UTxO{
				TxHash: sampleHash, Index: 0, Amount: 600_000_000,
				SigningKey: &UTxOKey{Address: controlled},
			})
			submitted := make(chan []byte, 1)
			cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
				func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
					submitted <- tx.Raw.Content.([]byte)
					return nil
				},
			))
			client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))
			require.True(t, workload.submit(pump, client, 1))
			var raw []byte
			select {
			case raw = <-submitted:
			case <-time.After(5 * time.Second):
				t.Fatal("submission callback was not reached")
			}
			var tx conway.ConwayTransaction
			_, err := cbor.Decode(raw, &tx)
			require.NoError(t, err)
			outputs := tx.Outputs()
			require.NotEmpty(t, outputs)
			change, err := outputs[len(outputs)-1].Address().Bytes()
			require.NoError(t, err)
			require.Equal(t, controlled, change, "submitted change must remain queryable by the wallet")
			if workload.name == "plutus lock" {
				require.Len(t, pump.plutusLocked, 1)
				require.Equal(t, controlled, pump.plutusLocked[0].address)
			}
		})
	}
}

func TestPlutusUnlockReturnsChangeToLockedWalletAddress(t *testing.T) {
	controlled := append([]byte{0x60}, bytes.Repeat([]byte{0x24}, 28)...)
	pump := testPump(time.Now().Add(-time.Second), time.Second)
	pump.cfg.ConfirmationSlots = 0
	pump.cfg.SlotLength = 0
	pump.wallet.Add(UTxO{
		TxHash: sampleHash, Index: 0, Amount: 600_000_000,
		SigningKey: &UTxOKey{Address: controlled},
	})
	submitted := make(chan []byte, 2)
	cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
		func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
			submitted <- tx.Raw.Content.([]byte)
			return nil
		},
	))
	client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))

	pump.intRangeFn = func(int, int) int { return 0 }
	require.True(t, pump.submitPlutus(client, 1), "Plutus lock submission must reach the protocol")
	require.Len(t, pump.plutusLocked, 1)
	pump.intRangeFn = func(int, int) int { return 1 }
	require.True(t, pump.submitPlutus(client, 1), "Plutus unlock submission must reach the protocol")

	var raw []byte
	select {
	case <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("lock submission callback was not reached")
	}
	select {
	case raw = <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("unlock submission callback was not reached")
	}
	var tx conway.ConwayTransaction
	_, err := cbor.Decode(raw, &tx)
	require.NoError(t, err)
	outputs := tx.Outputs()
	require.Len(t, outputs, 1)
	change, err := outputs[0].Address().Bytes()
	require.NoError(t, err)
	require.Equal(t, controlled, change, "Plutus unlock change must remain queryable by the wallet")
}

// TestUnsignedWorkloadSubmissionsKeepDeterministicChangeAddress pins the
// fallback that keeps keyless harness wallets submitting: with no signing key
// on the selected input, change returns to the address derived from the input
// transaction hash, and acceptance stays on the pacing-only path.
func TestUnsignedWorkloadSubmissionsKeepDeterministicChangeAddress(t *testing.T) {
	for _, workload := range []struct {
		name   string
		submit func(*Pump, *NodeClient, int) bool
	}{
		{"delegation", (*Pump).submitDelegation},
		{"governance", (*Pump).submitGovernance},
		{"plutus lock", (*Pump).submitPlutus},
	} {
		t.Run(workload.name, func(t *testing.T) {
			pump := testPump(time.Now().Add(-time.Second), time.Second)
			pump.cfg.DelegationStakeKeyHash = hex.EncodeToString(sampleStakeKeyHash)
			pump.cfg.DelegationPoolKeyHash = hex.EncodeToString(samplePoolKeyHash)
			pump.wallet.Add(UTxO{TxHash: sampleHash, Index: 0, Amount: 600_000_000})
			submitted := make(chan []byte, 1)
			cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
				func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
					submitted <- tx.Raw.Content.([]byte)
					return nil
				},
			))
			client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))
			require.True(t, workload.submit(pump, client, 1))
			var raw []byte
			select {
			case raw = <-submitted:
			case <-time.After(5 * time.Second):
				t.Fatal("submission callback was not reached")
			}
			var tx conway.ConwayTransaction
			_, err := cbor.Decode(raw, &tx)
			require.NoError(t, err)
			outputs := tx.Outputs()
			require.NotEmpty(t, outputs)
			change, err := outputs[len(outputs)-1].Address().Bytes()
			require.NoError(t, err)
			require.Equal(t, deterministicAddr(sampleHash), change,
				"a keyless input must keep the deterministic change address")
			require.Empty(t, pump.wallet.PendingIDs(),
				"an unsigned wallet keeps the pacing-only acceptance path")
		})
	}
}

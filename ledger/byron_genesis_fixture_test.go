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

package ledger

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
)

// loadByronGenesisForTest fills fields that make a fixture valid as Byron
// genesis while preserving fields the test is exercising.
func loadByronGenesisForTest(
	t testing.TB,
	cfg *cardano.CardanoNodeConfig,
	r io.Reader,
) error {
	t.Helper()

	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var genesis map[string]json.RawMessage
	if err := json.Unmarshal(raw, &genesis); err != nil {
		return err
	}
	protocolMagic := uint32(164)
	if protocolConsts, ok := genesis["protocolConsts"]; ok {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(protocolConsts, &fields); err != nil {
			return err
		}
		if magic, ok := fields["protocolMagic"]; ok {
			if err := json.Unmarshal(magic, &protocolMagic); err != nil {
				return err
			}
		}
	}
	issuer := newByronPBFTTestKey(0x31)
	delegate := newByronPBFTTestKey(0x32)
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		issuer.verificationKey,
	)
	if err != nil {
		return err
	}
	certificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, delegate,
	)
	bootStakeholders, err := json.Marshal(map[string]uint64{
		issuerHash.String(): 1,
	})
	if err != nil {
		return err
	}
	heavyDelegation, err := json.Marshal(map[string]any{
		issuerHash.String(): map[string]any{
			"cert": hex.EncodeToString(certificate[3].([]byte)),
			"delegatePk": base64.StdEncoding.EncodeToString(
				delegate.verificationKey,
			),
			"issuerPk": base64.StdEncoding.EncodeToString(
				issuer.verificationKey,
			),
			"omega": 0,
		},
	})
	if err != nil {
		return err
	}
	defaults := map[string]json.RawMessage{
		"avvmDistr":        json.RawMessage(`{}`),
		"bootStakeholders": bootStakeholders,
		"heavyDelegation":  heavyDelegation,
		"nonAvvmBalances":  json.RawMessage(`{}`),
		"startTime":        json.RawMessage(`1788739200`),
		"blockVersionData": json.RawMessage(`{
			"heavyDelThd":"300000000000","maxBlockSize":"2000000",
			"maxHeaderSize":"2000000","maxProposalSize":"700",
			"maxTxSize":"4096","mpcThd":"20000000000000",
			"scriptVersion":0,"slotDuration":"20000",
			"softforkRule":{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"},
			"txFeePolicy":{"multiplier":"43946000000","summand":"155381000000000"},
			"unlockStakeEpoch":"18446744073709551615","updateImplicit":"10000",
			"updateProposalThd":"100000000000000","updateVoteThd":"1000000000000"
		}`),
		"protocolConsts": json.RawMessage(`{"k":108,"protocolMagic":164}`),
	}
	for key, value := range defaults {
		if _, ok := genesis[key]; !ok {
			genesis[key] = value
		}
	}
	for key, nestedDefaults := range map[string]map[string]json.RawMessage{
		"blockVersionData": {
			"heavyDelThd": json.RawMessage(`"300000000000"`), "maxBlockSize": json.RawMessage(`"2000000"`),
			"maxHeaderSize": json.RawMessage(`"2000000"`), "maxProposalSize": json.RawMessage(`"700"`),
			"maxTxSize": json.RawMessage(`"4096"`), "mpcThd": json.RawMessage(`"20000000000000"`),
			"scriptVersion": json.RawMessage(`0`), "slotDuration": json.RawMessage(`"20000"`),
			"softforkRule":     json.RawMessage(`{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"}`),
			"txFeePolicy":      json.RawMessage(`{"multiplier":"43946000000","summand":"155381000000000"}`),
			"unlockStakeEpoch": json.RawMessage(`"18446744073709551615"`), "updateImplicit": json.RawMessage(`"10000"`),
			"updateProposalThd": json.RawMessage(`"100000000000000"`), "updateVoteThd": json.RawMessage(`"1000000000000"`),
		},
		"protocolConsts": {"k": json.RawMessage(`108`), "protocolMagic": json.RawMessage(`164`)},
	} {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(genesis[key], &nested); err != nil {
			return err
		}
		if nested == nil {
			continue
		}
		for nestedKey, value := range nestedDefaults {
			if _, ok := nested[nestedKey]; !ok {
				nested[nestedKey] = value
			}
		}
		encoded, err := json.Marshal(nested)
		if err != nil {
			return err
		}
		genesis[key] = encoded
	}
	completed, err := json.Marshal(genesis)
	if err != nil {
		return err
	}
	return cfg.LoadByronGenesisFromReader(bytes.NewReader(completed))
}

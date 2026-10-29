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

package ledgerrulesconformance

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
)

func TestOutsideForecastMatchesExpectedMessage(t *testing.T) {
	t.Parallel()
	for _, message := range []string{
		"outside forecast",
		"OutsideForecast (Slot 500000)",
	} {
		t.Run(message, func(t *testing.T) {
			t.Parallel()
			if !predicateMatches(
				errors.New(message),
				expectedDoc{Predicate: "OutsideForecast"},
			) {
				t.Fatalf("OutsideForecast did not match %q", message)
			}
		})
	}
}

func TestUnelectedCommitteeVoterIsReportedAsVotersDoNotExist(t *testing.T) {
	t.Parallel()
	err := errors.New(
		"conway utxo validation rule 60: committee voter is not elected",
	)
	for _, predicate := range []string{"VotersDoNotExist", "UnelectedCommitteeVoters"} {
		if !predicateMatches(err, expectedDoc{Predicate: predicate}) {
			t.Fatalf("an unelected committee voter did not match %s", predicate)
		}
	}
	if predicateMatches(
		errors.New("vote cast by unknown voter"),
		expectedDoc{Predicate: "UnelectedCommitteeVoters"},
	) {
		t.Fatal("an unknown voter matched UnelectedCommitteeVoters")
	}
}

func TestResolveReferencedObjectAppliesSiblingOverrides(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "common.json"), []byte(`{"slot":1,"epoch":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveReferencedObject(
		root,
		json.RawMessage(`{"$ref":"common.json","$override":{"epoch":3}}`),
	)
	if err != nil {
		t.Fatalf("resolve referenced object: %v", err)
	}
	if string(resolved["slot"]) != "1" || string(resolved["epoch"]) != "3" {
		t.Fatalf(
			"resolved object = %s, want slot 1 and epoch 3",
			mustJSON(t, resolved),
		)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return string(encoded)
}

func TestObservedLedgerResult(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		observed string
		want     string
		ok       bool
	}{
		{"accepted", "transaction accepted", "pass", true},
		{
			"unregistered DRep",
			"rejected: " + (conway.DRepNotRegisteredError{}).Error(),
			"DRepNotRegistered",
			true,
		},
		{
			"unregistered stake key",
			"rejected: " + (conway.StakeCredentialNotRegisteredError{}).Error(),
			"StakeKeyNotRegistered",
			true,
		},
		{"decoded", "transaction decoded successfully", "pass", true},
		{
			"decoding failure",
			"transaction decoding failure: bad cbor",
			"DecodingFailure",
			true,
		},
		{
			"mapped predicate",
			"rejected: output too small",
			"BabbageOutputTooSmallUTxO",
			true,
		},
		{
			"unelected committee voter",
			"rejected: conway utxo validation rule 60: committee voter is not elected",
			"VotersDoNotExist",
			true,
		},
		{
			"stake registration deposit",
			"rejected: " + conway.CertificateDepositIncorrectError{
				CertificateType: common.CertificateTypeStakeRegistration,
				Supplied:        1,
				Expected:        2,
			}.Error(),
			"IncorrectDepositDELEG",
			true,
		},
		{
			"DRep registration deposit",
			"rejected: " + conway.CertificateDepositIncorrectError{
				CertificateType: common.CertificateTypeRegistrationDrep,
				Supplied:        110,
				Expected:        500000000,
			}.Error(),
			"IncorrectDepositDRep",
			true,
		},
		{"unmapped rejection", "rejected: something novel", "", false},
		{"unknown observation", "weird", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := observedLedgerResult(tc.observed)
			if got != tc.want || ok != tc.ok {
				t.Fatalf(
					"observedLedgerResult(%q) = %q, %v; want %q, %v",
					tc.observed, got, ok, tc.want, tc.ok,
				)
			}
		})
	}
}

func TestUnregisteredDelegationPredicate(t *testing.T) {
	t.Parallel()
	credential := common.Credential{CredType: common.CredentialTypeAddrKeyHash}
	other := credential
	other.Credential[0] = 1
	script := credential
	script.CredType = common.CredentialTypeScriptHash
	pool := &common.StakeDelegationCertificate{StakeCredential: &credential}
	vote := &common.VoteDelegationCertificate{StakeCredential: credential}
	combined := &common.StakeVoteDelegationCertificate{
		StakeCredential: credential,
	}
	delegationErr := conway.DelegateUnregisteredStakeCredentialError{
		Credential: credential,
	}
	for _, tc := range []struct {
		name         string
		certificates []common.Certificate
		err          error
		want         string
	}{
		{"pool", []common.Certificate{pool}, delegationErr, "StakeCredentialInvalidPoolDelegation"},
		{"vote", []common.Certificate{vote}, delegationErr, "StakeCredentialInvalidVoteDelegation"},
		{"stake and vote", []common.Certificate{combined}, delegationErr, "StakeCredentialInvalidVoteDelegation"},
		{"pool before vote", []common.Certificate{pool, vote}, delegationErr, "StakeCredentialInvalidPoolDelegation"},
		{"vote before pool", []common.Certificate{vote, pool}, delegationErr, "StakeCredentialInvalidVoteDelegation"},
		{
			"different hash before vote",
			[]common.Certificate{&common.StakeDelegationCertificate{StakeCredential: &other}, vote},
			delegationErr,
			"StakeCredentialInvalidVoteDelegation",
		},
		{
			"different credential type before vote",
			[]common.Certificate{&common.StakeDelegationCertificate{StakeCredential: &script}, vote},
			delegationErr,
			"StakeCredentialInvalidVoteDelegation",
		},
		{"unrelated error", []common.Certificate{vote}, conway.DRepNotRegisteredError{}, ""},
		{"accepted", []common.Certificate{vote}, nil, ""},
		{"no matching certificate", []common.Certificate{}, delegationErr, ""},
		{"joined error", []common.Certificate{vote}, errors.Join(errors.New("another rule"), delegationErr), "StakeCredentialInvalidVoteDelegation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &conway.ConwayTransaction{}
			for _, cert := range tc.certificates {
				tx.Body.TxCertificates = append(
					tx.Body.TxCertificates,
					common.CertificateWrapper{Certificate: cert},
				)
			}
			state := mockledger.NewLedgerStateBuilder().Build()
			if got := unregisteredDelegationPredicate(tx, state, tc.err); got != tc.want {
				t.Fatalf("delegation predicate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestUnregisteredDelegationPredicateTracksRegistration(t *testing.T) {
	t.Parallel()
	credential := common.Credential{CredType: common.CredentialTypeAddrKeyHash}
	pool := &common.StakeDelegationCertificate{StakeCredential: &credential}
	vote := &common.VoteDelegationCertificate{StakeCredential: credential}
	registration := &common.RegistrationCertificate{StakeCredential: credential}
	deregistration := &common.DeregistrationCertificate{
		StakeCredential: credential,
	}
	for _, tc := range []struct {
		name         string
		certificates []common.Certificate
		want         string
	}{
		{
			"pool then deregister then vote",
			[]common.Certificate{pool, deregistration, vote},
			"StakeCredentialInvalidVoteDelegation",
		},
		{
			"vote then deregister then pool",
			[]common.Certificate{vote, deregistration, pool},
			"StakeCredentialInvalidPoolDelegation",
		},
		{
			"re-register then delegate then deregister then vote",
			[]common.Certificate{deregistration, registration, pool, deregistration, vote},
			"StakeCredentialInvalidVoteDelegation",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &conway.ConwayTransaction{}
			for _, cert := range tc.certificates {
				tx.Body.TxCertificates = append(
					tx.Body.TxCertificates,
					common.CertificateWrapper{Certificate: cert},
				)
			}
			state := mockledger.NewLedgerStateBuilder().
				WithStakeCredentialRegistered(credential.Credential, true).
				Build()
			err := conway.DelegateUnregisteredStakeCredentialError{
				Credential: credential,
			}
			if got := unregisteredDelegationPredicate(tx, state, err); got != tc.want {
				t.Fatalf("delegation predicate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMismatchReportsUnmappedRejectionWithoutAborting(t *testing.T) {
	t.Parallel()
	outcome, failure, err := mismatch(
		"00001",
		expectedDoc{Predicate: "BadInputsUTxO"},
		"rejected: something novel",
	)
	if err != nil {
		t.Fatalf("mismatch returned error for unmapped rejection: %v", err)
	}
	if outcome.ID != "00001" || outcome.Result != unmappedLedgerResult {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if failure == nil || failure.Observed != "rejected: something novel" ||
		failure.Expected != "BadInputsUTxO" {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func TestReportJSONFields(t *testing.T) {
	t.Parallel()
	data, err := json.Marshal(Report{
		SchemaVersion:  schemaVersion,
		Scenario:       "ledger-rules",
		GeneratedAt:    "2026-10-24T00:00:00Z",
		Implementation: map[string]string{"name": "dingo"},
		Totals:         Totals{Total: 1, Passed: 1},
		Scenarios:      []CaseOutcome{{ID: "00001", Result: "pass"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	keys := []string{
		"version",
		"scenario",
		"generated_at",
		"implementation",
		"scenarios",
	}
	if len(got) != len(keys) {
		t.Fatalf("report fields = %s, want %v", data, keys)
	}
	for _, key := range keys {
		if _, ok := got[key]; !ok {
			t.Errorf("report is missing %q", key)
		}
	}
	if got["version"] != schemaVersion {
		t.Fatalf("version = %v", got["version"])
	}
	impl, _ := got["implementation"].(map[string]any)
	if impl["name"] != "dingo" {
		t.Fatalf("implementation = %v", got["implementation"])
	}
}

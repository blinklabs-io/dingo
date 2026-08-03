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

package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestActionTypeName(t *testing.T) {
	tests := map[int64]string{
		0: "Parameter Change",
		1: "Hard Fork Initiation",
		2: "Treasury Withdrawal",
		3: "No Confidence",
		4: "Update Committee",
		5: "New Constitution",
		6: "Info",
	}
	for input, expected := range tests {
		if got := actionTypeName(input); got != expected {
			t.Fatalf("actionTypeName(%d) = %q, want %q", input, got, expected)
		}
	}
}

func TestGovtoolActionURL(t *testing.T) {
	got := govtoolActionURL(
		"https://preview.gov.tools",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		2,
	)
	want := "https://preview.gov.tools/governance_actions/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef#2"
	if got != want {
		t.Fatalf("govtoolActionURL() = %q, want %q", got, want)
	}
}

func TestIsHex(t *testing.T) {
	if !isHex("abcdef012345", 12) {
		t.Fatal("expected valid lowercase hex")
	}
	if isHex("ABCDEF012345", 12) {
		t.Fatal("expected uppercase hex to be rejected")
	}
	if isHex("abcdef012345", 10) {
		t.Fatal("expected mismatched length to be rejected")
	}
}

func TestVoteBackfillPending(t *testing.T) {
	if !voteBackfillPending(0, 110_000_000, &backfillStatus{
		LastSlot:  2_000_000,
		Completed: false,
	}) {
		t.Fatal("expected vote backfill to be pending")
	}
	if voteBackfillPending(1, 110_000_000, &backfillStatus{
		LastSlot:  2_000_000,
		Completed: false,
	}) {
		t.Fatal("votes already present should not be pending")
	}
	if voteBackfillPending(0, 110_000_000, &backfillStatus{
		LastSlot:  110_000_000,
		Completed: false,
	}) {
		t.Fatal("backfill at proposal slot should not be pending")
	}
	if voteBackfillPending(0, 110_000_000, &backfillStatus{
		LastSlot:  2_000_000,
		Completed: true,
	}) {
		t.Fatal("completed backfill should not be pending")
	}
}

func TestDrepExpiryPredicate(t *testing.T) {
	tests := map[string]struct {
		want string
		ok   bool
	}{
		"":        {want: "", ok: true},
		"expired": {want: drepExpiredPredicate, ok: true},
		"active":  {want: drepUnexpiredPredicate, ok: true},
		"bogus":   {want: "", ok: false},
		"ACTIVE":  {want: "", ok: false},
	}
	for filter, expected := range tests {
		got, ok := drepExpiryPredicate(filter)
		if ok != expected.ok {
			t.Fatalf("drepExpiryPredicate(%q) ok = %v, want %v", filter, ok, expected.ok)
		}
		if got != expected.want {
			t.Fatalf("drepExpiryPredicate(%q) = %q, want %q", filter, got, expected.want)
		}
	}
}

func TestDrepExpiryState(t *testing.T) {
	epoch := func(v int64) sql.NullInt64 {
		return sql.NullInt64{Int64: v, Valid: true}
	}

	status, remaining := drepExpiryState(0, epoch(120))
	if status != "unknown" || remaining != nil {
		t.Fatalf("zero expiry epoch = (%q, %v), want (unknown, nil)", status, remaining)
	}

	status, remaining = drepExpiryState(120, sql.NullInt64{})
	if status != "unknown" || remaining != nil {
		t.Fatalf("unknown latest epoch = (%q, %v), want (unknown, nil)", status, remaining)
	}

	status, remaining = drepExpiryState(125, epoch(120))
	if status != "active" || remaining == nil || *remaining != 5 {
		t.Fatalf("unexpired = (%q, %v), want (active, 5)", status, remaining)
	}

	// The Conway tally treats expiry_epoch <= current epoch as expired, so
	// an equal epoch is already expired rather than about to expire.
	status, remaining = drepExpiryState(120, epoch(120))
	if status != "expired" || remaining == nil || *remaining != 0 {
		t.Fatalf("expiry at current epoch = (%q, %v), want (expired, 0)", status, remaining)
	}

	status, remaining = drepExpiryState(100, epoch(120))
	if status != "expired" || remaining == nil || *remaining != -20 {
		t.Fatalf("expired = (%q, %v), want (expired, -20)", status, remaining)
	}

	// A bigint column cannot hold an epoch this large, so an unrepresentable
	// value is reported as unknown rather than wrapped into a bogus count.
	status, remaining = drepExpiryState(math.MaxUint64, epoch(120))
	if status != "unknown" || remaining != nil {
		t.Fatalf("unrepresentable expiry = (%q, %v), want (unknown, nil)", status, remaining)
	}
}

func TestFirstSeenSlot(t *testing.T) {
	if got := firstSeenSlot(4_000, 9_000); got != 4_000 {
		t.Fatalf("firstSeenSlot with cert history = %d, want 4000", got)
	}
	if got := firstSeenSlot(0, 9_000); got != 9_000 {
		t.Fatalf("firstSeenSlot without cert history = %d, want 9000", got)
	}
}

func TestWithdrawalZeroAmount(t *testing.T) {
	tests := map[string]bool{
		"":        true,
		" ":       true,
		"0":       true,
		"000":     true,
		"1":       false,
		"100":     false,
		"1000000": false,
	}
	for amount, expected := range tests {
		if got := withdrawalZeroAmount(amount); got != expected {
			t.Fatalf("withdrawalZeroAmount(%q) = %v, want %v", amount, got, expected)
		}
	}
}

func TestNullSlot(t *testing.T) {
	if got := nullSlot(sql.NullInt64{}); got != 0 {
		t.Fatalf("nullSlot(invalid) = %d, want 0", got)
	}
	if got := nullSlot(sql.NullInt64{Int64: -5, Valid: true}); got != 0 {
		t.Fatalf("nullSlot(negative) = %d, want 0", got)
	}
	if got := nullSlot(sql.NullInt64{Int64: 77, Valid: true}); got != 77 {
		t.Fatalf("nullSlot(77) = %d, want 77", got)
	}
}

func TestDrepJSONOmitsUnknownExpiryFields(t *testing.T) {
	body, err := json.Marshal(drep{
		Credential:   "aa",
		ExpiryStatus: "unknown",
	})
	if err != nil {
		t.Fatalf("marshal drep: %v", err)
	}
	encoded := string(body)
	for _, field := range []string{
		"epochsUntilExpiry",
		"firstSeenSlot",
		"lastRegistrationSlot",
	} {
		if strings.Contains(encoded, field) {
			t.Fatalf("drep JSON = %s, want %q omitted", encoded, field)
		}
	}
	if !strings.Contains(encoded, `"expiryStatus":"unknown"`) {
		t.Fatalf("drep JSON = %s, want expiryStatus", encoded)
	}
}

func TestHandleDrepsRejectsInvalidFilters(t *testing.T) {
	// A nil database is safe here: both filters are validated before any
	// query runs, so a rejected request must never reach the database.
	a := &app{}
	for _, query := range []string{
		"/api/dreps?active=maybe",
		"/api/dreps?expiry=soon",
	} {
		req := httptest.NewRequest(http.MethodGet, query, nil)
		rec := httptest.NewRecorder()
		a.handleDreps(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", query, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestHandleStakeLookupRejectsInvalidInput(t *testing.T) {
	a := &app{}
	validCredential := strings.Repeat("ab", 28)
	tests := map[string]string{
		"short credential": "/api/stake/abcd?credential_tag=0",
		"missing tag":      "/api/stake/" + validCredential,
		"invalid tag":      "/api/stake/" + validCredential + "?credential_tag=2",
	}
	for name, target := range tests {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		// PathValue is only populated by the router, so set the pattern
		// values the handler reads directly.
		req.SetPathValue("credential", strings.TrimSuffix(
			strings.TrimPrefix(strings.Split(target, "?")[0], "/api/stake/"),
			"/",
		))
		a.handleStakeLookup(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want %d", name, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestServerErrorDoesNotExposeInternalError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/proposals", nil)
	rec := httptest.NewRecorder()

	serverError(rec, req, "query proposals", errors.New("database password=secret failed"))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	body := rec.Body.String()
	if strings.Contains(body, "password=secret") || strings.Contains(body, "query proposals") {
		t.Fatalf("serverError exposed internal details: %q", body)
	}
	if !strings.Contains(body, "internal server error") {
		t.Fatalf("serverError body = %q, want generic error", body)
	}
}
